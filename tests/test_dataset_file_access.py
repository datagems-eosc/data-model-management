from pathlib import Path

import duckdb
import httpx
import pytest
from fastapi.testclient import TestClient

import dmm_api.resources.security as security
from dmm_api.main import app
from dmm_api.resources import dataset

DATASET_ID = "8930240b-a0e8-46e7-ace8-aab2b42fcc01"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT a FROM t WHERE b > 1",
        "WITH s AS (SELECT 1 AS a) SELECT a FROM s",
        "SELECT 1 UNION SELECT 2",
    ],
)
def test_a_single_query_runs(sql):
    dataset._require_one_query(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "COPY (SELECT 1) TO 'x.csv'",
        "ATTACH 'x.db'",
        "SELECT 1; SELECT 2",
        "PRAGMA version",
        "INSTALL httpfs",
        "LOAD httpfs",
        "SET enable_external_access = true",
        "SELECT FROM WHERE",
    ],
)
def test_anything_but_a_single_query_is_refused(sql):
    with pytest.raises(ValueError):
        dataset._require_one_query(sql)


def test_a_csv_url_maps_to_the_dataset_mount():
    assert dataset._dataset_csv_path("s3://dataset/d1/x.csv") == "/s3/dataset/d1/x.csv"
    assert (
        dataset._dataset_csv_path("s3://dataset/d1/it's.csv")
        == "/s3/dataset/d1/it''s.csv"
    )


@pytest.mark.parametrize(
    "url",
    [
        "/tmp/x.csv",
        "s3://dataset/../scratchpad/x.csv",
        "s3://dataset/d1/../../x.csv",
        "s3://other-bucket/x.csv",
        "https://example.org/x.csv",
        "",
    ],
)
def test_any_other_csv_url_is_refused(url):
    with pytest.raises(ValueError):
        dataset._dataset_csv_path(url)


@pytest.mark.parametrize("name", ["zoo", "Zoo_2024", "_x"])
def test_a_database_name_is_accepted(name):
    assert dataset._postgres_db_name(name) == name


@pytest.mark.parametrize(
    "name", ["", "zoo db", "zoo-db", "a.b", "x' AS y (TYPE postgres); COPY"]
)
def test_any_other_database_name_is_refused(name):
    with pytest.raises(ValueError):
        dataset._postgres_db_name(name)


@pytest.fixture
def mount(tmp_path, monkeypatch):
    (tmp_path / "dataset" / "d1").mkdir(parents=True)
    (tmp_path / "dataset" / "d1" / "x.csv").write_text("a,b\n1,2\n")
    (tmp_path / "outside.csv").write_text("secret\n42\n")
    monkeypatch.setattr(dataset, "DATASET_MOUNT", f"{tmp_path}/dataset/")
    return tmp_path


def test_a_restricted_connection_reads_only_the_datasets(mount):
    con = duckdb.connect(database=":memory:", config=dataset.DUCKDB_CONFIG)
    dataset._restrict_to_datasets(con)
    con.sql(f"CREATE VIEW v AS SELECT * FROM read_csv_auto('{mount}/dataset/d1/x.csv')")

    assert con.sql("SELECT * FROM v").fetchall() == [(1, 2)]
    for sql in [
        f"SELECT * FROM read_csv_auto('{mount}/outside.csv')",
        f"SELECT * FROM read_text('{mount}/dataset/../outside.csv')",
    ]:
        with pytest.raises(duckdb.PermissionException):
            con.sql(sql).fetchall()
    with pytest.raises(duckdb.InvalidInputException):
        con.sql("SET enable_external_access = true")


def mixed_query(query, argname="arg1"):
    return dataset.execute_query_mixed(
        {
            "query": query,
            "args_map": {
                argname: {"mimeType": "text/csv", "contentUrl": "s3://dataset/d1/x.csv"}
            },
        }
    )


def test_a_mixed_query_reads_its_csv(mount):
    assert mixed_query("SELECT t.a, t.b FROM {{arg1}} as t").values.tolist() == [[1, 2]]


def test_an_argument_name_never_reaches_sql(mount):
    argname = (
        f"x AS SELECT 1; COPY (SELECT 1) TO '{mount}/escaped.csv'; CREATE VIEW tail"
    )

    assert mixed_query("SELECT 1 AS a", argname)["a"].tolist() == [1]
    assert not (mount / "escaped.csv").exists()


def test_a_second_statement_is_refused(mount):
    with pytest.raises(Exception, match="single SELECT"):
        mixed_query("SELECT t.a FROM {{arg1}} as t; PRAGMA version")


@pytest.fixture
def client():
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def logged_in():
    app.dependency_overrides[security.require_app_scope] = lambda: {}


@pytest.fixture
def scratchpad(tmp_path, monkeypatch):
    monkeypatch.setenv("SCRATCHPAD_DIR", str(tmp_path / "scratchpad"))
    return tmp_path


def upload(client, dataset_id, file_name):
    return client.post(
        "/api/v1/data-workflow",
        data={"dataset_id": dataset_id, "file_name": file_name},
        files={"file": ("upload.csv", b"a,b\n1,2\n")},
    )


def test_data_workflow_needs_a_login(client, scratchpad):
    assert upload(client, DATASET_ID, "x.csv").status_code == 401
    assert list(scratchpad.iterdir()) == []


def test_data_workflow_writes_under_the_scratchpad(client, logged_in, scratchpad):
    response = upload(client, DATASET_ID, "x.csv")

    assert response.status_code == 200
    written = scratchpad / "scratchpad" / DATASET_ID / "x.csv"
    assert written.read_text() == "a,b\n1,2\n"


@pytest.mark.parametrize(
    "dataset_id, file_name",
    [
        ("../d1", "x.csv"),
        ("not-a-uuid", "x.csv"),
        (DATASET_ID, "../x.csv"),
        (DATASET_ID, "d2/x.csv"),
        (DATASET_ID, "/tmp/x.csv"),
        (DATASET_ID, ".."),
    ],
)
def test_data_workflow_refuses_a_path(
    client, logged_in, scratchpad, dataset_id, file_name
):
    assert upload(client, dataset_id, file_name).status_code == 400
    assert list(scratchpad.iterdir()) == []


def test_a_query_result_needs_a_valid_token(client, monkeypatch):
    async def no_keys():
        return {}

    monkeypatch.setattr(security, "_get_jwks", no_keys)
    response = client.get(
        f"/api/v1/polyglot/query/result/{DATASET_ID}",
        headers={"Authorization": "Bearer not-a-token"},
    )

    assert response.status_code == 401


ZOO_ID = "c893daaf-680f-4947-88e5-03fd61900795"
ZOO_LOAD = (Path(__file__).parent / "load" / "zoo.json").read_text()


@pytest.fixture
def bucket(tmp_path, monkeypatch):
    """A bucket mount with the zoo dataset in the scratchpad and in the
    staging folder, an empty dataset folder, and MoMa saying the dataset
    exists."""

    async def exists(*args, **kwargs):
        return True, {"nodes": [], "edges": []}

    async def no_moma(self, url, **kwargs):
        raise AssertionError(f"the test reached MoMa: {url}")

    # A load that passes the checks updates MoMa next; no test may reach it.
    monkeypatch.setattr(httpx.AsyncClient, "patch", no_moma)
    monkeypatch.setattr(dataset, "S3_MOUNT", tmp_path)
    monkeypatch.setenv("SCRATCHPAD_DIR", str(tmp_path / "scratchpad"))
    monkeypatch.setenv("DATASET_STAGING_DIR", str(tmp_path / "dataset_staging"))
    monkeypatch.setenv("DATASET_DIR", str(tmp_path / "dataset"))
    monkeypatch.setattr(dataset, "get_dataset_metadata", exists)
    for folder in ["scratchpad", "dataset_staging"]:
        (tmp_path / folder / ZOO_ID).mkdir(parents=True)
        (tmp_path / folder / ZOO_ID / "zoo.csv").write_text("a\n1\n")
    (tmp_path / "dataset").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "kept.log").write_text("x\n")
    return tmp_path


def load(client, archived_at, force=False, dataset_id=ZOO_ID):
    body = (
        ZOO_LOAD.replace(f"s3://scratchpad/{ZOO_ID}", "ARCHIVED_AT")
        .replace(ZOO_ID, dataset_id)
        .replace("ARCHIVED_AT", archived_at)
    )
    return client.put(
        "/api/v1/dataset/load",
        params={"force": force},
        content=body,
        headers={"Content-Type": "application/json", "Authorization": "Bearer t"},
    )


@pytest.mark.parametrize(
    "archived_at, force",
    [
        ("s3://scratchpad/../logs", False),
        ("s3://logs", False),
        ("s3://scratchpad", False),
        ("s3://dataset_staging", False),
        ("s3://dataset_staging/../logs", False),
        (f"s3://dataset/{ZOO_ID}", False),
        ("s3://scratchpad/../logs", True),
    ],
)
def test_load_moves_nothing_from_outside_the_source_folders(
    client, logged_in, bucket, archived_at, force
):
    response = load(client, archived_at, force)

    assert response.status_code == 400
    assert (bucket / "logs" / "kept.log").exists()
    assert list((bucket / "dataset").iterdir()) == []


@pytest.mark.parametrize("dataset_id", ["../x", "a/b", "ABSOLUTE"])
def test_load_refuses_a_dataset_id_that_names_a_path(
    client, logged_in, bucket, dataset_id
):
    if dataset_id == "ABSOLUTE":
        # Inside DATASET_DIR once resolved, but it would also replace the
        # catalogue's root.
        dataset_id = str(bucket / "dataset" / "example")
    response = load(client, f"s3://scratchpad/{ZOO_ID}", dataset_id=dataset_id)

    assert response.status_code == 400
    assert (bucket / "scratchpad" / ZOO_ID / "zoo.csv").exists()
    assert list((bucket / "dataset").iterdir()) == []


def test_load_refuses_a_file_as_its_source(client, logged_in, bucket):
    response = load(client, f"s3://scratchpad/{ZOO_ID}/zoo.csv")

    assert response.status_code == 400
    assert (bucket / "scratchpad" / ZOO_ID / "zoo.csv").exists()
    assert list((bucket / "dataset").iterdir()) == []


@pytest.mark.parametrize("folder", ["scratchpad", "dataset_staging"])
def test_load_takes_a_folder_from_a_source_folder(
    client, logged_in, bucket, monkeypatch, folder
):
    async def moma_fails(self, url, **kwargs):
        return httpx.Response(500, request=httpx.Request("PATCH", url))

    # The move passes the check; MoMa's failure then rolls it back.
    monkeypatch.setattr(httpx.AsyncClient, "patch", moma_fails)
    response = load(client, f"s3://{folder}/{ZOO_ID}")

    assert response.status_code == 502
    assert (bucket / folder / ZOO_ID / "zoo.csv").exists()
