import duckdb
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


def test_a_restricted_connection_reads_only_the_datasets(tmp_path, monkeypatch):
    (tmp_path / "dataset" / "d1").mkdir(parents=True)
    (tmp_path / "dataset" / "d1" / "x.csv").write_text("a,b\n1,2\n")
    (tmp_path / "outside.csv").write_text("secret\n42\n")
    monkeypatch.setattr(dataset, "DATASET_MOUNT", f"{tmp_path}/dataset/")

    con = duckdb.connect(database=":memory:", config=dataset.DUCKDB_CONFIG)
    con.sql(
        f"CREATE VIEW v AS SELECT * FROM read_csv_auto('{tmp_path}/dataset/d1/x.csv')"
    )
    dataset._restrict_to_datasets(con)

    assert con.sql("SELECT * FROM v").fetchall() == [(1, 2)]
    for sql in [
        f"SELECT * FROM read_csv_auto('{tmp_path}/outside.csv')",
        f"SELECT * FROM read_text('{tmp_path}/dataset/../outside.csv')",
    ]:
        with pytest.raises(duckdb.PermissionException):
            con.sql(sql).fetchall()
    with pytest.raises(duckdb.InvalidInputException):
        con.sql("SET enable_external_access = true")


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
