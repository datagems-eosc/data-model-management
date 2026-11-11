"""Error bodies carry no exception text; the log keeps it."""

import json
import traceback
from pathlib import Path

import duckdb
import httpx
import pytest
from fastapi.testclient import TestClient
from structlog.testing import capture_logs

import dmm_api.resources.security as security
from dmm_api.main import app
from dmm_api.resources import dataset

TESTS = Path(__file__).parent
QUERY_REQUEST = json.loads((TESTS / "query" / "query_CSV.json").read_text())
CDD_REQUEST = json.loads(
    (TESTS / "cross-dataset" / "cdd-search-ap-request.json").read_text()
)
TEXT2SQL = TESTS / "in-dataset" / "text2sql"
TOKEN = {"Authorization": "Bearer token"}


@pytest.fixture
def client():
    app.dependency_overrides[security.require_app_scope] = lambda: {}
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_a_server_error_keeps_the_exception_out_of_the_body(client, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("password=hunter2")

    monkeypatch.setattr(dataset, "get_results_uuid", fail)
    with capture_logs() as logs:
        response = client.get("/api/v1/polyglot/query/result/x", headers=TOKEN)

    assert response.status_code == 500
    assert response.json()["error"] == "Failed to retrieve query result"
    assert "hunter2" not in response.text
    (entry,) = [e for e in logs if e["event"] == "request failed with an exception"]
    assert isinstance(entry["exc_info"], RuntimeError)


PASSWORD = "SECRETPW"
SQL_ERROR = duckdb.BinderException('Referenced column "x" not found')
CONNECTION_ERROR = duckdb.IOException(
    f"Unable to connect to Postgres at dbname=db user=reader password={PASSWORD} "
    "host=db.internal port=5432: connection refused"
)


class _FakeDuckDB:
    """A DuckDB connection whose ATTACH or query fails as DuckDB's would."""

    def __init__(self, attach_error=None, query_error=None):
        self.attach_error = attach_error
        self.query_error = query_error

    def sql(self, statement):
        if statement.startswith("ATTACH") and self.attach_error:
            raise self.attach_error

    def execute(self, *args):
        raise self.query_error

    def close(self):
        pass


@pytest.fixture(params=["postgres", "mixed"])
def failing_query(request, monkeypatch):
    """Run the real query helper of one query type, against a fake DuckDB."""

    async def query_builder(ap, token=None):
        return {
            "software": "DuckDB",
            "type": request.param,
            "query": "SELECT x FROM t",
            "args_map": {"t": {"mimeType": "text/sql", "dbConnection": {"name": "db"}}},
        }

    monkeypatch.setenv("DATAGEMS_POSTGRES_HOST", "db.internal")
    monkeypatch.setenv("DATAGEMS_POSTGRES_PORT", "5432")
    monkeypatch.setenv("DS_READER_USER", "reader")
    monkeypatch.setenv("DS_READER_PS", PASSWORD)
    monkeypatch.setattr(dataset, "extract_query_from_AP", query_builder)
    monkeypatch.setattr(dataset, "query_rewriting", lambda qb: qb["query"])
    monkeypatch.setattr(dataset, "query_rewriting_views", lambda q, views: q)
    monkeypatch.setattr(dataset, "extract_alias", lambda q, args: args)
    monkeypatch.setattr(dataset, "write_views_minimal_extraction", lambda q, a: (a, q))

    def fail_with(attach_error=None, query_error=None):
        fake = _FakeDuckDB(attach_error, query_error)
        monkeypatch.setattr(dataset.duckdb, "connect", lambda **kwargs: fake)

    return fail_with


def test_a_failed_query_shows_what_is_wrong_with_the_sql(client, failing_query):
    failing_query(query_error=SQL_ERROR)
    response = client.post("/api/v1/polyglot/query", json=QUERY_REQUEST, headers=TOKEN)

    assert response.status_code == 500
    assert response.json()["error"] == f"Failed to execute query: {SQL_ERROR}"


def test_a_failed_connection_keeps_the_password_out_of_body_and_log(
    client, failing_query
):
    failing_query(attach_error=CONNECTION_ERROR)
    with capture_logs() as logs:
        response = client.post(
            "/api/v1/polyglot/query", json=QUERY_REQUEST, headers=TOKEN
        )

    assert response.status_code == 500
    assert response.json()["error"] == "Failed to execute query"
    (entry,) = [e for e in logs if e["event"] == "request failed with an exception"]
    logged = "".join(traceback.format_exception(entry["exc_info"]))
    assert PASSWORD not in logged
    assert "password=***" in logged


def _answer(status_code, **content):
    """A fake reply from a called service, with the request raise_for_status needs."""

    async def answer(self, url, **kwargs):
        return httpx.Response(
            status_code, **content, request=httpx.Request("POST", url)
        )

    return answer


def test_a_moma_error_keeps_moma_reply_out_of_the_body(client, monkeypatch):
    monkeypatch.setattr(
        httpx.AsyncClient, "get", _answer(500, text="trace at moma.internal")
    )
    with capture_logs() as logs:
        response = client.get("/api/v1/dataset/get/d1", headers=TOKEN)

    assert response.status_code == 502
    assert response.json()["details"] == {
        "dataset_id": "d1",
        "source": "MoMa",
        "statusCode": 500,
    }
    assert "moma.internal" not in response.text
    assert any(e.get("response_text") == "trace at moma.internal" for e in logs)


def test_an_unreachable_moma_keeps_the_exception_out_of_the_body(client, monkeypatch):
    async def fail(self, url, **kwargs):
        raise httpx.ConnectError("cannot connect to moma.internal:443")

    monkeypatch.setattr(httpx.AsyncClient, "get", fail)
    with capture_logs() as logs:
        response = client.get("/api/v1/dataset/get/d1", headers=TOKEN)

    assert response.status_code == 503
    assert response.json()["details"]["source"] == "MoMa"
    assert "moma.internal" not in response.text
    (entry,) = [e for e in logs if e["event"] == "request failed with an exception"]
    assert isinstance(entry["exc_info"], httpx.ConnectError)


@pytest.mark.parametrize(
    "status_code, shown",
    [(400, True), (500, False)],
)
def test_a_forwarded_service_message_reaches_the_caller_only_after_a_400(
    client, monkeypatch, status_code, shown
):
    monkeypatch.setattr(
        httpx.AsyncClient,
        "post",
        _answer(status_code, json={"error": "query rejected by db.internal"}),
    )
    response = client.post(
        "/api/v1/cross-dataset-discovery/search", json=CDD_REQUEST, headers=TOKEN
    )

    assert response.status_code == status_code
    assert response.json()["details"] == {
        "source": "Cross-Dataset Discovery",
        "statusCode": status_code,
    }
    assert ("db.internal" in response.json()["error"]) is shown


def test_an_unreachable_login_server_keeps_its_host_out_of_the_body(monkeypatch):
    async def fail():
        raise httpx.ConnectError("cannot connect to login.internal:443")

    monkeypatch.setattr(security, "_get_jwks", fail)
    response = TestClient(app).get(
        "/api/v1/polyglot/query/result/x", headers={"Authorization": "Bearer x"}
    )

    assert response.status_code == 503
    assert response.json()["error"] == "Could not fetch JWKS"


@pytest.mark.parametrize(
    "errors, shown",
    [({"query_error": SQL_ERROR}, True), ({"attach_error": CONNECTION_ERROR}, False)],
)
def test_a_query_after_text2sql_shows_only_what_is_wrong_with_the_sql(
    client, monkeypatch, failing_query, errors, shown
):
    failing_query(**errors)
    reply = json.loads((TEXT2SQL / "in-dataset-response.json").read_text())
    monkeypatch.setattr(httpx.AsyncClient, "post", _answer(200, json=reply))
    monkeypatch.setattr(dataset, "store_AP_in_grafeo", lambda ap: None)
    response = client.post(
        "/api/v1/in-dataset-discovery/text2sql",
        json=json.loads((TEXT2SQL / "in-dataset-request.json").read_text()),
        headers=TOKEN,
    )

    query_error = response.json()["content"]["query_error"]
    assert response.json()["code"] == 207
    assert (query_error == str(SQL_ERROR)) is shown
    assert PASSWORD not in response.text
