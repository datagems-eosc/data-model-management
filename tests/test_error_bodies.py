"""Error bodies carry no exception text; the log keeps it."""

import json
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


@pytest.mark.parametrize(
    "error, shown",
    [
        (duckdb.BinderException('Referenced column "x" not found'), True),
        (duckdb.IOException("could not connect: host=db user=reader"), False),
    ],
)
def test_a_failed_query_shows_only_what_is_wrong_with_the_sql(
    client, monkeypatch, error, shown
):
    async def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(dataset, "execute_query", fail)
    response = client.post("/api/v1/polyglot/query", json=QUERY_REQUEST, headers=TOKEN)

    assert response.status_code == 500
    assert (str(error) in response.json()["error"]) is shown


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
    "error, shown",
    [
        (duckdb.BinderException('Referenced column "x" not found'), True),
        (Exception("PostgreSQL connection failed: host=db.internal"), False),
    ],
)
def test_a_query_after_text2sql_shows_only_what_is_wrong_with_the_sql(
    client, monkeypatch, error, shown
):
    async def fail(*args, **kwargs):
        raise error

    reply = json.loads((TEXT2SQL / "in-dataset-response.json").read_text())
    monkeypatch.setattr(httpx.AsyncClient, "post", _answer(200, json=reply))
    monkeypatch.setattr(dataset, "store_AP_in_grafeo", lambda ap: None)
    monkeypatch.setattr(dataset, "execute_query", fail)
    response = client.post(
        "/api/v1/in-dataset-discovery/text2sql",
        json=json.loads((TEXT2SQL / "in-dataset-request.json").read_text()),
        headers=TOKEN,
    )

    assert response.json()["code"] == 207
    assert (str(error) in response.json()["content"]["query_error"]) is shown
    assert "db.internal" not in response.text
