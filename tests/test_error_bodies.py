"""Error bodies carry no exception text; the log keeps it."""

import json
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient
from structlog.testing import capture_logs

import dmm_api.resources.security as security
from dmm_api.main import app
from dmm_api.resources import dataset

QUERY_REQUEST = json.loads(
    (Path(__file__).parent / "query" / "query_CSV.json").read_text()
)
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
