"""The dataset search's user_id filter: the datasets that user owns in Grafeo."""

import httpx
import pytest
from fastapi.testclient import TestClient

import dmm_api.resources.security as security
from dmm_api.main import app
from dmm_api.resources import dataset
from dmm_api.tools.AP import log_AP

TOKEN = {"Authorization": "Bearer token"}
SEARCH = "/api/v1/dataset/search"
NO_DATASETS = {
    "code": 200,
    "message": "No datasets found matching the search criteria",
    "datasets": [],
    "offset": 0,
    "count": 0,
    "total": 0,
}


@pytest.fixture
def client():
    app.dependency_overrides[security.require_app_scope] = lambda: {}
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def moma(monkeypatch):
    """Records the query parameters of each MoMa search; finds nothing."""
    searches = []

    async def get(self, url, params=None, **kwargs):
        searches.append(params)
        return httpx.Response(
            200,
            json={"datasets": [], "page": 1, "pageSize": 25, "total": 0},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "get", get)
    return searches


def _owns(monkeypatch, owned):
    monkeypatch.setattr(dataset, "get_datasets_by_user", lambda user_id: owned[user_id])


def _node_ids(params):
    return sorted(value for key, value in params if key == "nodeIds")


def test_user_id_searches_the_datasets_the_user_owns(client, moma, monkeypatch):
    _owns(monkeypatch, {"user-123": ["ds-a", "ds-b"]})
    response = client.get(SEARCH, params={"user_id": "user-123"}, headers=TOKEN)

    assert response.status_code == 200
    assert len(moma) == 1
    assert _node_ids(moma[0]) == ["ds-a", "ds-b"]


def test_user_id_and_node_ids_search_the_intersection(client, moma, monkeypatch):
    _owns(monkeypatch, {"user-123": ["ds-a", "ds-b"]})
    response = client.get(
        SEARCH,
        params=[("user_id", "user-123"), ("nodeIds", "ds-b"), ("nodeIds", "ds-c")],
        headers=TOKEN,
    )

    assert response.status_code == 200
    assert len(moma) == 1
    assert _node_ids(moma[0]) == ["ds-b"]


@pytest.mark.parametrize(
    ("owned", "node_ids"),
    [
        # The user owns no dataset.
        ([], []),
        # The user owns some, none of them among nodeIds.
        (["ds-a"], ["ds-c"]),
    ],
)
def test_a_user_filter_leaving_no_dataset_finds_none(
    client, moma, monkeypatch, owned, node_ids
):
    # Without a nodeIds filter, MoMa would return every dataset.
    _owns(monkeypatch, {"user-123": owned})
    params = [("user_id", "user-123")] + [("nodeIds", n) for n in node_ids]
    response = client.get(SEARCH, params=params, headers=TOKEN)

    assert response.status_code == 200
    assert response.json() == NO_DATASETS
    assert moma == []


def test_more_than_one_user_id_is_refused(client, moma, monkeypatch):
    _owns(monkeypatch, {"user-1": ["ds-a"], "user-2": ["ds-b"]})
    response = client.get(
        SEARCH, params=[("user_id", "user-1"), ("user_id", "user-2")], headers=TOKEN
    )

    assert response.status_code == 400
    assert response.json()["error"] == (
        "Only one user_id is allowed for filtering datasets."
    )
    assert moma == []


@pytest.mark.parametrize(
    "reply",
    [
        # Grafeo 0.5.44
        {"columns": ["d.id"], "rows": [["ds-a"], ["ds-b"]]},
        # Grafeo 0.6.3 adds two keys
        {
            "columns": ["d.id"],
            "rows": [["ds-a"], ["ds-b"]],
            "execution_time_ms": 1.2,
            "rows_scanned": 4,
        },
    ],
)
def test_get_datasets_by_user_returns_the_dataset_ids(monkeypatch, reply):
    monkeypatch.setattr(log_AP, "grafeo_begin", lambda: None)
    monkeypatch.setattr(log_AP, "grafeo_commit", lambda tx: None)
    monkeypatch.setattr(log_AP, "grafeo_execute", lambda tx, query: reply)

    assert log_AP.get_datasets_by_user("user-123") == ["ds-a", "ds-b"]
