"""Direct graph queries go to the Grafeo that GRAFEO_URL names."""

from fastapi.testclient import TestClient

from dmm_api.main import app
from dmm_api.resources import dataset


class _GrafeoReply:
    def raise_for_status(self):
        pass

    def json(self):
        return {"columns": ["ok"], "rows": [[1]]}


def test_a_direct_query_uses_grafeo_url(monkeypatch):
    posted = []

    def post(url, **kwargs):
        posted.append(url)
        return _GrafeoReply()

    monkeypatch.setattr(dataset, "GRAFEO_URL", "http://grafeo.example:1234")
    monkeypatch.setattr(dataset.requests, "post", post)
    response = TestClient(app).get("/api/v1/grafeo/test")

    assert response.status_code == 200
    assert response.json() == [{"ok": 1}]
    assert posted == ["http://grafeo.example:1234/cypher"]
