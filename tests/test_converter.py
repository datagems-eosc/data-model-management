import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import dmm_api.resources.security as security
from dmm_api.main import app

PROFILE = Path(__file__).parent / "data" / "zoo" / "zoo_2024_pg.json"
FORMATS = {"from": "moma", "to": "croissant"}


@pytest.fixture
def client():
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def logged_in():
    app.dependency_overrides[security.require_app_scope] = lambda: {}


def test_convert_needs_a_login(client):
    response = client.post(
        "/api/v1/convert",
        params=FORMATS,
        files={"file": ("profile.json", PROFILE.read_bytes())},
    )
    assert response.status_code == 401


def test_convert_writes_no_file_whatever_the_upload_is_called(
    client, logged_in, tmp_path, monkeypatch
):
    temp_dir = tmp_path / "tmp"
    temp_dir.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp_dir))

    response = client.post(
        "/api/v1/convert",
        params=FORMATS,
        files={"file": ("../escaped.json", PROFILE.read_bytes())},
    )

    assert response.status_code == 200
    assert response.json()["output"]["@id"] == "8930240b-a0e8-46e7-ace8-aab2b42fcc01"
    assert list(tmp_path.rglob("*")) == [temp_dir]
