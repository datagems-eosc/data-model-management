import json
from pathlib import Path

from fastapi.testclient import TestClient

from dmm_api.main import app
from dmm_api.resources.converter import convertProfile

PROFILE = Path(__file__).parent / "data" / "zoo" / "zoo_2024_pg.json"


def test_convert_answers_410_to_an_old_client_without_a_token():
    response = TestClient(app).post(
        "/api/v1/convert",
        params={"from": "moma", "to": "croissant"},
        files={"file": ("profile.json", PROFILE.read_bytes())},
    )

    assert response.status_code == 410
    assert response.json() == {
        "code": 410,
        "error": (
            "POST /convert is deprecated since v0.3.1 and no longer converts. "
            "Use GET /api/v1/dataset/get/{dataset_id}?format=croissant."
        ),
        "correlationId": response.headers["x-tracking-correlation"],
    }


def test_a_profile_still_converts_to_croissant():
    croissant = json.loads(convertProfile(json.loads(PROFILE.read_text())))
    assert croissant["@id"] == "8930240b-a0e8-46e7-ace8-aab2b42fcc01"
