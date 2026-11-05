import asyncio
import io

import pytest
from fastapi import HTTPException, UploadFile
from fastapi.testclient import TestClient

from dmm_api import uploads
from dmm_api.main import app


@pytest.fixture
def small_limit(monkeypatch):
    monkeypatch.setattr(uploads, "MAX_UPLOAD_BYTES", 10)
    monkeypatch.setattr(uploads, "CHUNK_BYTES", 4)


def test_a_file_at_the_limit_is_read_whole(small_limit):
    file = UploadFile(file=io.BytesIO(b"x" * 10))
    assert asyncio.run(uploads.read_upload(file)) == b"x" * 10


def test_reading_stops_at_the_first_piece_past_the_limit(small_limit):
    source = io.BytesIO(b"x" * 100)
    with pytest.raises(HTTPException) as error:
        asyncio.run(uploads.read_upload(UploadFile(file=source)))
    assert error.value.status_code == 413
    assert source.tell() == 12


def test_one_byte_over_gets_the_413_body(small_limit):
    response = TestClient(app).post(
        "/api/v1/convert?from=moma&to=croissant",
        files={"file": ("big.json", b"x" * 11)},
    )
    assert response.status_code == 413
    assert response.json() == {
        "code": 413,
        "error": f"File too large: the limit is {uploads.MAX_UPLOAD_MB} MB.",
    }
