import asyncio
import io

import pytest
from fastapi import APIRouter, HTTPException, Request, UploadFile
from fastapi.testclient import TestClient

from dmm_api import uploads
from dmm_api.main import app


@pytest.fixture
def small_limit(monkeypatch):
    monkeypatch.setattr(uploads, "MAX_UPLOAD_BYTES", 10)
    monkeypatch.setattr(uploads, "CHUNK_BYTES", 4)


@pytest.fixture(scope="module")
def client():
    router = APIRouter()

    @router.post("/test-uploads/size")
    async def size(request: Request):
        return {"size": len(await request.body())}

    app.include_router(router)
    yield TestClient(app)
    app.router.routes[:] = [
        r
        for r in app.router.routes
        if not getattr(r, "path", "").startswith("/test-uploads")
    ]


def test_a_file_at_the_limit_is_read_whole(small_limit):
    file = UploadFile(file=io.BytesIO(b"x" * 10))
    assert asyncio.run(uploads.read_upload(file)) == b"x" * 10


def test_reading_stops_at_the_first_piece_past_the_limit(small_limit):
    source = io.BytesIO(b"x" * 100)
    with pytest.raises(HTTPException) as error:
        asyncio.run(uploads.read_upload(UploadFile(file=source)))
    assert error.value.status_code == 413
    assert source.tell() == 12


def test_a_body_at_the_limit_reaches_the_endpoint(small_limit, client):
    response = client.post("/test-uploads/size", content=b"x" * 10)
    assert response.json() == {"size": 10}


def test_a_declared_length_over_the_limit_gets_the_413_body(small_limit, client):
    response = client.post(
        "/api/v1/convert?from=moma&to=croissant",
        files={"file": ("big.json", b"x" * 11)},
    )
    assert response.status_code == 413
    assert response.json() == {
        "code": 413,
        "error": f"Request too large: the limit is {uploads.MAX_UPLOAD_MB} MB.",
        "correlationId": response.headers["x-tracking-correlation"],
    }


def test_a_declared_length_over_the_limit_is_refused_unread(small_limit):
    async def endpoint(scope, receive, send):
        raise AssertionError("the request reached the endpoint")

    async def receive():
        raise AssertionError("the body was read")

    sent = []

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "headers": [(b"content-length", b"11")]}
    asyncio.run(uploads.BodyLimitMiddleware(endpoint)(scope, receive, send))
    assert sent[0]["status"] == 413


def test_a_body_without_a_length_is_counted_across_pieces(small_limit):
    calls = 0

    async def receive():
        nonlocal calls
        calls += 1
        return {"type": "http.request", "body": b"x" * 4, "more_body": calls < 5}

    async def endpoint(scope, receive, send):
        while (await receive())["more_body"]:
            pass

    scope = {"type": "http", "headers": []}
    with pytest.raises(HTTPException) as error:
        asyncio.run(uploads.BodyLimitMiddleware(endpoint)(scope, receive, None))
    assert error.value.status_code == 413
    assert calls == 3


def test_a_body_without_a_length_is_stopped_past_the_limit(small_limit, client):
    def pieces():
        for _ in range(3):
            yield b"x" * 4

    response = client.post("/test-uploads/size", content=pieces())
    assert response.status_code == 413
    assert response.json() == {
        "code": 413,
        "error": f"Request too large: the limit is {uploads.MAX_UPLOAD_MB} MB.",
        "correlationId": response.headers["x-tracking-correlation"],
    }
