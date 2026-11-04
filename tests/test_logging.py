import ast
import gzip
import io
import json
import logging
import re
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
import structlog
from fastapi import APIRouter, HTTPException
from fastapi.testclient import TestClient
from structlog.testing import capture_logs

from dmm_api import config
from dmm_api.logging_config import (
    FIXED_FIELDS,
    ChunkFileHandler,
    json_formatter,
    request_context,
    set_user_id,
)
from dmm_api.main import app
from dmm_api.resources.security import FailedDependencyException
from dmm_api.tools.log_archive import run


def _utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _record(when: datetime, message: str = "entry") -> logging.LogRecord:
    record = logging.LogRecord("tests", logging.INFO, __file__, 1, message, None, None)
    record.created = when.timestamp()
    return record


def _handler(logs_dir: Path, pod: str = "pod-a", **kwargs) -> ChunkFileHandler:
    handler = ChunkFileHandler(logs_dir, pod, **kwargs)
    handler.setFormatter(json_formatter())
    return handler


def _files(logs_dir: Path) -> list[str]:
    return sorted(str(p.relative_to(logs_dir)) for p in logs_dir.rglob("*.*"))


def _events(path: Path) -> list[str]:
    return [json.loads(line)["event"] for line in path.read_text().splitlines()]


def _entry(timestamp: str, event: str) -> str:
    return json.dumps({"timestamp": timestamp, "event": event}) + "\n"


def _write(path: Path, *lines: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(lines))


# Entries


def _log_as_json(log_calls) -> list[dict]:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(json_formatter())
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        log_calls(structlog.get_logger("tests.fields"))
    finally:
        root.removeHandler(handler)
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_an_entry_has_the_fixed_fields_in_order():
    def log_calls(logger):
        with request_context("abc-123"):
            set_user_id("user-1")
            logger.warning("something happened", dataset_id="d1", where=Path("x"))
        logger.info("outside a request")

    inside, outside = _log_as_json(log_calls)
    assert list(inside) == [*FIXED_FIELDS, "dataset_id", "where"]
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z", inside["timestamp"])
    assert inside["level"] == "warning"
    assert inside["event"] == "something happened"
    assert inside["logger"] == "tests.fields"
    assert inside["app_version"] == config.APP_VERSION
    assert inside["pod"] == config.POD_NAME
    assert inside["correlation_id"] == "abc-123"
    assert inside["user_id"] == "user-1"
    assert inside["where"] == "x"
    assert outside["correlation_id"] is None
    assert outside["user_id"] is None


def test_an_exception_comes_last_and_positional_arguments_are_filled_in():
    def log_calls(logger):
        try:
            raise ValueError("boom")
        except ValueError:
            logger.exception("failed", step="parse")
        logger.warning("JWT validation failed: %s", "expired")

    failed, warned = _log_as_json(log_calls)
    assert list(failed)[-2:] == ["step", "exception"]
    assert "ValueError: boom" in failed["exception"]
    assert warned["event"] == "JWT validation failed: expired"


def test_no_log_call_passes_a_reserved_field():
    levels = {"debug", "info", "warning", "error", "critical", "exception"}
    offending = []
    for path in (Path(__file__).parent.parent / "dmm_api").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in levels
            ):
                offending += [
                    f"{path.name}:{node.lineno} {kw.arg}"
                    for kw in node.keywords
                    if kw.arg in FIXED_FIELDS
                ]
    assert offending == []


# Chunks


def test_a_flush_writes_one_chunk(tmp_path):
    handler = _handler(tmp_path)
    handler.emit(_record(_utc(2026, 11, 3, 10, 15, 3, 412000), "first"))
    handler.emit(_record(_utc(2026, 11, 3, 10, 15, 4), "second"))
    handler.write_chunks()
    assert _files(tmp_path) == ["2026-11/dmm-api_2026-11-03T10-15-03Z_pod-a.log"]
    chunk = tmp_path / "2026-11/dmm-api_2026-11-03T10-15-03Z_pod-a.log"
    assert _events(chunk) == ["first", "second"]


def test_a_chunk_never_spans_two_days(tmp_path):
    handler = _handler(tmp_path)
    handler.emit(_record(_utc(2026, 10, 31, 23, 59, 59)))
    handler.emit(_record(_utc(2026, 11, 1, 0, 0, 1)))
    handler.write_chunks()
    assert _files(tmp_path) == [
        "2026-10/dmm-api_2026-10-31T23-59-59Z_pod-a.log",
        "2026-11/dmm-api_2026-11-01T00-00-01Z_pod-a.log",
    ]


def test_a_taken_chunk_name_gets_a_number(tmp_path):
    handler = _handler(tmp_path)
    for _ in range(2):
        handler.emit(_record(_utc(2026, 11, 3, 10, 0, 0)))
        handler.write_chunks()
    assert _files(tmp_path) == [
        "2026-11/dmm-api_2026-11-03T10-00-00Z-2_pod-a.log",
        "2026-11/dmm-api_2026-11-03T10-00-00Z_pod-a.log",
    ]


def test_a_failed_write_keeps_the_entries_for_the_next_flush(tmp_path):
    blocked = tmp_path / "blocked"
    blocked.write_text("")
    handler = _handler(blocked)
    handler.emit(_record(_utc(2026, 11, 3, 10, 0, 0), "kept"))
    handler.write_chunks()
    handler.logs_dir = tmp_path / "logs"
    handler.logs_dir.mkdir()
    handler.write_chunks()
    assert _events(
        handler.logs_dir / "2026-11/dmm-api_2026-11-03T10-00-00Z_pod-a.log"
    ) == ["kept"]


def test_a_full_buffer_drops_the_oldest_entries(tmp_path):
    handler = _handler(tmp_path, max_entries=3)
    for second in range(5):
        handler.emit(_record(_utc(2026, 11, 3, 10, 0, second), f"e{second}"))
    handler.write_chunks()
    (chunk,) = tmp_path.rglob("*.log")
    assert _events(chunk) == ["e2", "e3", "e4"]


def test_a_buffer_at_chunk_size_wakes_the_writer(tmp_path):
    handler = _handler(tmp_path, chunk_entries=2)
    handler.emit(_record(_utc(2026, 11, 3, 10, 0, 0)))
    assert not handler._wake.is_set()
    handler.emit(_record(_utc(2026, 11, 3, 10, 0, 1)))
    assert handler._wake.is_set()


# Daily files


def test_the_daily_merge_joins_the_pods_chunks_in_order(tmp_path):
    month = tmp_path / "2026-11"
    _write(month / "dmm-api_2026-11-02T23-00-00Z-2_pod-a.log", "a2\n")
    _write(month / "dmm-api_2026-11-02T23-00-00Z_pod-a.log", "a1\n")
    _write(month / "dmm-api_2026-11-02T09-00-00Z_pod-a.log", "a0\n")
    _write(month / "dmm-api_2026-11-02T10-00-00Z_pod-b.log", "b0\n")
    _write(month / "dmm-api_2026-11-03T00-01-00Z_pod-a.log", "today\n")
    _handler(tmp_path).merge_days(today=date(2026, 11, 3))
    assert (month / "dmm-api_2026-11-02_pod-a.log").read_text() == "a0\na1\na2\n"
    assert _files(tmp_path) == [
        "2026-11/dmm-api_2026-11-02T10-00-00Z_pod-b.log",
        "2026-11/dmm-api_2026-11-02_pod-a.log",
        "2026-11/dmm-api_2026-11-03T00-01-00Z_pod-a.log",
    ]


def test_the_daily_file_goes_in_the_folder_of_its_month(tmp_path):
    _write(tmp_path / "2026-10/dmm-api_2026-10-31T23-00-00Z_pod-a.log", "a\n")
    _handler(tmp_path).merge_days(today=date(2026, 11, 1))
    assert _files(tmp_path) == ["2026-10/dmm-api_2026-10-31_pod-a.log"]


def test_an_existing_daily_file_stops_the_merge(tmp_path):
    month = tmp_path / "2026-11"
    _write(month / "dmm-api_2026-11-02_pod-a.log", "old\n")
    _write(month / "dmm-api_2026-11-02T09-00-00Z_pod-a.log", "a0\n")
    _handler(tmp_path).merge_days(today=date(2026, 11, 3))
    assert (month / "dmm-api_2026-11-02_pod-a.log").read_text() == "old\n"
    assert (month / "dmm-api_2026-11-02T09-00-00Z_pod-a.log").exists()


# Weekly archive


def test_the_weekly_archive_merges_every_pod_in_time_order(tmp_path):
    october, november = tmp_path / "2026-10", tmp_path / "2026-11"
    _write(
        october / "dmm-api_2026-10-30_pod-a.log",
        _entry("2026-10-30T10:00:00.000Z", "a1"),
        _entry("2026-10-30T12:00:00.000Z", "a3"),
    )
    _write(
        october / "dmm-api_2026-10-30_pod-b.log",
        _entry("2026-10-30T11:00:00.000Z", "b2"),
        "not json\n",
        _entry("2026-10-30T13:00:00.000Z", "b4"),
    )
    # Covered by pod-a's daily file: left out, but deleted.
    _write(
        october / "dmm-api_2026-10-30T10-00-00Z_pod-a.log",
        _entry("2026-10-30T10:00:00.000Z", "a1 again"),
    )
    # A pod that died before its daily merge.
    _write(
        november / "dmm-api_2026-11-01T09-00-00Z_pod-c.log",
        _entry("2026-11-01T09:00:00.000Z", "c5"),
    )
    _write(
        november / "dmm-api_2026-11-03_pod-a.log",
        _entry("2026-11-03T09:00:00.000Z", "now"),
    )

    assert run(tmp_path, date(2026, 11, 4)) == 0

    archive = october / "dmm-api_week_2026-10-26.log.gz"
    with gzip.open(archive, "rt") as f:
        lines = f.read().splitlines()
    assert [
        json.loads(line)["event"] if line.startswith("{") else line for line in lines
    ] == [
        "a1",
        "b2",
        "not json",
        "a3",
        "b4",
        "c5",
    ]
    assert _files(tmp_path) == [
        "2026-10/dmm-api_week_2026-10-26.log.gz",
        "2026-11/dmm-api_2026-11-03_pod-a.log",
    ]


def test_an_existing_archive_leaves_the_week_untouched(tmp_path):
    october = tmp_path / "2026-10"
    _write(
        october / "dmm-api_2026-10-30_pod-a.log",
        _entry("2026-10-30T10:00:00.000Z", "a"),
    )
    october.joinpath("dmm-api_week_2026-10-26.log.gz").write_bytes(b"existing")
    assert run(tmp_path, date(2026, 11, 4)) == 1
    assert (
        october.joinpath("dmm-api_week_2026-10-26.log.gz").read_bytes() == b"existing"
    )
    assert october.joinpath("dmm-api_2026-10-30_pod-a.log").exists()


def test_nothing_to_archive_is_not_an_error(tmp_path):
    assert run(tmp_path, date(2026, 11, 4)) == 0


# Requests


@pytest.fixture(scope="module")
def client():
    router = APIRouter()

    @router.get("/test-logging/fail/{status_code}")
    async def fail(status_code: int):
        if status_code == 424:
            raise FailedDependencyException(
                source="MoMa", status_code=502, detail="MoMa returned an error."
            )
        raise HTTPException(status_code=status_code, detail="failed")

    @router.get("/test-logging/dict")
    async def fail_with_dict():
        raise HTTPException(status_code=409, detail={"code": 409, "error": "conflict"})

    @router.get("/test-logging/crash")
    async def crash():
        raise RuntimeError("boom")

    app.include_router(router)
    yield TestClient(app)
    app.router.routes[:] = [
        r
        for r in app.router.routes
        if not getattr(r, "path", "").startswith("/test-logging")
    ]


def test_a_request_without_the_header_gets_a_new_id(client):
    response = client.get("/api/v1")
    uuid.UUID(response.headers["x-tracking-correlation"])


def test_a_valid_header_is_echoed_back(client):
    response = client.get("/api/v1", headers={"x-tracking-correlation": "abc.DEF_1-2"})
    assert response.headers["x-tracking-correlation"] == "abc.DEF_1-2"


@pytest.mark.parametrize("value", ["has space", "a" * 129, "new\\nline"])
def test_an_invalid_header_is_replaced(client, value):
    response = client.get("/api/v1", headers={"x-tracking-correlation": value})
    uuid.UUID(response.headers["x-tracking-correlation"])


def test_an_error_body_carries_the_correlation_id(client):
    response = client.get(
        "/test-logging/fail/404", headers={"x-tracking-correlation": "id-1"}
    )
    assert response.json() == {"code": 404, "error": "failed", "correlationId": "id-1"}
    response = client.get(
        "/test-logging/dict", headers={"x-tracking-correlation": "id-2"}
    )
    assert response.json() == {
        "code": 409,
        "error": "conflict",
        "correlationId": "id-2",
    }


def test_an_unhandled_exception_gives_a_json_500(client):
    with capture_logs() as logs:
        response = client.get(
            "/test-logging/crash", headers={"x-tracking-correlation": "id-3"}
        )
    assert response.status_code == 500
    assert response.json() == {
        "code": 500,
        "error": "Internal server error",
        "correlationId": "id-3",
    }
    assert response.headers["x-tracking-correlation"] == "id-3"
    assert [e["event"] for e in logs] == ["unhandled exception", "request finished"]


@pytest.mark.parametrize(
    ("path", "status_code", "outcome", "level"),
    [
        ("/api/v1", 200, "success", "info"),
        ("/test-logging/fail/404", 404, "client_error", "warning"),
        ("/test-logging/fail/424", 424, "server_error", "error"),
        ("/test-logging/crash", 500, "server_error", "error"),
    ],
)
def test_the_request_summary(client, path, status_code, outcome, level):
    with capture_logs() as logs:
        response = client.get(path)
    assert response.status_code == status_code
    (summary,) = [e for e in logs if e["event"] == "request finished"]
    assert summary["log_level"] == level
    assert summary["outcome"] == outcome
    assert summary["status_code"] == status_code
    assert summary["method"] == "GET"
    assert summary["path"] == path
    assert summary["duration_ms"] >= 0
