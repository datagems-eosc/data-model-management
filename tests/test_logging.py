import ast
import gzip
import io
import json
import logging
import queue
import re
import threading
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
import structlog
from fastapi import APIRouter, HTTPException
from fastapi.testclient import TestClient
from structlog.testing import capture_logs

from dmm_api import config, logging_config
from dmm_api.logging_config import (
    FIXED_FIELDS,
    DailyFileWriter,
    close_log_files,
    configure_logging,
    json_formatter,
    request_context,
    set_user_id,
)
from dmm_api.main import app
from dmm_api.resources.security import FailedDependencyException
from dmm_api.tools import log_archive
from dmm_api.tools.log_archive import run


def _utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _record(when: datetime, message: str = "entry") -> logging.LogRecord:
    record = logging.LogRecord("tests", logging.INFO, __file__, 1, message, None, None)
    record.created = when.timestamp()
    return record


def _line(event: str) -> str:
    return json.dumps({"event": event})


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


# Files


def _at(*args: int) -> float:
    return _utc(*args).timestamp()


def test_entries_go_to_the_pods_file_of_their_day(tmp_path):
    writer = DailyFileWriter(tmp_path, "pod-a")
    writer.write(_at(2026, 11, 2, 23, 59, 59), _line("before"))
    writer.write(_at(2026, 11, 3, 0, 0, 1), _line("after"))
    writer.write(_at(2026, 11, 2, 23, 59, 59, 999000), _line("late"))
    writer.close()
    assert _files(tmp_path) == [
        "dmm-api_2026-11-02_pod-a.log",
        "dmm-api_2026-11-03_pod-a.log",
    ]
    assert _events(tmp_path / "dmm-api_2026-11-02_pod-a.log") == ["before", "late"]
    assert _events(tmp_path / "dmm-api_2026-11-03_pod-a.log") == ["after"]


def test_a_restarted_pod_appends_and_ends_a_cut_line_first(tmp_path):
    path = tmp_path / "dmm-api_2026-11-03_pod-a.log"
    _write(path, _line("old") + "\n", '{"event": "cut')
    writer = DailyFileWriter(tmp_path, "pod-a")
    writer.write(_at(2026, 11, 3, 10), _line("new"))
    writer.close()
    assert path.read_text().splitlines() == [
        _line("old"),
        '{"event": "cut',
        _line("new"),
    ]


def test_a_failed_open_is_reported_once_and_writing_resumes(tmp_path):
    blocked = tmp_path / "blocked"
    blocked.write_text("")
    writer = DailyFileWriter(blocked, "pod-a")
    with capture_logs() as logs:
        writer.write(_at(2026, 11, 3, 10), _line("lost 1"))
        writer.write(_at(2026, 11, 3, 10), _line("lost 2"))
        writer.logs_dir = tmp_path
        writer.write(_at(2026, 11, 3, 10), _line("kept"))
    writer.close()
    assert [e["event"] for e in logs] == [
        "could not write the log file, entries reach stdout only",
        "log file writes resumed",
    ]
    assert logs[1]["missed"] == 2
    assert _events(tmp_path / "dmm-api_2026-11-03_pod-a.log") == ["kept"]


class _FullDisk:
    def write(self, data):
        raise OSError(28, "No space left on device")

    def close(self):
        pass


def test_a_failed_write_to_an_open_file_reopens_it(tmp_path):
    writer = DailyFileWriter(tmp_path, "pod-a")
    writer.write(_at(2026, 11, 3, 10), _line("first"))
    writer._file.close()
    writer._file = _FullDisk()
    with capture_logs() as logs:
        writer.write(_at(2026, 11, 3, 10), _line("lost"))
        writer.write(_at(2026, 11, 3, 10), _line("kept"))
    writer.close()
    assert logs[1]["missed"] == 1
    assert _events(tmp_path / "dmm-api_2026-11-03_pod-a.log") == ["first", "kept"]


def test_no_error_stops_the_file_writer():
    class Writer:
        def __init__(self):
            self.lines = []

        def write(self, created, line):
            if line == "bad":
                raise ValueError("not a log line")
            self.lines.append(line)

    file_queue: queue.Queue = queue.Queue()
    for line in ("bad", "good"):
        file_queue.put((0.0, line))
    file_queue.put(logging_config._STOP)
    writer = Writer()
    with capture_logs() as logs:
        logging_config._write_queued(file_queue, writer)
    assert writer.lines == ["good"]
    assert logs[0]["event"] == "log file writer failed, entry not written"


def test_a_full_queue_drops_entries_and_says_how_many():
    file_queue: queue.Queue = queue.Queue(maxsize=1)
    handler = logging_config._FileQueueHandler(file_queue)
    handler.setFormatter(json_formatter())
    with capture_logs() as logs:
        for event in ("queued", "dropped 1", "dropped 2"):
            handler.handle(_record(_utc(2026, 11, 3, 10), event))
        file_queue.get_nowait()
        handler.handle(_record(_utc(2026, 11, 3, 10), "queued again"))
    assert [e["event"] for e in logs] == [
        "log file queue full, entries reach stdout only",
        "log file queue has room again",
    ]
    assert logs[1]["dropped"] == 2
    _, line = file_queue.get_nowait()
    assert json.loads(line)["event"] == "queued again"


@pytest.fixture
def log_files(tmp_path, monkeypatch):
    """Log to files under `tmp_path`, as the API does, for one test."""
    monkeypatch.setattr(config, "LOGS_DIR", str(tmp_path))
    monkeypatch.setattr(config, "POD_NAME", "pod-a")
    configure_logging()
    yield tmp_path
    monkeypatch.undo()
    configure_logging()


def _file_entries(logs_dir: Path) -> list[dict]:
    (path,) = logs_dir.glob("dmm-api_*_pod-a.log")
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_a_file_entry_carries_the_request_context(log_files):
    with request_context("abc-123"):
        set_user_id("user-1")
        structlog.get_logger("tests.files").warning("inside", dataset_id="d1")
    close_log_files()
    (entry,) = [e for e in _file_entries(log_files) if e["event"] == "inside"]
    assert entry["correlation_id"] == "abc-123"
    assert entry["user_id"] == "user-1"
    assert entry["pod"] == "pod-a"
    assert entry["dataset_id"] == "d1"


def test_a_lone_surrogate_is_written_escaped(log_files):
    logger = structlog.get_logger("tests.files")
    logger.warning("odd", ap={"q": "\ud800"})
    logger.warning("next")
    close_log_files()
    odd, after = [e for e in _file_entries(log_files) if e["event"] in ("odd", "next")]
    assert odd["ap"] == {"q": "\ud800"}
    assert after["event"] == "next"


def test_shutdown_does_not_wait_for_a_hung_disk(log_files, monkeypatch):
    monkeypatch.setattr(logging_config, "CLOSE_SECONDS", 0.1)
    released = threading.Event()
    monkeypatch.setattr(
        logging_config._writer, "write", lambda created, line: released.wait()
    )
    structlog.get_logger("tests.files").warning("stuck")
    started = time.monotonic()
    with capture_logs() as logs:
        close_log_files()
    released.set()
    assert time.monotonic() - started < 1
    assert logs[-1]["event"] == (
        "log file writer did not finish in time, waiting entries not written"
    )


# Weekly archive


def _archived(archive_dir: Path, name: str = "dmm-api_week_2026-10-26.log.gz"):
    with gzip.open(archive_dir / name, "rt") as f:
        return [
            json.loads(line)["event"] if line.startswith("{") else line.rstrip("\n")
            for line in f
        ]


def test_the_weekly_archive_merges_every_pod_in_time_order(tmp_path):
    logs, s3 = tmp_path / "logs", tmp_path / "s3"
    _write(
        logs / "dmm-api_2026-10-30_pod-a.log",
        _entry("2026-10-30T10:00:00.000Z", "a1"),
        _entry("2026-10-30T12:00:00.000Z", "a3"),
    )
    _write(
        logs / "dmm-api_2026-10-30_pod-b.log",
        _entry("2026-10-30T11:00:00.000Z", "b2"),
        "not json\n",
        _entry("2026-10-30T13:00:00.000Z", "b4"),
    )
    _write(
        logs / "dmm-api_2026-11-01_pod-c.log",
        _entry("2026-11-01T09:00:00.000Z", "c5"),
    )
    # The current week: left alone.
    _write(
        logs / "dmm-api_2026-11-03_pod-a.log",
        _entry("2026-11-03T09:00:00.000Z", "now"),
    )

    assert run(logs, s3, date(2026, 11, 4)) == 0

    assert _archived(s3) == ["a1", "b2", "not json", "a3", "b4", "c5"]
    assert _files(logs) == ["dmm-api_2026-11-03_pod-a.log"]
    assert _files(s3) == ["dmm-api_week_2026-10-26.log.gz"]


def test_a_failed_archive_is_deleted_and_the_files_kept(tmp_path, monkeypatch):
    logs, s3 = tmp_path / "logs", tmp_path / "s3"
    _write(
        logs / "dmm-api_2026-10-30_pod-a.log",
        _entry("2026-10-30T10:00:00.000Z", "a"),
    )

    def fail_midway(path):
        yield "2026-10-30T10:00:00.000Z", _line("written")
        raise OSError("disk gone")

    with monkeypatch.context() as patch:
        patch.setattr(log_archive, "_entries", fail_midway)
        assert run(logs, s3, date(2026, 11, 4)) == 1
    assert _files(s3) == []
    assert _files(logs) == ["dmm-api_2026-10-30_pod-a.log"]

    assert run(logs, s3, date(2026, 11, 4)) == 0
    assert _archived(s3) == ["a"]


def test_a_taken_archive_name_gets_a_number(tmp_path):
    logs, s3 = tmp_path / "logs", tmp_path / "s3"
    _write(s3 / "dmm-api_week_2026-10-26.log.gz", "left by an earlier run")
    _write(
        logs / "dmm-api_2026-10-30_pod-a.log",
        _entry("2026-10-30T23:00:00.000Z", "late"),
    )
    assert run(logs, s3, date(2026, 11, 4)) == 0
    assert _archived(s3, "dmm-api_week_2026-10-26-2.log.gz") == ["late"]
    assert (s3 / "dmm-api_week_2026-10-26.log.gz").read_text() == (
        "left by an earlier run"
    )


def test_a_file_that_could_not_be_deleted_is_archived_again(tmp_path, monkeypatch):
    logs, s3 = tmp_path / "logs", tmp_path / "s3"
    path = logs / "dmm-api_2026-10-30_pod-a.log"
    _write(path, _entry("2026-10-30T10:00:00.000Z", "a"))

    def refuse(self, missing_ok=False):
        raise PermissionError("deletes not allowed")

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", refuse)
        assert run(logs, s3, date(2026, 11, 4)) == 1
    assert path.exists()

    assert run(logs, s3, date(2026, 11, 4)) == 0
    assert not path.exists()
    assert _files(s3) == [
        "dmm-api_week_2026-10-26-2.log.gz",
        "dmm-api_week_2026-10-26.log.gz",
    ]


def test_a_file_that_grows_while_archived_is_kept(tmp_path, monkeypatch):
    logs, s3 = tmp_path / "logs", tmp_path / "s3"
    path = logs / "dmm-api_2026-11-01_pod-a.log"
    _write(path, _entry("2026-11-01T23:00:00.000Z", "first"))
    read = log_archive._entries

    def append_after_reading(p):
        yield from read(p)
        with open(p, "a") as f:
            f.write(_entry("2026-11-01T23:59:59.000Z", "backlog"))

    with monkeypatch.context() as patch:
        patch.setattr(log_archive, "_entries", append_after_reading)
        assert run(logs, s3, date(2026, 11, 4)) == 0
    assert _archived(s3) == ["first"]
    assert path.exists()

    assert run(logs, s3, date(2026, 11, 4)) == 0
    assert _archived(s3, "dmm-api_week_2026-10-26-2.log.gz") == ["first", "backlog"]
    assert not path.exists()


def test_nothing_to_archive_is_not_an_error(tmp_path):
    assert run(tmp_path, tmp_path / "s3", date(2026, 11, 4)) == 0


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
