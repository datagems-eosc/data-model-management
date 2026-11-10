"""Logging to stdout and to log files on a disk claim.

stdout goes to the platform's log pipeline, which keeps it for a short time
only. So every entry is also appended to one file per pod and UTC day under
`LOGS_DIR`, a volume that outlives the pod, and a weekly job
(`dmm_api.tools.log_archive`) moves each finished week to S3 as one gzip file.
`LOGGING.md` describes the files and the entry format.
"""

import json
import logging
import os
import queue
import re
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import IO, Any, Iterator

import structlog

from dmm_api import config

# The fields every entry starts with. `timestamp`, `level`, `msg`,
# `DGCorrelationId`, `UserId` and `ClientId` are the json-cf-2 format of the
# DataGEMS logging service, which makes them searchable there. Its parser
# lowercases every key, so a log call must not pass a key that equals one of
# these once lowercased.
FIXED_FIELDS = (
    "timestamp",
    "level",
    "msg",
    "logger",
    "app_version",
    "pod",
    "DGCorrelationId",
    "UserId",
    "ClientId",
)

LOG_FILE_RE = re.compile(
    r"^dmm-api_(?P<day>\d{4}-\d{2}-\d{2})_(?P<pod>[A-Za-z0-9.-]+)\.log$"
)

CORRELATION_HEADER = "x-tracking-correlation"
CORRELATION_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,128}")

# Problems with the log files themselves go to stdout only, so that a failing
# disk cannot feed its own error messages back into the file queue.
_FILES_LOGGER_NAME = "dmm_api.logging_config.files"
_files_logger = structlog.get_logger(_FILES_LOGGER_NAME)

# At most this many entries wait for the file writer; past that, new entries
# reach stdout only.
MAX_QUEUED = 50_000
# How long shutdown waits for the file writer, well within the 30 s Kubernetes
# gives a stopping pod, so that a hung disk cannot hold the pod until it is
# killed.
CLOSE_SECONDS = 5.0

_handlers: list[logging.Handler] = []
_queue_handler: "_FileQueueHandler | None" = None
_writer: "DailyFileWriter | None" = None
_writer_thread: threading.Thread | None = None
_STOP = object()


@dataclass
class RequestContext:
    correlation_id: str
    user_id: str | None = None
    client_id: str | None = None


# A mutable holder rather than separate context variables: the caller is
# recorded inside the endpoint, which runs in a copy of the middleware's
# context, and the middleware still has to see it for the request summary.
_request: ContextVar[RequestContext | None] = ContextVar("request", default=None)


def correlation_id_from_header(value: str | None) -> str:
    """Return the header value if it is a safe ID, otherwise a new UUID4."""
    if value and CORRELATION_ID_RE.fullmatch(value):
        return value
    return str(uuid.uuid4())


@contextmanager
def request_context(correlation_id: str) -> Iterator[RequestContext]:
    """Attach `correlation_id` to every entry logged inside the block."""
    token = _request.set(RequestContext(correlation_id))
    try:
        yield _request.get()
    finally:
        _request.reset(token)


def get_correlation_id() -> str | None:
    context = _request.get()
    return context.correlation_id if context else None


def set_caller(user_id: str | None, client_id: str | None) -> None:
    """Record the caller's user and client IDs for the rest of the request."""
    context = _request.get()
    if context:
        context.user_id = user_id
        context.client_id = client_id


def _timestamp(created: float) -> str:
    moment = datetime.fromtimestamp(created, timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def _add_fixed_fields(_, method_name: str, event_dict: dict) -> dict:
    """Put the fixed fields first, then the call's own, then `exception`."""
    record = event_dict.get("_record")
    context = _request.get()
    entry: dict[str, Any] = {
        "timestamp": _timestamp(record.created if record else time.time()),
        "level": record.levelname.lower() if record else method_name,
        "msg": str(event_dict.get("event", "")),
        "logger": record.name if record else "",
        "app_version": config.APP_VERSION,
        "pod": config.POD_NAME,
        "DGCorrelationId": context.correlation_id if context else None,
        "UserId": context.user_id if context else None,
        "ClientId": context.client_id if context else None,
    }
    # structlog's `event` is the message, already written as `msg`.
    for key, value in event_dict.items():
        if key not in entry and key not in (
            "event",
            "exception",
            "_record",
            "_from_structlog",
        ):
            entry[key] = value
    if "exception" in event_dict:
        entry["exception"] = event_dict["exception"]
    return entry


def _render_json(_, __, event_dict: dict) -> str:
    text = json.dumps(event_dict, default=str, ensure_ascii=False)
    # A lone surrogate, as a JSON body may carry, cannot be encoded as UTF-8.
    # Written as `\udXXX`, it stays a valid JSON escape.
    return text.encode("utf-8", "backslashreplace").decode("utf-8")


def json_formatter() -> logging.Formatter:
    return structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.processors.format_exc_info,
            _add_fixed_fields,
            _render_json,
        ]
    )


class _StdoutHandler(logging.StreamHandler):
    """Write to whatever `sys.stdout` is at the time, as `print()` does."""

    @property
    def stream(self):
        return sys.stdout

    @stream.setter
    def stream(self, _):
        pass


class DailyFileWriter:
    """Append each entry to this pod's file for the entry's UTC day.

    Used by the file writer's thread only.
    """

    def __init__(self, logs_dir: Path, pod: str):
        self.logs_dir = logs_dir
        self.pod = pod
        self._day: date | None = None
        self._file: IO[bytes] | None = None
        self._missed = 0

    def write(self, created: float, line: str) -> None:
        day = datetime.fromtimestamp(created, timezone.utc).date()
        try:
            if day != self._day:
                self._open(day)
            self._file.write(line.encode("utf-8") + b"\n")
            self._file.flush()
        except OSError as exc:
            self._close()
            if not self._missed:
                _files_logger.warning(
                    "could not write the log file, entries reach stdout only",
                    error=str(exc),
                )
            self._missed += 1
            return
        if self._missed:
            _files_logger.warning("log file writes resumed", missed=self._missed)
            self._missed = 0

    def _open(self, day: date) -> None:
        self._close()
        path = self.logs_dir / f"dmm-api_{day.isoformat()}_{self.pod}.log"
        self._file = open(path, "a+b")
        # A line cut short, by a crash or a failed write, is ended first, so
        # that the next entry starts on a line of its own.
        size = self._file.seek(0, os.SEEK_END)
        if size:
            self._file.seek(size - 1)
            if self._file.read(1) != b"\n":
                self._file.write(b"\n")
        self._day = day

    def _close(self) -> None:
        if self._file:
            try:
                self._file.close()
            except OSError:
                pass  # Every line was flushed when it was written.
        self._file = None
        self._day = None

    def close(self) -> None:
        self._close()


def _write_queued(file_queue: queue.Queue, writer: DailyFileWriter) -> None:
    """Write queued entries until told to stop.

    This is the only thread that writes the files, so no error may end it.
    """
    while (item := file_queue.get()) is not _STOP:
        try:
            writer.write(*item)
        except Exception:
            _files_logger.exception("log file writer failed, entry not written")


class _FileQueueHandler(logging.Handler):
    """Format each entry and queue it for the file writer's thread.

    Formatting happens here, in the caller's thread, where the request context
    is set. A slow or hung disk then never slows a request. When the queue is
    full, entries are dropped from the files; stdout still has them.
    """

    def __init__(self, file_queue: queue.Queue):
        super().__init__()
        self.queue = file_queue
        self._dropped = 0

    # `Handler.handle` holds the handler's lock around this, so the count
    # needs no lock of its own.
    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:
            self.handleError(record)
            return
        try:
            self.queue.put_nowait((record.created, line))
        except queue.Full:
            if not self._dropped:
                _files_logger.warning(
                    "log file queue full, entries reach stdout only",
                    max_queued=self.queue.maxsize,
                )
            self._dropped += 1
            return
        if self._dropped:
            _files_logger.warning(
                "log file queue has room again", dropped=self._dropped
            )
            self._dropped = 0


def configure_logging(write_files: bool = True) -> None:
    """Send structlog and standard logging to stdout and, if asked, to files."""
    global _queue_handler, _writer, _writer_thread
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )

    close_log_files()
    root = logging.getLogger()
    for handler in _handlers:
        root.removeHandler(handler)
        handler.close()
    _handlers.clear()

    stdout = _StdoutHandler()
    # The platform's log pipeline collects stdout, and parses JSON lines.
    stdout.setFormatter(json_formatter())
    root.addHandler(stdout)
    _handlers.append(stdout)
    root.setLevel(config.LOG_LEVEL)

    files_logger = logging.getLogger(_FILES_LOGGER_NAME)
    files_logger.handlers = [stdout]
    files_logger.propagate = False

    # uvicorn configures its own loggers before it imports the app. Its server
    # messages join the same handlers; its access log is dropped, since the
    # request summary replaces it.
    for name in ("uvicorn", "uvicorn.error"):
        logging.getLogger(name).handlers = []
        logging.getLogger(name).propagate = True
    logging.getLogger("uvicorn.access").disabled = True
    # httpx logs every outgoing request at INFO.
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)

    if not write_files:
        return
    logs_dir = Path(config.LOGS_DIR)
    try:
        logs_dir.mkdir(parents=True, exist_ok=True)
        if not os.access(logs_dir, os.W_OK):
            raise PermissionError(f"{logs_dir} is not writable")
    except OSError as exc:
        structlog.get_logger(__name__).warning(
            "log files disabled, logging to stdout only",
            logs_dir=str(logs_dir),
            error=str(exc),
        )
        return
    file_queue: queue.Queue = queue.Queue(maxsize=MAX_QUEUED)
    _writer = DailyFileWriter(logs_dir, config.POD_NAME)
    _writer_thread = threading.Thread(
        target=_write_queued, args=(file_queue, _writer), name="log-files", daemon=True
    )
    _writer_thread.start()
    _queue_handler = _FileQueueHandler(file_queue)
    _queue_handler.setFormatter(json_formatter())
    root.addHandler(_queue_handler)


def close_log_files() -> None:
    """Write what is still queued and close the file, at shutdown.

    Waits at most `CLOSE_SECONDS` for the queue and as long again for the
    writer. Entries logged after this reach stdout only.
    """
    global _queue_handler, _writer, _writer_thread
    if not _queue_handler:
        return
    handler, writer, thread = _queue_handler, _writer, _writer_thread
    _queue_handler = _writer = _writer_thread = None
    logging.getLogger().removeHandler(handler)
    try:
        handler.queue.put(_STOP, timeout=CLOSE_SECONDS)
    except queue.Full:
        pass  # The writer is stuck; the join below times out and says so.
    thread.join(timeout=CLOSE_SECONDS)
    if thread.is_alive():
        _files_logger.warning(
            "log file writer did not finish in time, waiting entries not written",
            waiting=handler.queue.qsize(),
        )
        return
    writer.close()
