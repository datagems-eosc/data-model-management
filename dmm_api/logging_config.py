"""Logging to stdout and to log files on the S3 mount.

The pod's own filesystem is lost when the pod goes, so log files go to the S3
mount. A file there cannot be appended to, and may not be renamable or
overwritable, so every file is written once, under its final name: entries are
buffered and written as a new chunk every `LOG_FLUSH_SECONDS`, and each pod
merges its own chunks of a finished day into one daily file. `LOGGING.md`
describes the file layout and the entry format.
"""

import json
import logging
import os
import re
import shutil
import sys
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from itertools import groupby
from pathlib import Path
from typing import Any, Iterator

import structlog

from dmm_api import config

FIXED_FIELDS = (
    "timestamp",
    "level",
    "event",
    "logger",
    "app_version",
    "pod",
    "correlation_id",
    "user_id",
)

CHUNK_RE = re.compile(
    r"^dmm-api_(?P<day>\d{4}-\d{2}-\d{2})T(?P<time>\d{2}-\d{2}-\d{2})Z"
    r"(?:-(?P<n>\d+))?_(?P<pod>[A-Za-z0-9.-]+)\.log$"
)
DAILY_RE = re.compile(
    r"^dmm-api_(?P<day>\d{4}-\d{2}-\d{2})_(?P<pod>[A-Za-z0-9.-]+)\.log$"
)

CORRELATION_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,128}")

# Problems with the log files themselves go to stdout only, so that a failing
# mount cannot feed its own error messages back into the buffer.
_FILES_LOGGER_NAME = "dmm_api.logging_config.files"
_files_logger = structlog.get_logger(_FILES_LOGGER_NAME)

_handlers: list[logging.Handler] = []
_file_handler: "ChunkFileHandler | None" = None


@dataclass
class RequestContext:
    correlation_id: str
    user_id: str | None = None


# A mutable holder rather than two context variables: the user ID is recorded
# inside the endpoint, which runs in a copy of the middleware's context, and
# the middleware still has to see it for the request summary.
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


def set_user_id(user_id: str | None) -> None:
    context = _request.get()
    if context:
        context.user_id = user_id


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
        "event": str(event_dict.get("event", "")),
        "logger": record.name if record else "",
        "app_version": config.APP_VERSION,
        "pod": config.POD_NAME,
        "correlation_id": context.correlation_id if context else None,
        "user_id": context.user_id if context else None,
    }
    for key, value in event_dict.items():
        if key not in entry and key not in ("exception", "_record", "_from_structlog"):
            entry[key] = value
    if "exception" in event_dict:
        entry["exception"] = event_dict["exception"]
    return entry


def _drop_console_noise(_, __, event_dict: dict) -> dict:
    """Leave out of stdout what is the same on every line, or empty."""
    for key in ("app_version", "pod"):
        event_dict.pop(key, None)
    for key in ("correlation_id", "user_id"):
        if event_dict.get(key) is None:
            event_dict.pop(key, None)
    return event_dict


def _render_json(_, __, event_dict: dict) -> str:
    return json.dumps(event_dict, default=str, ensure_ascii=False)


def json_formatter() -> logging.Formatter:
    return structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.processors.format_exc_info,
            _add_fixed_fields,
            _render_json,
        ]
    )


def console_formatter() -> logging.Formatter:
    return structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.processors.format_exc_info,
            _add_fixed_fields,
            _drop_console_noise,
            structlog.dev.ConsoleRenderer(
                colors=False, exception_formatter=structlog.dev.plain_traceback
            ),
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


class ChunkFileHandler(logging.Handler):
    """Buffer formatted entries and write them to the log folder as chunks.

    Requests only append to the buffer; a background thread does the writing,
    so a slow mount never slows an API call.
    """

    def __init__(
        self,
        logs_dir: Path,
        pod: str,
        flush_seconds: float = 180.0,
        chunk_entries: int = 10_000,
        max_entries: int = 50_000,
    ):
        super().__init__()
        self.logs_dir = logs_dir
        self.pod = pod
        self.flush_seconds = flush_seconds
        self.chunk_entries = chunk_entries
        self.max_entries = max_entries
        self._buffer: deque[tuple[float, str]] = deque()
        self._dropped = 0
        self._buffer_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._merged_days: set[date] = set()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="log-files", daemon=True)
        self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:
            self.handleError(record)
            return
        with self._buffer_lock:
            self._buffer.append((record.created, line))
            self._enforce_cap()
            full = len(self._buffer) >= self.chunk_entries
        if full:
            self._wake.set()

    def _enforce_cap(self) -> None:
        while len(self._buffer) > self.max_entries:
            self._buffer.popleft()
            self._dropped += 1

    def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(self.flush_seconds)
            self._wake.clear()
            # This is the only thread that writes the files, so no error may
            # end it: it is reported, and the next flush tries again.
            try:
                self.write_chunks()
                self.merge_days()
            except Exception:
                _files_logger.exception("log writer failed, retrying at the next flush")

    def write_chunks(self) -> None:
        """Write the buffer as chunks, one per UTC day; keep it on failure."""
        with self._write_lock:
            with self._buffer_lock:
                entries = list(self._buffer)
                self._buffer.clear()
                dropped, self._dropped = self._dropped, 0
            if dropped:
                _files_logger.warning(
                    "log buffer full, oldest entries dropped", dropped=dropped
                )
            days = [
                list(group)
                for _, group in groupby(entries, key=lambda e: _utc_day(e[0]))
            ]
            for index, day_entries in enumerate(days):
                try:
                    self._write_chunk(day_entries)
                except OSError as exc:
                    unwritten = [e for group in days[index:] for e in group]
                    with self._buffer_lock:
                        self._buffer.extendleft(reversed(unwritten))
                        self._enforce_cap()
                    _files_logger.warning(
                        "could not write a log chunk, retrying at the next flush",
                        error=str(exc),
                        entries=len(unwritten),
                    )
                    return

    def _write_chunk(self, entries: list[tuple[float, str]]) -> None:
        first = datetime.fromtimestamp(entries[0][0], timezone.utc)
        folder = self.logs_dir / first.strftime("%Y-%m")
        folder.mkdir(exist_ok=True)
        stem = first.strftime("dmm-api_%Y-%m-%dT%H-%M-%SZ")
        data = "".join(line + "\n" for _, line in entries)
        path = folder / f"{stem}_{self.pod}.log"
        n = 2
        while True:
            try:
                with open(path, "x", encoding="utf-8") as f:
                    f.write(data)
                return
            except FileExistsError:
                path = folder / f"{stem}-{n}_{self.pod}.log"
                n += 1

    def merge_days(self, today: date | None = None) -> None:
        """Merge this pod's chunks of each finished day into a daily file."""
        today = today or datetime.now(timezone.utc).date()
        # A day with entries still in the buffer, after a failed write, is not
        # finished: its last chunk is still to come.
        with self._buffer_lock:
            skip = self._merged_days | {_utc_day(c) for c, _ in self._buffer}
        found: dict[date, list[tuple[str, int, Path]]] = {}
        months = {f"{day:%Y-%m}" for day in (today, today - timedelta(days=1))}
        for month in sorted(months):
            for path in (self.logs_dir / month).glob(f"dmm-api_*_{self.pod}.log"):
                match = CHUNK_RE.match(path.name)
                if not match or match["pod"] != self.pod:
                    continue
                day = date.fromisoformat(match["day"])
                if day < today and day not in skip:
                    found.setdefault(day, []).append(
                        (match["time"], int(match["n"] or 1), path)
                    )
        for day, chunks in sorted(found.items()):
            self._merge_day(day, [path for *_, path in sorted(chunks)])

    def _merge_day(self, day: date, chunks: list[Path]) -> None:
        daily = chunks[0].parent / f"dmm-api_{day.isoformat()}_{self.pod}.log"
        try:
            with open(daily, "xb") as out:
                for chunk in chunks:
                    with open(chunk, "rb") as f:
                        shutil.copyfileobj(f, out)
        except FileExistsError:
            self._merged_days.add(day)
            _files_logger.warning(
                "daily log file already exists, chunks left in place",
                file=daily.name,
            )
            return
        except OSError as exc:
            # A partial daily file would hide the chunks from the weekly
            # archive, so it goes, and the day is tried again at the next flush.
            daily.unlink(missing_ok=True)
            _files_logger.warning(
                "could not write the daily log file", file=daily.name, error=str(exc)
            )
            return
        self._merged_days.add(day)
        try:
            for chunk in chunks:
                chunk.unlink()
        except OSError as exc:
            _files_logger.warning(
                "could not delete merged log chunks", file=daily.name, error=str(exc)
            )

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=10)
        self.write_chunks()
        super().close()


def _utc_day(created: float) -> date:
    return datetime.fromtimestamp(created, timezone.utc).date()


def configure_logging(write_files: bool = True) -> None:
    """Send structlog and standard logging to stdout and, if asked, to files."""
    global _file_handler
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )

    root = logging.getLogger()
    for handler in _handlers:
        root.removeHandler(handler)
        handler.close()
    _handlers.clear()
    _file_handler = None

    stdout = _StdoutHandler()
    stdout.setFormatter(console_formatter())
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
    _file_handler = ChunkFileHandler(
        logs_dir, config.POD_NAME, config.LOG_FLUSH_SECONDS
    )
    _file_handler.setFormatter(json_formatter())
    root.addHandler(_file_handler)
    _handlers.append(_file_handler)
    _file_handler.start()


def write_log_files() -> None:
    """Write the buffered entries now, at shutdown."""
    if _file_handler:
        _file_handler.write_chunks()
