"""Weekly archive of the log files, run by a Kubernetes CronJob.

The API pods append to one file per pod and UTC day under `LOGS_DIR`, a disk
claim. Once a week has ended, this merges every pod's files for that week into
one gzip file under `LOG_ARCHIVE_DIR` on the S3 mount, in timestamp order,
reads it back to check it, and deletes the files. `LOGGING.md` describes the
layout and the CronJob.

Run as `python -m dmm_api.tools.log_archive`; it exits with 1 if any week
failed.
"""

import gzip
import heapq
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import structlog

from dmm_api import config
from dmm_api.logging_config import LOG_FILE_RE, configure_logging

logger = structlog.get_logger(__name__)


def _monday(day: date) -> date:
    return day - timedelta(days=day.weekday())


def find_weeks(logs_dir: Path, today: date) -> dict[date, list[Path]]:
    """Group the log files of every finished week by the week's Monday."""
    current = _monday(today)
    weeks: dict[date, list[Path]] = {}
    for path in sorted(logs_dir.glob("dmm-api_*.log")):
        match = LOG_FILE_RE.match(path.name)
        if not match:
            continue
        monday = _monday(date.fromisoformat(match["day"]))
        if monday < current:
            weeks.setdefault(monday, []).append(path)
    return weeks


def _entries(path: Path) -> Iterator[tuple[str, str]]:
    """Yield (timestamp, line) for each line of a log file.

    A line that is not a JSON entry keeps the timestamp of the line before it,
    so it stays where it was in the file.
    """
    timestamp = ""
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                timestamp = str(json.loads(line)["timestamp"])
            except (ValueError, KeyError, TypeError):
                pass
            yield timestamp, line


def _free_name(archive_dir: Path, monday: date) -> Path:
    """The week's archive name, numbered if an earlier run already took it.

    S3 files cannot be overwritten, so files left by a failed delete, or
    written late, go into a second archive of the same week rather than into
    the first.
    """
    stem = f"dmm-api_week_{monday.isoformat()}"
    path = archive_dir / f"{stem}.log.gz"
    n = 2
    while path.exists():
        path = archive_dir / f"{stem}-{n}.log.gz"
        n += 1
    return path


def _discard(archive: Path) -> None:
    """Delete a failed archive, so that nobody takes it for a whole week."""
    try:
        archive.unlink(missing_ok=True)
    except OSError as exc:
        logger.error(
            "could not delete a failed weekly log archive, the next run writes "
            "another beside it",
            archive=str(archive),
            error=str(exc),
        )


def _delete(sizes: dict[Path, int]) -> bool:
    """Delete the archived files, keeping any that grew after it was read.

    A pod still writing a backlog after a disk outage can append to a file of
    the week being archived; such a file goes into the week's next archive.
    """
    ok = True
    for path, size in sizes.items():
        try:
            if path.stat().st_size != size:
                logger.warning(
                    "log file grew while it was archived, kept for the next run",
                    file=str(path),
                )
                continue
            path.unlink()
        except OSError as exc:
            logger.error(
                "could not delete an archived log file, the next run archives "
                "it again",
                file=str(path),
                error=str(exc),
            )
            ok = False
    return ok


def archive_week(archive_dir: Path, monday: date, paths: list[Path]) -> bool:
    """Write the week's archive and delete its files; False on any error."""
    name: Path | None = None
    # Set once this run has created the file, so that only its own file is
    # ever discarded.
    archive: Path | None = None
    written = 0
    try:
        sizes = {p: p.stat().st_size for p in paths}
        archive_dir.mkdir(parents=True, exist_ok=True)
        name = _free_name(archive_dir, monday)
        with gzip.open(name, "xt", encoding="utf-8") as out:
            archive = name
            merged = heapq.merge(*(_entries(p) for p in paths), key=lambda e: e[0])
            for _, line in merged:
                out.write(line + "\n")
                written += 1
        with gzip.open(archive, "rt", encoding="utf-8") as f:
            read_back = sum(1 for _ in f)
    except (OSError, EOFError) as exc:
        logger.error(
            "could not write the weekly log archive, files kept",
            archive=str(name),
            error=str(exc),
        )
        if archive:
            _discard(archive)
        return False
    if read_back != written:
        logger.error(
            "weekly log archive check failed, files kept",
            archive=str(archive),
            written=written,
            read_back=read_back,
        )
        _discard(archive)
        return False

    ok = _delete(sizes)
    logger.info(
        "archived a week of logs",
        archive=str(archive),
        files=len(paths),
        entries=written,
    )
    return ok


def run(logs_dir: Path, archive_dir: Path, today: date) -> int:
    weeks = find_weeks(logs_dir, today)
    if not weeks:
        logger.info("no finished week to archive", logs_dir=str(logs_dir))
        return 0
    results = [
        archive_week(archive_dir, monday, paths)
        for monday, paths in sorted(weeks.items())
    ]
    return 0 if all(results) else 1


def main() -> int:
    configure_logging(write_files=False)
    return run(
        Path(config.LOGS_DIR),
        Path(config.LOG_ARCHIVE_DIR),
        datetime.now(timezone.utc).date(),
    )


if __name__ == "__main__":
    sys.exit(main())
