"""Weekly archive of the log files, run by a Kubernetes CronJob.

The pods write plain chunk and daily files under `LOGS_DIR`. Once a week has
ended, this merges every pod's files for that week into one gzip file in
timestamp order, checks it, and deletes the inputs. `LOGGING.md` describes the
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
from dmm_api.logging_config import CHUNK_RE, DAILY_RE, configure_logging

logger = structlog.get_logger(__name__)


def _monday(day: date) -> date:
    return day - timedelta(days=day.weekday())


def find_weeks(logs_dir: Path, today: date) -> dict[date, list[Path]]:
    """Group the plain log files of every finished week by the week's Monday."""
    current = _monday(today)
    weeks: dict[date, list[Path]] = {}
    for path in sorted(logs_dir.glob("*/dmm-api_*.log")):
        match = CHUNK_RE.match(path.name) or DAILY_RE.match(path.name)
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


def archive_week(logs_dir: Path, monday: date, paths: list[Path]) -> bool:
    """Write the week's archive and delete its inputs; False on any error."""
    archive = logs_dir / f"{monday:%Y-%m}" / f"dmm-api_week_{monday.isoformat()}.log.gz"
    if archive.exists():
        logger.error(
            "weekly log archive already exists, week left untouched",
            archive=str(archive),
        )
        return False

    # A daily file already holds its pod's chunks for that day. Such chunks
    # are left out of the archive but deleted with the rest.
    dailies = {(m["pod"], m["day"]) for p in paths if (m := DAILY_RE.match(p.name))}
    sources = [
        p
        for p in paths
        if not ((m := CHUNK_RE.match(p.name)) and (m["pod"], m["day"]) in dailies)
    ]

    written = 0
    try:
        with gzip.open(archive, "xt", encoding="utf-8") as out:
            merged = heapq.merge(*(_entries(p) for p in sources), key=lambda e: e[0])
            for _, line in merged:
                out.write(line + "\n")
                written += 1
        with gzip.open(archive, "rt", encoding="utf-8") as f:
            read_back = sum(1 for _ in f)
    except OSError as exc:
        logger.error(
            "could not write the weekly log archive",
            archive=str(archive),
            error=str(exc),
        )
        return False
    if read_back != written:
        logger.error(
            "weekly log archive check failed, inputs kept",
            archive=str(archive),
            written=written,
            read_back=read_back,
        )
        return False

    ok = True
    for path in paths:
        try:
            path.unlink()
        except OSError as exc:
            logger.error(
                "could not delete an archived log file", file=str(path), error=str(exc)
            )
            ok = False
    logger.info(
        "archived a week of logs",
        archive=str(archive),
        files=len(paths),
        entries=written,
    )
    return ok


def run(logs_dir: Path, today: date) -> int:
    weeks = find_weeks(logs_dir, today)
    if not weeks:
        logger.info("no finished week to archive", logs_dir=str(logs_dir))
        return 0
    results = [archive_week(logs_dir, m, paths) for m, paths in sorted(weeks.items())]
    return 0 if all(results) else 1


def main() -> int:
    configure_logging(write_files=False)
    return run(Path(config.LOGS_DIR), datetime.now(timezone.utc).date())


if __name__ == "__main__":
    sys.exit(main())
