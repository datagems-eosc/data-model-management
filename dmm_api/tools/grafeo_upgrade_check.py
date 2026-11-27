"""Check that a candidate Grafeo version can replace the one a deployment runs.

Before any Grafeo version change other than a patch (before 1.0 a minor bump
counts as major), the deployment restores one backup of its Grafeo data into
two test instances: the version it runs ("old", the reference) and the
candidate ("new"). This module runs against both, twice: phase `first`, then,
after the deployment restarts the new instance on the same data, phase
`after-restart`. The restart matters: the first start of a new version may
convert the files, and every later start opens the converted ones.

It shows that the candidate reads the data the current version wrote, answers
the service's own queries as the current version does, and is not noticeably
slower. Where the service reads or writes, it runs the service's own code, not
a copy of its queries: the module variables that hold Grafeo's URL and headers
are pointed at one instance at a time. Nothing here names a Grafeo version.

It writes: test APs, links and transactions, under ids starting with
`upgrade-test-`, and load for a minute. Point it only at two test instances
restored from one backup, never at a Grafeo in use. As a guard, it writes only
when both instances start with the same data, as two copies of one backup do;
a Grafeo in use that has taken no write since the backup passes that test too.

    python -m dmm_api.tools.grafeo_upgrade_check --phase first
    python -m dmm_api.tools.grafeo_upgrade_check --phase after-restart

The settings come from the environment (`Settings.from_env`). The output is
one line per check, then `RESULT: PASS` or `RESULT: FAIL`. The exit status is
0 when every check passes, 1 when one fails, and 2 on a setup error or when
the deadline passes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import statistics
import sys
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

import requests
import structlog

# Every id this module writes starts with this, so the comparisons can leave
# the test data out, and the test instances alone ever hold it.
TEST_PREFIX = "upgrade-test-"
CONCURRENCY_PREFIX = TEST_PREFIX + "conc-"
BENCH_PREFIX = TEST_PREFIX + "bench-"

# The modules holding the URL and headers the service's Grafeo calls use. A
# test checks that no other module holds a copy.
SERVICE_MODULES = ("dmm_api.tools.AP.log_AP", "dmm_api.resources.dataset")

# Keys Grafeo uses for its internal ids, which may differ between versions.
_INTERNAL_KEYS = frozenset({"_id", "_source", "_target"})
# Set from the service's clock when it writes, so they differ between the two
# copies of the same test data.
_WRITE_TIME_KEYS = frozenset({"created_at", "updated_at"})

_MAX_DIFFERENCES = 5
_MAX_DETAIL_CHARS = 300
_AP_BATCH = 100
_SAMPLE = 5


class SetupError(Exception):
    """A missing setting, an unreachable instance: nothing was compared."""


@dataclass(frozen=True)
class Settings:
    old_url: str
    new_url: str
    old_token: str | None
    new_token: str | None
    readers: int = 8
    writers: int = 2
    concurrency_seconds: float = 60
    bench_warmup: int = 5
    bench_repeats: int = 30
    bench_max_ratio: float = 1.5
    bench_min_delta_ms: float = 5
    # The benchmark repeats the full dump on both instances, which on a large
    # database takes minutes.
    deadline_seconds: float = 1800
    request_timeout_seconds: float = 60

    @classmethod
    def from_env(cls, environ: dict[str, str]) -> Settings:
        if environ.get("GRAFEO_URL"):
            raise SetupError(
                "GRAFEO_URL is set: unset it, so that a service call this check "
                "does not redirect fails instead of reaching that Grafeo"
            )
        missing = [
            n for n in ("GRAFEO_OLD_URL", "GRAFEO_NEW_URL") if not environ.get(n)
        ]
        if missing:
            raise SetupError(f"missing: {', '.join(missing)}")
        old_url = environ["GRAFEO_OLD_URL"].strip().rstrip("/")
        new_url = environ["GRAFEO_NEW_URL"].strip().rstrip("/")
        if old_url == new_url:
            raise SetupError("GRAFEO_OLD_URL and GRAFEO_NEW_URL are the same")

        def number(name: str, kind: type, default: float) -> Any:
            raw = environ.get(name)
            if raw is None or raw.strip() == "":
                return default
            try:
                value = kind(raw)
            except ValueError:
                raise SetupError(f"{name} is not a number: {raw!r}") from None
            if value <= 0:
                raise SetupError(f"{name} must be above 0: {raw!r}")
            return value

        return cls(
            old_url=old_url,
            new_url=new_url,
            old_token=environ.get("GRAFEO_OLD_AUTH_TOKEN") or None,
            new_token=environ.get("GRAFEO_NEW_AUTH_TOKEN") or None,
            readers=number("CONCURRENCY_READERS", int, cls.readers),
            writers=number("CONCURRENCY_WRITERS", int, cls.writers),
            concurrency_seconds=number(
                "CONCURRENCY_SECONDS", float, cls.concurrency_seconds
            ),
            bench_warmup=number("BENCH_WARMUP", int, cls.bench_warmup),
            bench_repeats=number("BENCH_REPEATS", int, cls.bench_repeats),
            bench_max_ratio=number("BENCH_MAX_RATIO", float, cls.bench_max_ratio),
            bench_min_delta_ms=number(
                "BENCH_MIN_DELTA_MS", float, cls.bench_min_delta_ms
            ),
            deadline_seconds=number(
                "CHECK_DEADLINE_SECONDS", float, cls.deadline_seconds
            ),
            request_timeout_seconds=number(
                "REQUEST_TIMEOUT_SECONDS", float, cls.request_timeout_seconds
            ),
        )


@dataclass
class Check:
    name: str
    # None is a check that did not apply, printed as SKIP.
    ok: bool | None
    reason: str
    details: list[str] = field(default_factory=list)

    def lines(self) -> list[str]:
        word = {True: "PASS", False: "FAIL", None: "SKIP"}[self.ok]
        return [f"{word} {self.name}: {self.reason}"] + [
            f"    {d}" for d in self.details
        ]


def canonical(value: Any) -> str:
    """JSON with sorted keys, so equal data gives equal text and types stay
    apart: 1, 1.0, "1" and true are four different values."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )


def without_keys(value: Any, keys: frozenset[str]) -> Any:
    if isinstance(value, dict):
        return {k: without_keys(v, keys) for k, v in value.items() if k not in keys}
    if isinstance(value, list):
        return [without_keys(v, keys) for v in value]
    return value


def sorted_labels(value: Any) -> Any:
    """Labels in a fixed order: Grafeo does not keep the order they were
    written in across versions, and no client may rely on it."""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            is_labels = (
                key in ("labels", "_labels")
                and isinstance(item, list)
                and all(isinstance(label, str) for label in item)
            )
            result[key] = sorted(item) if is_labels else sorted_labels(item)
        return result
    if isinstance(value, list):
        return [sorted_labels(v) for v in value]
    return value


def _excerpt(old: str, new: str) -> tuple[str, str]:
    """Both texts around the first character where they part."""
    at = next(
        (i for i, (a, b) in enumerate(zip(old, new)) if a != b), min(len(old), len(new))
    )
    start = max(0, at - 60)

    def cut(text: str) -> str:
        part = text[start : at + 100]
        return ("..." if start else "") + part + ("..." if at + 100 < len(text) else "")

    return cut(old), cut(new)


def _shorten(line: str) -> str:
    return line if len(line) <= _MAX_DETAIL_CHARS else line[:_MAX_DETAIL_CHARS] + "..."


def mentions_test_data(value: Any) -> bool:
    if isinstance(value, str):
        return value.startswith(TEST_PREFIX)
    if isinstance(value, dict):
        return any(mentions_test_data(v) for v in value.values())
    if isinstance(value, list):
        return any(mentions_test_data(v) for v in value)
    return False


def differences(
    old: Iterable[str], new: Iterable[str], keyed: bool = False
) -> list[str]:
    """The first entries found on one side only, counting duplicates. With
    `keyed`, entries are `key: value`, and a key on both sides with different
    values is shown side by side, from where the two part."""
    old_count, new_count = Counter(old), Counter(new)
    only_old = sorted((old_count - new_count).elements())
    only_new = sorted((new_count - old_count).elements())

    def key(entry: str) -> str:
        return entry.split(": ", 1)[0]

    paired = {key(e) for e in only_old} & {key(e) for e in only_new} if keyed else set()
    lines = []
    for k in sorted(paired)[:_MAX_DIFFERENCES]:
        old_entry = next(e for e in only_old if key(e) == k)
        new_entry = next(e for e in only_new if key(e) == k)
        old_part, new_part = _excerpt(old_entry, new_entry)
        lines += [f"{k} differs:", f"  old: {old_part}", f"  new: {new_part}"]
    rest_old = [e for e in only_old if key(e) not in paired]
    rest_new = [e for e in only_new if key(e) not in paired]
    lines += [_shorten(f"only in old: {e}") for e in rest_old[:_MAX_DIFFERENCES]]
    lines += [_shorten(f"only in new: {e}") for e in rest_new[:_MAX_DIFFERENCES]]
    if len(only_old) > _MAX_DIFFERENCES or len(only_new) > _MAX_DIFFERENCES:
        lines.append(f"{len(only_old)} only in old, {len(only_new)} only in new")
    return lines


def compare(
    name: str, what: str, old: list[str], new: list[str], keyed: bool = False
) -> Check:
    if Counter(old) == Counter(new):
        return Check(name, True, f"{len(old)} {what}, the same on both")
    return Check(
        name,
        False,
        f"{len(old)} {what} on old, {len(new)} on new, not the same",
        differences(old, new, keyed),
    )


def compare_ordered(name: str, what: str, old: list[str], new: list[str]) -> Check:
    if old == new:
        return Check(name, True, f"{len(old)} {what} in the same order on both")
    index = next(
        (i for i, (a, b) in enumerate(zip(old, new)) if a != b), min(len(old), len(new))
    )
    details = [
        f"first difference at row {index}",
        f"old: {old[index] if index < len(old) else '(none)'}",
        f"new: {new[index] if index < len(new) else '(none)'}",
    ]
    return Check(name, False, f"{what} differ", details)


def bench_verdict(
    old_ms: list[float], new_ms: list[float], max_ratio: float, min_delta_ms: float
) -> tuple[bool, float, float, float]:
    """Whether the new median is within bounds, and the two medians and ratio.

    It fails only when the new median is above the old one by more than the
    ratio and by more than the floor, so that tiny queries cannot flap."""
    old_median, new_median = statistics.median(old_ms), statistics.median(new_ms)
    ratio = new_median / old_median if old_median > 0 else math.inf
    slower = new_median > old_median * max_ratio and (
        new_median - old_median > min_delta_ms
    )
    return not slower, old_median, new_median, ratio


def p95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


class Instance:
    """One Grafeo instance, for this module's own queries. They go through a
    session of their own, so the service's calls can be told apart."""

    def __init__(self, name: str, url: str, token: str | None, timeout: float):
        self.name = name
        self.url = url
        self.token = token
        self.timeout = timeout
        self._session = requests.Session()

    def headers(self, token: str | None = None, *, no_token: bool = False) -> dict:
        headers = {"Content-Type": "application/json"}
        token = token if token is not None else self.token
        if token and not no_token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def post(self, path: str, body: dict, headers: dict | None = None):
        return self._session.post(
            self.url + path,
            json=body,
            headers=headers if headers is not None else self.headers(),
            timeout=self.timeout,
        )

    def query(self, cypher: str, session_id: str | None = None) -> dict:
        headers = self.headers()
        path = "/cypher"
        if session_id:
            headers["X-Session-Id"] = session_id
            path = "/tx/query"
        reply = self.post(path, {"query": cypher}, headers)
        if reply.status_code != 200:
            raise RuntimeError(
                f"{self.name}: {path} answered {reply.status_code}: {reply.text[:200]}"
            )
        return reply.json()

    def rows(self, cypher: str) -> list[list]:
        return self.query(cypher).get("rows", [])

    def health(self) -> dict | None:
        reply = self._session.get(
            self.url + "/health", headers=self.headers(), timeout=self.timeout
        )
        return reply.json() if reply.status_code == 200 else None

    def tx(self, action: str, session_id: str | None = None) -> dict:
        headers = self.headers()
        if session_id:
            headers["X-Session-Id"] = session_id
        reply = self.post(f"/tx/{action}", {}, headers)
        if reply.status_code != 200:
            raise RuntimeError(
                f"{self.name}: /tx/{action} answered {reply.status_code}: "
                f"{reply.text[:200]}"
            )
        return reply.json()


class ServiceBinding:
    """Points the service's Grafeo calls at one instance at a time.

    The service reads Grafeo's URL and headers into module variables at
    import; this sets them before each call. Every request the service sends
    passes through `_post`, which adds a timeout, refuses one that does not go
    to the instance in use, and records the replies while `recording` is a
    list."""

    def __init__(self, modules: list, timeout: float):
        self.modules = modules
        self.timeout = timeout
        self.target: Instance | None = None
        self.recording: list[tuple[str, str]] | None = None
        self.stray: list[str] = []
        self._original_post = requests.post
        self._local = threading.local()
        for module in modules:
            for name in ("GRAFEO_URL", "GRAFEO_HEADERS"):
                if not hasattr(module, name):
                    raise SetupError(
                        f"{module.__name__} has no {name}: this check no longer "
                        "matches the service's code"
                    )

    def install(self) -> None:
        requests.post = self._post

    def uninstall(self) -> None:
        requests.post = self._original_post

    def use(self, instance: Instance) -> None:
        for module in self.modules:
            module.GRAFEO_URL = instance.url
            module.GRAFEO_HEADERS = instance.headers()
        self.target = instance

    def _post(self, url: str, *args, **kwargs):
        if self.target is None or not url.startswith(self.target.url + "/"):
            self.stray.append(url)
            raise SetupError(
                f"the service sent a request to {url}, not to the instance in use"
            )
        kwargs.setdefault("timeout", self.timeout)
        # The service opens a connection per request. Under the concurrency
        # check's load that runs the client out of local ports long before
        # Grafeo is stressed, so each thread reuses a connection of its own.
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._local.session = requests.Session()
        reply = session.post(url, *args, **kwargs)
        if self.recording is not None and reply.status_code == 200:
            query = (kwargs.get("json") or {}).get("query", "")
            data = reply.json()
            # The service's queries do not leave the test data out; this does.
            rows = [r for r in data.get("rows", []) if not mentions_test_data(r)]
            rows = sorted_labels(without_keys(rows, _INTERNAL_KEYS))
            self.recording.append(
                (" ".join(query.split()), canonical(sorted(rows, key=canonical)))
            )
        return reply

    def check_no_stray(self) -> None:
        if self.stray:
            raise SetupError(
                f"the service sent {len(self.stray)} request(s) elsewhere, "
                f"first {self.stray[0]}"
            )


def _not_test(variable: str) -> str:
    return f'NOT coalesce(toString({variable}.id), "") STARTS WITH "{TEST_PREFIX}"'


def snapshot(instance: Instance) -> dict[str, list[str]]:
    """Everything in the instance except the test data, as canonical text."""
    nodes = [
        canonical(
            {
                "labels": sorted(row[0].get("_labels", [])),
                "properties": without_keys(row[0], _INTERNAL_KEYS | {"_labels"}),
            }
        )
        for row in instance.rows(f"MATCH (n) WHERE {_not_test('n')} RETURN n")
    ]
    edges = [
        canonical(
            {
                "type": row[2].get("_type"),
                "from": [sorted(row[0]), row[1]],
                "to": [sorted(row[3]), row[4]],
                "properties": without_keys(row[2], _INTERNAL_KEYS | {"_type"}),
            }
        )
        for row in instance.rows(
            f"MATCH (a)-[r]->(b) WHERE {_not_test('a')} AND {_not_test('b')} "
            "RETURN labels(a), a.id, r, labels(b), b.id"
        )
    ]
    labels = instance.rows(
        f"MATCH (n) WHERE {_not_test('n')} UNWIND labels(n) AS label "
        "RETURN label, count(*) AS c ORDER BY label"
    )
    types = instance.rows(
        f"MATCH (a)-[r]->(b) WHERE {_not_test('a')} AND {_not_test('b')} "
        "RETURN type(r) AS t, count(*) AS c ORDER BY t"
    )
    two_hops = instance.rows(
        f"MATCH (a)-[r1]->(b)-[r2]->(c) WHERE {_not_test('a')} AND {_not_test('b')} "
        f"AND {_not_test('c')} "
        "RETURN type(r1) AS first, type(r2) AS second, count(*) AS paths "
        "ORDER BY first, second"
    )
    ordered = instance.rows(
        f"MATCH (n) WHERE n.id IS NOT NULL AND {_not_test('n')} "
        "RETURN n.id AS id, n.created_at AS created ORDER BY created, id"
    )
    return {
        "nodes": nodes,
        "edges": edges,
        "labels": [canonical(r) for r in labels],
        "types": [canonical(r) for r in types],
        "two-hops": [canonical(r) for r in two_hops],
        "ordered": [canonical(r) for r in ordered],
    }


def compare_snapshots(prefix: str, old: dict, new: dict) -> list[Check]:
    return [
        compare(
            f"{prefix}/nodes",
            "nodes, labels and properties",
            old["nodes"],
            new["nodes"],
        ),
        compare(
            f"{prefix}/edges",
            "relationships, endpoints and properties",
            old["edges"],
            new["edges"],
        ),
        compare_ordered(
            f"{prefix}/label-counts", "label counts", old["labels"], new["labels"]
        ),
        compare_ordered(
            f"{prefix}/type-counts",
            "relationship type counts",
            old["types"],
            new["types"],
        ),
        compare_ordered(
            f"{prefix}/two-hop-paths",
            "two-hop path counts",
            old["two-hops"],
            new["two-hops"],
        ),
        compare_ordered(
            f"{prefix}/order-by",
            "ids ordered by created_at, then id",
            old["ordered"],
            new["ordered"],
        ),
    ]


def snapshot_test_data(
    instance: Instance, prefix: str = TEST_PREFIX
) -> tuple[list[str], list[str]]:
    """The test data under `prefix`, except the concurrency and benchmark
    writes, without the write times."""
    keep = (
        f'toString({{v}}.id) STARTS WITH "{prefix}" '
        f'AND NOT toString({{v}}.id) STARTS WITH "{CONCURRENCY_PREFIX}" '
        f'AND NOT toString({{v}}.id) STARTS WITH "{BENCH_PREFIX}"'
    )
    drop = _INTERNAL_KEYS | _WRITE_TIME_KEYS
    nodes = [
        canonical(sorted_labels(without_keys(row[0], drop)))
        for row in instance.rows(f"MATCH (n) WHERE {keep.format(v='n')} RETURN n")
    ]
    edges = [
        canonical([row[0], row[1].get("_type"), without_keys(row[1], drop), row[2]])
        for row in instance.rows(
            f"MATCH (a)-[r]->(b) WHERE {keep.format(v='a')} AND {keep.format(v='b')} "
            "RETURN a.id, r, b.id"
        )
    ]
    return nodes, edges


def synthetic_ap(prefix: str) -> dict:
    """An AP shaped like the ones the service stores, every id under `prefix`."""
    started = "2026-01-01T00:00:00Z"
    nodes = [
        ("user", ["User"], {}),
        (
            "task",
            ["Task"],
            {"name": "upgrade check", "description": "test data", "startTime": started},
        ),
        (
            "ap",
            ["Analytical_Pattern"],
            {
                "name": "upgrade check",
                "description": "test data",
                "process": "upgrade check",
                "startTime": started,
            },
        ),
        (
            "op1",
            ["SQL_Operator", "Query_Operator", "Operator"],
            {
                "name": "first",
                "query": "SELECT 1",
                "queryType": "SQL",
                "startTime": started,
            },
        ),
        (
            "op2",
            ["Query_Operator", "Operator"],
            {"name": "second", "startTime": started},
        ),
        ("dataset", ["sc:Dataset"], {"name": "test dataset"}),
        ("file-in", ["cr:FileObject"], {"name": "in.csv"}),
        ("file-out", ["cr:FileObject", "Data"], {"name": "out.csv"}),
        ("db", ["dg:DatabaseConnection"], {}),
    ]
    edges = [
        ("user", "request", "task", {}),
        ("task", "is_accomplished_by", "ap", {}),
        ("ap", "consist_of", "op1", {"step": "1"}),
        ("ap", "consist_of", "op2", {"step": "2"}),
        ("op1", "follows", "op2", {}),
        ("op1", "input", "file-in", {}),
        ("op1", "output", "file-out", {}),
        ("dataset", "distribution", "file-in", {}),
        ("file-in", "contained_in", "db", {}),
    ]
    return {
        "nodes": [
            {"id": prefix + n, "labels": labels, "properties": props}
            for n, labels, props in nodes
        ],
        "edges": [
            {
                "from": prefix + a,
                "to": prefix + b,
                "labels": [label],
                "properties": props,
            }
            for a, label, b, props in edges
        ],
    }


def normalise_ap(graph: dict, drop: frozenset[str] = frozenset()) -> dict:
    """The service's AP output with nodes and edges sorted: the service builds
    them from rows, whose order Grafeo does not promise."""
    ap = sorted_labels(without_keys(graph.get("ap", graph), drop))
    return {
        "nodes": sorted(ap.get("nodes", []), key=canonical),
        "edges": sorted(ap.get("edges", []), key=canonical),
    }


def outcome(call: Callable[[], Any]) -> Any:
    """A call's result, or what it raised, so both can be compared."""
    from fastapi import HTTPException

    try:
        return ("result", call())
    except HTTPException as exc:
        return ("http", exc.status_code, canonical(exc.detail))
    except SetupError:
        raise
    except Exception as exc:  # noqa: BLE001: compared, not handled
        return ("error", type(exc).__name__, str(exc)[:200])


def datasets_by_user(log_AP: Any, user: str) -> Any:
    """The outcome of get_datasets_by_user, its ids sorted: its query has no
    ORDER BY, so they come in Grafeo's row order, which two versions need not
    share. A list, not a set, so that a duplicated id still shows."""
    return outcome(lambda: sorted(log_AP.get_datasets_by_user(user), key=canonical))


class Run:
    def __init__(self, settings: Settings, out=sys.stdout):
        self.settings = settings
        self.out = out
        self.failed = False
        self.step = "starting"
        # Set by `data`: whether both instances hold the same data, test data
        # aside. The steps that write run only then.
        self.same_data = False
        # log_AP refuses to load without a token. This module sets the headers
        # itself, so the value only has to exist.
        os.environ.setdefault("GRAFEO_AUTH_TOKEN", "unused-by-the-upgrade-check")
        try:
            from dmm_api.resources import dataset
            from dmm_api.tools.AP import log_AP, parse_AP
        except Exception as exc:  # noqa: BLE001: reported as a setup error
            # dataset.py asks for the temporary directory at import, so it
            # fails, for one, on a read-only root without a writable /tmp.
            raise SetupError(
                f"the service's modules do not load: {type(exc).__name__}: {exc}"
            ) from None

        self.dataset, self.log_AP, self.parse_AP = dataset, log_AP, parse_AP
        timeout = settings.request_timeout_seconds
        self.old = Instance("old", settings.old_url, settings.old_token, timeout)
        self.new = Instance("new", settings.new_url, settings.new_token, timeout)
        self.binding = ServiceBinding([log_AP, dataset], timeout)
        self.run_id = "r" + uuid.uuid4().hex[:8]

    @property
    def both(self) -> tuple[Instance, Instance]:
        return self.old, self.new

    def report(self, check: Check) -> None:
        if check.ok is False:
            self.failed = True
        for line in check.lines():
            print(line, file=self.out, flush=True)

    def run_step(self, name: str, step: Callable[[], Iterable[Check]]) -> None:
        self.step = name
        try:
            for check in step():
                self.report(check)
            self.binding.check_no_stray()
        except (SetupError, requests.ConnectionError, requests.Timeout):
            raise
        except Exception as exc:  # noqa: BLE001: reported as that step's failure
            self.report(Check(name, False, f"{type(exc).__name__}: {str(exc)[:300]}"))

    def run_writing_step(self, name: str, step: Callable[[], Iterable[Check]]) -> None:
        """`run_step` for a step that writes: only on two copies of the same
        data, as the module's description says."""
        if not self.same_data:
            self.report(
                Check(
                    name,
                    None,
                    "not run, since it writes: the instances do not start with "
                    "the same data, so they may not be two copies of one backup",
                )
            )
            return
        self.run_step(name, step)

    # Check 1
    def reachable(self) -> Iterable[Check]:
        versions = []
        for instance in self.both:
            try:
                health = instance.health()
                rows = instance.rows("RETURN 1 AS ok")
            except requests.RequestException as exc:
                raise SetupError(
                    f"{instance.name} ({instance.url}) unreachable: {exc}"
                ) from None
            except RuntimeError as exc:
                raise SetupError(str(exc)) from None
            if rows != [[1]]:
                raise SetupError(f"{instance.name} answered RETURN 1 with {rows!r}")
            version = (health or {}).get("version", "version not reported")
            versions.append(f"{instance.name} {version} at {instance.url}")
        yield Check("reachable", True, "; ".join(versions))

    # Check 2, and 6 in phase after-restart
    def data(self) -> Iterable[Check]:
        checks = compare_snapshots("data", snapshot(self.old), snapshot(self.new))
        self.same_data = all(check.ok for check in checks)
        yield from checks

    def service_reads(self, instance: Instance) -> dict[str, list[str]]:
        dataset, log_AP = self.dataset, self.log_AP
        self.binding.use(instance)
        self.binding.recording = []
        try:
            ap_ids = self._ids(
                f"MATCH (n:Analytical_Pattern) WHERE {_not_test('n')} RETURN n.id"
            )
            users = self._ids(f"MATCH (n:User) WHERE {_not_test('n')} RETURN n.id")
            datasets = self._ids(
                f"MATCH (n) WHERE {_not_test('n')} AND any(l IN labels(n) WHERE l ENDS WITH 'Dataset') RETURN n.id"
            )[:_SAMPLE]
            files = self._ids(
                f"MATCH (n) WHERE {_not_test('n')} AND any(l IN labels(n) WHERE l ENDS WITH 'FileObject') RETURN n.id"
            )[:_SAMPLE]
            reads: dict[str, list[str]] = {}
            reads["grafeo-test"] = [
                canonical(outcome(lambda: asyncio.run(dataset.grafeo_test())))
            ]
            aplogs = []
            for start in range(0, len(ap_ids), _AP_BATCH):
                batch = ap_ids[start : start + _AP_BATCH]
                graphs = dataset.get_full_aplogs(batch, None)
                aplogs += [f"{i}: {canonical(normalise_ap(graphs[i]))}" for i in batch]
            reads["aplogs"] = aplogs
            reads["datasets-by-user"] = [
                f"{u}: {canonical(datasets_by_user(log_AP, u))}" for u in users
            ]
            variants: list[tuple[str, dict]] = [("all", {})]
            variants += [(f"userId={u}", {"userId": [u]}) for u in users[:_SAMPLE]]
            variants += [
                (f"operator={op.value}", {"operator": [op.value]})
                for op in dataset.OPERATOR_LABELS
            ]
            variants += [(f"datasetId={d}", {"datasetId": [d]}) for d in datasets]
            variants += [(f"fileObjectId={f}", {"fileObjectId": [f]}) for f in files]
            variants.append(
                ("dates", {"startDate": ["2000-01-01"], "endDate": ["2100-01-01"]})
            )
            reads["aplog-search"] = [
                f"{name}: {canonical(outcome(lambda p=params: self._search(p)))}"
                for name, params in variants
            ]
            reads["raw-replies"] = [reply for _query, reply in self.binding.recording]
            return reads
        finally:
            self.binding.recording = None

    def _ids(self, cypher: str) -> list[str]:
        return sorted(
            str(r[0]) for r in self.old.rows(cypher) if r and r[0] is not None
        )

    def _search(self, params: dict, limit: int = 10**9) -> Any:
        """The AP search route, called directly. The default limit returns
        every match: with a smaller one, which APs make the cut among equal
        start times is up to Grafeo's row order."""
        arguments = {
            "userId": None,
            "startDate": None,
            "endDate": None,
            "operator": None,
            "datasetId": None,
            "fileObjectId": None,
        }
        arguments.update(params)
        reply = asyncio.run(
            self.dataset.search_APs(
                **arguments, limit=limit, token="", token_payload={}
            )
        )
        content = reply.model_dump()
        # The search does not leave the test data out, and the concurrency
        # writes are on new only. The count is then of what is left, and the
        # total, of every match, goes.
        kept = [a for a in content.get("aplogs", []) if not mentions_test_data(a)]
        content["aplogs"] = sorted((normalise_ap(a) for a in kept), key=canonical)
        content["count"] = len(kept)
        content.pop("total", None)
        return content

    def reads(self) -> Iterable[Check]:
        old, new = self.service_reads(self.old), self.service_reads(self.new)
        what = {
            "grafeo-test": "answers of /grafeo/test",
            "aplogs": "APs read with get_full_aplogs",
            "datasets-by-user": "users read with get_datasets_by_user",
            "aplog-search": "AP searches",
            "raw-replies": "raw Grafeo replies to the service's reads",
        }
        for key, label in what.items():
            keyed = key in ("aplogs", "datasets-by-user", "aplog-search")
            yield compare(f"reads/{key}", label, old[key], new[key], keyed)

    # Check 3
    def writes(self) -> Iterable[Check]:
        log_AP, dataset = self.log_AP, self.dataset
        prefix = f"{TEST_PREFIX}{self.run_id}-"
        ap = self.parse_AP.APRequest.model_validate(synthetic_ap(prefix))
        user, owned = prefix + "link-user", prefix + "link-dataset"
        read_back, again, links, by_user = {}, {}, {}, {}
        for instance in self.both:
            name = instance.name
            self.binding.use(instance)
            log_AP.store_AP_in_grafeo(ap)
            graphs = dataset.get_full_aplogs([prefix + "ap"], None)
            read_back[name] = canonical(
                normalise_ap(graphs[prefix + "ap"], _WRITE_TIME_KEYS)
            )
            before = self._raw_test_data(instance, prefix)
            repeated = outcome(lambda: log_AP.store_AP_in_grafeo(ap))
            refused = repeated[0] == "error" and "AP already exist" in repeated[2]
            unchanged = self._raw_test_data(instance, prefix) == before
            again[name] = (refused, unchanged, repeated)
            links[name] = self._link_twice(instance, user, owned)
            by_user[name] = canonical(datasets_by_user(log_AP, user))
        yield compare(
            "writes/ap-read-back",
            "APs written and read back",
            [read_back["old"]],
            [read_back["new"]],
        )
        yield self._expectation(
            "writes/ap-stored-again",
            {
                n: (
                    r and u,
                    f"refused: {r}, data unchanged: {u}, got {canonical(o)[:150]}",
                )
                for n, (r, u, o) in again.items()
            },
            "storing the same AP again is refused and changes nothing",
        )
        yield self._expectation(
            "writes/link-added-again",
            links,
            "adding the same user-dataset link again keeps one link and created_at, and moves updated_at",
        )
        yield compare(
            "writes/datasets-by-user",
            "answers for the test user",
            [by_user["old"]],
            [by_user["new"]],
        )

    def _raw_test_data(self, instance: Instance, prefix: str) -> str:
        nodes = instance.rows(
            f'MATCH (n) WHERE toString(n.id) STARTS WITH "{prefix}" RETURN n'
        )
        edges = instance.rows(
            f'MATCH (a)-[r]->(b) WHERE toString(a.id) STARTS WITH "{prefix}" RETURN a.id, r, b.id'
        )
        return canonical(
            sorted(canonical(without_keys(r, _INTERNAL_KEYS)) for r in nodes + edges)
        )

    def _link_twice(
        self, instance: Instance, user: str, owned: str
    ) -> tuple[bool, str]:
        def state() -> tuple[int, list]:
            count = instance.rows(
                f'MATCH (u {{id: "{user}"}})-[r:OWNED_BY]->(d {{id: "{owned}"}}) RETURN count(r)'
            )[0][0]
            times = instance.rows(
                f'MATCH (n) WHERE n.id IN ["{user}", "{owned}"] '
                "RETURN n.id AS id, n.created_at, n.updated_at ORDER BY id"
            )
            return count, times

        self.log_AP.add_dataset_user_connection(user, owned)
        count_1, times_1 = state()
        # The service's timestamps have millisecond resolution.
        time.sleep(0.01)
        self.log_AP.add_dataset_user_connection(user, owned)
        count_2, times_2 = state()
        created_kept = [t[1] for t in times_1] == [t[1] for t in times_2]
        updated_moved = len(times_2) == 2 and all(
            b[2] is not None and a[2] is not None and b[2] > a[2]
            for a, b in zip(times_1, times_2)
        )
        ok = count_1 == 1 and count_2 == 1 and created_kept and updated_moved
        return ok, (
            f"links {count_1} then {count_2}, created_at kept: {created_kept}, "
            f"updated_at moved: {updated_moved}"
        )

    def _expectation(
        self, name: str, results: dict[str, tuple[bool, str]], what: str
    ) -> Check:
        """The candidate must meet it. That the reference does not is reported,
        not failed: it is what the deployment runs today."""
        new_ok, new_detail = results["new"]
        old_ok, old_detail = results["old"]
        if not new_ok:
            return Check(
                name,
                False,
                f"new does not: {what}",
                [f"new: {new_detail}", f"old: {old_detail}"],
            )
        if not old_ok:
            return Check(
                name, True, f"{what}, on new; old does not", [f"old: {old_detail}"]
            )
        return Check(name, True, f"{what}, on both")

    # Check 4
    def transactions(self) -> Iterable[Check]:
        prefix = f"{TEST_PREFIX}{self.run_id}-"
        results = {}
        for instance in self.both:
            count = 'MATCH (n {{id: "{0}"}}) RETURN count(n)'
            session = instance.tx("begin")["session_id"]
            instance.query(
                f'CREATE (:UpgradeTest {{id: "{prefix}tx-rolled-back"}})', session
            )
            hidden = instance.rows(count.format(prefix + "tx-rolled-back"))[0][0] == 0
            instance.tx("rollback", session)
            gone = instance.rows(count.format(prefix + "tx-rolled-back"))[0][0] == 0
            session = instance.tx("begin")["session_id"]
            instance.query(
                f'CREATE (:UpgradeTest {{id: "{prefix}tx-committed"}})', session
            )
            instance.tx("commit", session)
            kept = instance.rows(count.format(prefix + "tx-committed"))[0][0] == 1
            results[instance.name] = (
                hidden and gone and kept,
                f"uncommitted write hidden: {hidden}, gone after rollback: {gone}, kept after commit: {kept}",
            )
        yield self._expectation(
            "transactions/grafeo",
            results,
            "Grafeo's /tx API hides, rolls back and commits writes",
        )
        # The service starts its transactions elsewhere, and falls back to
        # /cypher, without one, when that answers 404.
        statuses = {
            i.name: i.post("/transaction/start", {}).status_code for i in self.both
        }
        same = statuses["old"] == statuses["new"]
        mode = "without transactions" if statuses["new"] == 404 else "in transactions"
        yield Check(
            "transactions/service",
            same,
            f"/transaction/start answers {statuses['old']} on old and "
            f"{statuses['new']} on new: the service would write {mode} on new",
        )

    # Check 5
    def concurrency(self) -> Iterable[Check]:
        s = self.settings
        log_AP, dataset = self.log_AP, self.dataset
        ap_ids = self._ids(
            f"MATCH (n:Analytical_Pattern) WHERE {_not_test('n')} RETURN n.id"
        )[:20]
        users = self._ids(f"MATCH (n:User) WHERE {_not_test('n')} RETURN n.id")[:20]
        if not ap_ids:
            ap_ids = [f"{TEST_PREFIX}{self.run_id}-ap"]
        if not users:
            users = [f"{TEST_PREFIX}{self.run_id}-link-user"]
        self.binding.use(self.new)
        stop = time.monotonic() + s.concurrency_seconds
        errors: list[str] = []
        done = Counter()

        def reader(n: int) -> None:
            reads = [
                lambda: dataset.get_full_aplogs(ap_ids, None),
                lambda: log_AP.get_datasets_by_user(users[n % len(users)]),
                lambda: dataset.run_grafeo_query(
                    "MATCH (n) UNWIND labels(n) AS label RETURN label, count(*) AS c"
                ),
            ]
            i = 0
            while time.monotonic() < stop:
                try:
                    reads[i % len(reads)]()
                    done["reads"] += 1
                except Exception as exc:  # noqa: BLE001: counted and reported
                    errors.append(f"reader {n}: {type(exc).__name__}: {str(exc)[:200]}")
                i += 1

        def writer(n: int) -> None:
            i = 0
            while time.monotonic() < stop:
                prefix = f"{CONCURRENCY_PREFIX}{self.run_id}-w{n}-{i}-"
                try:
                    log_AP.store_AP_in_grafeo(
                        self.parse_AP.APRequest.model_validate(synthetic_ap(prefix))
                    )
                    log_AP.add_dataset_user_connection(
                        prefix + "user", prefix + "dataset"
                    )
                    done["writes"] += 1
                except Exception as exc:  # noqa: BLE001: counted and reported
                    errors.append(f"writer {n}: {type(exc).__name__}: {str(exc)[:200]}")
                i += 1

        with ThreadPoolExecutor(max_workers=s.readers + s.writers) as pool:
            futures = [pool.submit(reader, n) for n in range(s.readers)]
            futures += [pool.submit(writer, n) for n in range(s.writers)]
            for future in futures:
                future.result()
        summary = (
            f"{s.readers} readers and {s.writers} writers for "
            f"{s.concurrency_seconds:g} s on new: {done['reads']} reads, "
            f"{done['writes']} AP writes, {len(errors)} errors"
        )
        yield Check("concurrency/run", not errors, summary, errors[:_MAX_DIFFERENCES])
        yield from compare_snapshots(
            "concurrency/data-after", snapshot(self.old), snapshot(self.new)
        )
        # These writes are on new only. Left there, they would make every
        # later query on new scan more data than on old, the benchmark's too.
        self.new.query(
            f'MATCH (n) WHERE toString(n.id) STARTS WITH "{CONCURRENCY_PREFIX}" '
            "DETACH DELETE n"
        )
        left = self.new.rows(
            f'MATCH (n) WHERE toString(n.id) STARTS WITH "{CONCURRENCY_PREFIX}" '
            "RETURN count(n)"
        )[0][0]
        yield Check(
            "concurrency/cleanup",
            left == 0,
            f"the concurrency writes deleted from new, {left} nodes left",
        )

    # Check 6, the test data part
    def test_data_kept(self) -> Iterable[Check]:
        old_nodes, old_edges = snapshot_test_data(self.old)
        new_nodes, new_edges = snapshot_test_data(self.new)
        if not old_nodes:
            yield Check(
                "test-data", False, "none found on old: run phase first before this one"
            )
            return
        yield compare(
            "test-data/nodes", "test nodes from phase first", old_nodes, new_nodes
        )
        yield compare(
            "test-data/edges",
            "test relationships from phase first",
            old_edges,
            new_edges,
        )

    # Check 7
    def auth(self) -> Iterable[Check]:
        for instance in self.both:
            name = f"auth/{instance.name}"
            if not instance.token:
                variable = f"GRAFEO_{instance.name.upper()}_AUTH_TOKEN"
                yield Check(name, None, f"{variable} is not set")
                continue
            body = {"query": "RETURN 1 AS ok"}
            without = instance.post(
                "/cypher", body, instance.headers(no_token=True)
            ).status_code
            wrong = instance.post(
                "/cypher", body, instance.headers(instance.token + "-wrong")
            ).status_code
            right = instance.post("/cypher", body).status_code
            ok = without in (401, 403) and wrong in (401, 403) and right == 200
            yield Check(
                name,
                ok,
                f"no token: {without}, wrong token: {wrong}, right token: {right}",
            )

    # Check 8
    def benchmark(self) -> Iterable[Check]:
        s = self.settings
        log_AP, dataset = self.log_AP, self.dataset
        ap_ids = self._ids(
            f"MATCH (n:Analytical_Pattern) WHERE {_not_test('n')} RETURN n.id"
        )[:20]
        users = self._ids(f"MATCH (n:User) WHERE {_not_test('n')} RETURN n.id")
        ap_ids = (
            ap_ids
            or self._ids(
                f'MATCH (n:Analytical_Pattern) WHERE toString(n.id) STARTS WITH "{TEST_PREFIX}" RETURN n.id'
            )[:20]
        )
        user = (users or [f"{BENCH_PREFIX}{self.run_id}-nobody"])[0]
        stores = Counter()

        def store(instance: Instance) -> None:
            stores[instance.name] += 1
            prefix = (
                f"{BENCH_PREFIX}{self.run_id}-{instance.name}-{stores[instance.name]}-"
            )
            log_AP.store_AP_in_grafeo(
                self.parse_AP.APRequest.model_validate(synthetic_ap(prefix))
            )

        operations: dict[str, Callable[[Instance], Any]] = {
            "grafeo-test": lambda i: asyncio.run(dataset.grafeo_test()),
            "aplogs": lambda i: dataset.get_full_aplogs(ap_ids, None),
            "aplog-search": lambda i: self._search({}, limit=20),
            "datasets-by-user": lambda i: log_AP.get_datasets_by_user(user),
            "full-dump": lambda i: (
                i.rows("MATCH (n) RETURN n"),
                i.rows("MATCH (a)-[r]->(b) RETURN a.id, r, b.id"),
            ),
            "two-hop": lambda i: i.rows(
                "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN type(r1), type(r2), count(*)"
            ),
            "ap-store": store,
        }

        def timed(operation: Callable, instance: Instance) -> float:
            self.binding.use(instance)
            started = time.perf_counter()
            operation(instance)
            return (time.perf_counter() - started) * 1000

        table = ["operation         old median  old p95  new median  new p95   new/old"]
        slow = []
        for name, operation in operations.items():
            self.step = f"benchmark {name}"
            for _ in range(s.bench_warmup):
                for instance in self.both:
                    timed(operation, instance)
            times: dict[str, list[float]] = {"old": [], "new": []}
            for repeat in range(s.bench_repeats):
                # Alternate who goes first, so both share any noise.
                order = self.both if repeat % 2 == 0 else self.both[::-1]
                for instance in order:
                    times[instance.name].append(timed(operation, instance))
            ok, old_median, new_median, ratio = bench_verdict(
                times["old"], times["new"], s.bench_max_ratio, s.bench_min_delta_ms
            )
            table.append(
                f"{name:<17} {old_median:10.1f} {p95(times['old']):8.1f} "
                f"{new_median:11.1f} {p95(times['new']):8.1f} {ratio:9.2f}"
                + ("" if ok else "  slower")
            )
            if not ok:
                slow.append(name)
        reason = (
            f"medians in ms over {s.bench_repeats} runs each; fails above "
            f"{s.bench_max_ratio:g}x and {s.bench_min_delta_ms:g} ms slower"
        )
        if slow:
            reason += f"; slower on new: {', '.join(slow)} (rerun before concluding)"
        yield Check("benchmark", not slow, reason, table)

    def phase_first(self) -> None:
        self.run_step("reachable", self.reachable)
        self.run_step("data", self.data)
        self.run_step("reads", self.reads)
        self.run_writing_step("writes", self.writes)
        self.run_writing_step("transactions", self.transactions)
        self.run_writing_step("concurrency", self.concurrency)

    def phase_after_restart(self) -> None:
        self.run_step("reachable", self.reachable)
        self.run_step("data", self.data)
        self.run_step("reads", self.reads)
        self.run_step("test-data", self.test_data_kept)
        self.run_step("auth", self.auth)
        # The benchmark stores APs too.
        self.run_writing_step("benchmark", self.benchmark)


def _quiet_service_logs() -> None:
    """The service's own log lines go to stderr, errors only: stdout is the
    report, and the service warns on every call it makes without a
    transaction."""
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.ERROR),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _start_deadline(seconds: float, run_holder: dict, out) -> threading.Timer:
    def expire() -> None:
        run = run_holder.get("run")
        step = run.step if run else "setup"
        print(
            f"FAIL deadline: no result after {seconds:g} s, during {step}",
            file=out,
            flush=True,
        )
        print("RESULT: FAIL", file=out, flush=True)
        os._exit(2)

    timer = threading.Timer(seconds, expire)
    timer.daemon = True
    timer.start()
    return timer


def main(
    argv: list[str] | None = None, environ: dict[str, str] | None = None, out=None
) -> int:
    out = out or sys.stdout
    parser = argparse.ArgumentParser(
        prog="python -m dmm_api.tools.grafeo_upgrade_check",
        description="Compare a candidate Grafeo version with the running one.",
    )
    parser.add_argument("--phase", required=True, choices=("first", "after-restart"))
    args = parser.parse_args(argv)
    run_holder: dict[str, Run] = {}
    timer = None
    try:
        settings = Settings.from_env(dict(os.environ if environ is None else environ))
        timer = _start_deadline(settings.deadline_seconds, run_holder, out)
        _quiet_service_logs()
        run = Run(settings, out)
        run_holder["run"] = run
        run.binding.install()
        try:
            print(f"phase {args.phase}, run {run.run_id}", file=out, flush=True)
            if args.phase == "first":
                run.phase_first()
            else:
                run.phase_after_restart()
        finally:
            run.binding.uninstall()
    except (SetupError, requests.ConnectionError, requests.Timeout) as exc:
        print(f"FAIL setup: {exc}", file=out, flush=True)
        print("RESULT: FAIL", file=out, flush=True)
        return 2
    finally:
        if timer is not None:
            timer.cancel()
    print(f"RESULT: {'FAIL' if run.failed else 'PASS'}", file=out, flush=True)
    return 1 if run.failed else 0


if __name__ == "__main__":
    sys.exit(main())
