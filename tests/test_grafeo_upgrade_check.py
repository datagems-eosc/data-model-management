"""The Grafeo upgrade check's own logic, with Grafeo stubbed out.

The comparisons against two real Grafeo versions are not run here: they need
both servers. These tests cover what decides a result without them: the
settings, where the service's calls go, how results are compared, the
benchmark's verdict, the auth check and the exit status.
"""

import ast
import io
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from dmm_api.tools import grafeo_upgrade_check as check
from dmm_api.tools.AP.log_AP import AP_to_Grafeo
from dmm_api.tools.AP.parse_AP import APRequest

URLS = {"GRAFEO_OLD_URL": "http://old:7474", "GRAFEO_NEW_URL": "http://new:7474"}


def test_settings_refuse_a_missing_url():
    with pytest.raises(check.SetupError) as error:
        check.Settings.from_env({})
    assert str(error.value) == "missing: GRAFEO_OLD_URL, GRAFEO_NEW_URL"


def test_settings_refuse_grafeo_url():
    with pytest.raises(check.SetupError) as error:
        check.Settings.from_env({**URLS, "GRAFEO_URL": "http://grafeo:7474"})
    assert str(error.value).startswith("GRAFEO_URL is set")


def test_settings_refuse_the_same_url_twice():
    with pytest.raises(check.SetupError) as error:
        check.Settings.from_env(
            {"GRAFEO_OLD_URL": "http://a:7474/", "GRAFEO_NEW_URL": "http://a:7474"}
        )
    assert str(error.value) == "GRAFEO_OLD_URL and GRAFEO_NEW_URL are the same"


def test_settings_refuse_a_setting_that_is_not_a_number():
    with pytest.raises(check.SetupError) as error:
        check.Settings.from_env({**URLS, "BENCH_REPEATS": "many"})
    assert str(error.value) == "BENCH_REPEATS is not a number: 'many'"


def test_settings_defaults_and_overrides():
    settings = check.Settings.from_env(
        {
            **URLS,
            "GRAFEO_NEW_AUTH_TOKEN": "t",
            "GRAFEO_OLD_AUTH_TOKEN": "",
            "BENCH_MAX_RATIO": "2",
        }
    )
    assert settings == check.Settings(
        old_url="http://old:7474",
        new_url="http://new:7474",
        old_token=None,
        new_token="t",
        bench_max_ratio=2.0,
    )


def _module_level_grafeo_names(path: Path) -> set[str]:
    names = set()
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign):
            names |= {t.id for t in node.targets if isinstance(t, ast.Name)}
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.ImportFrom):
            names |= {alias.asname or alias.name for alias in node.names}
    return names & {"GRAFEO_URL", "GRAFEO_HEADERS"}


def test_the_binding_covers_every_module_holding_grafeo_settings():
    # A module holding its own copy of the URL or headers would keep pointing
    # at the default while the check compares instances.
    package = Path(check.__file__).resolve().parents[1]
    holders = {
        ".".join(path.relative_to(package.parent).with_suffix("").parts)
        for path in package.rglob("*.py")
        if _module_level_grafeo_names(path)
    }
    assert holders == set(check.SERVICE_MODULES)


def test_the_binding_refuses_a_module_without_the_settings():
    with pytest.raises(check.SetupError):
        check.ServiceBinding([SimpleNamespace(__name__="m", GRAFEO_URL="x")], 5)


class _Reply:
    status_code = 200

    def json(self):
        return {
            "columns": ["n"],
            "rows": [
                [{"_id": 7, "_labels": ["B", "A"], "id": "x"}],
                [{"_id": 8, "_labels": ["A"], "id": "upgrade-test-r1-ap"}],
            ],
            "execution_time_ms": 1.5,
        }


def test_the_binding_points_the_service_at_one_instance(monkeypatch):
    posted = []

    class Session:
        def post(self, url, *args, **kwargs):
            posted.append((url, kwargs))
            return _Reply()

    monkeypatch.setattr(check.requests, "Session", Session)
    module = SimpleNamespace(__name__="m", GRAFEO_URL="unset", GRAFEO_HEADERS={})
    binding = check.ServiceBinding([module], timeout=5)
    new = check.Instance("new", "http://new:7474", "tok", timeout=5)
    binding.use(new)
    assert module.GRAFEO_URL == "http://new:7474"
    assert module.GRAFEO_HEADERS == {
        "Content-Type": "application/json",
        "Authorization": "Bearer tok",
    }

    binding.recording = []
    binding._post("http://new:7474/cypher", json={"query": "MATCH (n)\n  RETURN n"})
    assert posted == [
        (
            "http://new:7474/cypher",
            {"json": {"query": "MATCH (n)\n  RETURN n"}, "timeout": 5},
        )
    ]
    # Internal ids and test data left out, labels sorted, the query on one line.
    assert binding.recording == [
        ("MATCH (n) RETURN n", '[[{"_labels":["A","B"],"id":"x"}]]')
    ]

    with pytest.raises(check.SetupError):
        binding._post("http://old:7474/cypher", json={"query": "RETURN 1"})
    assert binding.stray == ["http://old:7474/cypher"]
    assert len(posted) == 1


def test_compare_ignores_order_but_not_types():
    same = check.compare("x", "rows", ['{"i":1}', '{"i":2}'], ['{"i":2}', '{"i":1}'])
    assert (same.ok, same.reason) == (True, "2 rows, the same on both")

    differ = check.compare("x", "rows", ['{"i":1}'], ['{"i":"1"}'])
    assert differ.ok is False
    assert differ.details == ['only in old: {"i":1}', 'only in new: {"i":"1"}']


def test_compare_shows_a_keyed_difference_side_by_side():
    result = check.compare("x", "users", ['u1: ["a"]'], ['u1: ["b"]'], keyed=True)
    assert result.details == ["u1 differs:", '  old: u1: ["a"]', '  new: u1: ["b"]']


def test_compare_ordered_reports_the_first_difference():
    result = check.compare_ordered("x", "ids", ["a", "b", "c"], ["a", "c", "b"])
    assert result.ok is False
    assert result.details == ["first difference at row 1", "old: b", "new: c"]


def test_an_ap_compares_equal_whatever_the_order_of_nodes_and_labels():
    first = {
        "ap": {
            "nodes": [
                {"id": "a", "labels": ["Operator", "SQL_Operator"]},
                {"id": "b", "labels": ["Task"]},
            ],
            "edges": [{"from": "a", "to": "b", "labels": ["x"]}],
        }
    }
    second = {
        "ap": {
            "nodes": [
                {"id": "b", "labels": ["Task"]},
                {"id": "a", "labels": ["SQL_Operator", "Operator"]},
            ],
            "edges": [{"from": "a", "to": "b", "labels": ["x"]}],
        }
    }
    assert check.normalise_ap(first) == check.normalise_ap(second)
    first["ap"]["nodes"][1]["labels"] = ["User"]
    assert check.normalise_ap(first) != check.normalise_ap(second)


def test_write_times_are_left_out_only_when_asked():
    graph = {"nodes": [{"id": "a", "properties": {"created_at": "t1", "name": "n"}}]}
    assert check.normalise_ap(graph, check._WRITE_TIME_KEYS) == {
        "nodes": [{"id": "a", "properties": {"name": "n"}}],
        "edges": [],
    }
    assert check.normalise_ap(graph)["nodes"][0]["properties"]["created_at"] == "t1"


@pytest.mark.parametrize(
    ("old_ms", "new_ms", "ok"),
    [
        # Twice as slow, but by 2 ms only: under the floor.
        ([2.0], [4.0], True),
        # Twice as slow and by 10 ms: fails.
        ([10.0], [20.0], False),
        # 100 ms slower, but 1.4 times only: under the ratio.
        ([250.0], [350.0], True),
    ],
)
def test_the_benchmark_fails_only_past_both_the_ratio_and_the_floor(old_ms, new_ms, ok):
    assert check.bench_verdict(old_ms, new_ms, max_ratio=1.5, min_delta_ms=5)[0] is ok


def test_the_synthetic_ap_is_one_the_service_stores():
    prefix = "upgrade-test-r1-"
    ap = APRequest.model_validate(check.synthetic_ap(prefix))
    assert all(str(n.id).startswith(prefix) for n in ap.nodes)
    check_queries, writes = AP_to_Grafeo(ap)
    assert sorted(check_queries) == ["Analytical_Pattern", "Operator", "Task"]
    assert len(writes) == len(ap.nodes) + len(ap.edges)


class _Instance:
    def __init__(self, name, token, statuses):
        self.name, self.token, self.statuses, self.seen = name, token, statuses, []

    def headers(self, token=None, *, no_token=False):
        return {"token": None if no_token else (token or self.token)}

    def post(self, path, body, headers=None):
        token = (headers or self.headers())["token"]
        self.seen.append(token)
        kind = "right" if token == self.token else "wrong" if token else "none"
        return SimpleNamespace(status_code=self.statuses[kind])


def _auth(old, new):
    run = object.__new__(check.Run)
    run.old, run.new = old, new
    return list(run.auth())


def test_auth_passes_when_only_the_right_token_is_accepted():
    new = _Instance("new", "t", {"none": 401, "wrong": 403, "right": 200})
    old = _Instance("old", None, {})
    results = _auth(old, new)
    assert [(c.name, c.ok) for c in results] == [("auth/old", None), ("auth/new", True)]
    assert new.seen == [None, "t-wrong", "t"]


def test_auth_fails_when_a_request_without_a_token_is_accepted():
    new = _Instance("new", "t", {"none": 200, "wrong": 401, "right": 200})
    results = _auth(_Instance("old", None, {}), new)
    assert results[1].ok is False
    assert results[1].reason == "no token: 200, wrong token: 401, right token: 200"


class _FakeRun:
    """Stands in for `Run`, reporting one check the way `Run` does."""

    outcome = True
    report = check.Run.report

    def __init__(self, settings, out):
        self.out, self.failed, self.run_id, self.step = out, False, "r0", ""
        self.binding = SimpleNamespace(install=lambda: None, uninstall=lambda: None)

    def phase_first(self):
        self.report(check.Check("a", self.outcome, "why"))


@pytest.mark.parametrize(
    ("outcome", "status", "lines"),
    [
        (True, 0, ["phase first, run r0", "PASS a: why", "RESULT: PASS"]),
        (False, 1, ["phase first, run r0", "FAIL a: why", "RESULT: FAIL"]),
    ],
)
def test_main_ends_with_the_result_and_its_exit_status(
    monkeypatch, outcome, status, lines
):
    monkeypatch.setattr(check, "_quiet_service_logs", lambda: None)
    monkeypatch.setattr(_FakeRun, "outcome", outcome)
    monkeypatch.setattr(check, "Run", _FakeRun)
    out = io.StringIO()
    assert check.main(["--phase", "first"], environ=dict(URLS), out=out) == status
    assert out.getvalue().splitlines() == lines


def test_main_exits_2_on_a_setup_error():
    out = io.StringIO()
    assert check.main(["--phase", "first"], environ={}, out=out) == 2
    assert out.getvalue().splitlines() == [
        "FAIL setup: missing: GRAFEO_OLD_URL, GRAFEO_NEW_URL",
        "RESULT: FAIL",
    ]


def test_the_deadline_ends_the_run_with_exit_2(monkeypatch):
    exited = threading.Event()
    codes = []

    def fake_exit(code):
        codes.append(code)
        exited.set()

    monkeypatch.setattr(check.os, "_exit", fake_exit)
    out = io.StringIO()
    check._start_deadline(0.01, {"run": SimpleNamespace(step="benchmark aplogs")}, out)
    assert exited.wait(5)
    assert codes == [2]
    assert out.getvalue().splitlines() == [
        "FAIL deadline: no result after 0.01 s, during benchmark aplogs",
        "RESULT: FAIL",
    ]


def _snapshot(nodes):
    return {
        "nodes": nodes,
        "edges": [],
        "labels": [],
        "types": [],
        "two-hops": [],
        "ordered": [],
    }


@pytest.mark.parametrize(
    ("new_nodes", "written"),
    [
        (["a"], ["writes", "transactions", "concurrency"]),
        (["a", "b"], []),
    ],
)
def test_phase_first_writes_only_when_both_start_with_the_same_data(
    monkeypatch, new_nodes, written
):
    snapshots = {"old": _snapshot(["a"]), "new": _snapshot(new_nodes)}
    monkeypatch.setattr(check, "snapshot", lambda instance: snapshots[instance.name])
    calls = []
    run = object.__new__(check.Run)
    run.out, run.failed, run.step, run.same_data = io.StringIO(), False, "", False
    run.old, run.new = SimpleNamespace(name="old"), SimpleNamespace(name="new")
    run.binding = SimpleNamespace(check_no_stray=lambda: None)
    run.reachable = run.reads = lambda: iter([])
    for step in ("writes", "transactions", "concurrency"):
        setattr(run, step, lambda step=step: calls.append(step) or iter([]))

    run.phase_first()

    assert calls == written
    skipped = [
        line for line in run.out.getvalue().splitlines() if line.startswith("SKIP")
    ]
    if written:
        assert skipped == []
    else:
        assert skipped == [
            f"SKIP {step}: not run, since it writes: the instances do not start "
            "with the same data, so they may not be two copies of one backup"
            for step in ("writes", "transactions", "concurrency")
        ]
