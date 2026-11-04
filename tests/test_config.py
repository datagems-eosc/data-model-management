import asyncio
import os
import subprocess
import sys

import httpx
import pytest
from structlog.testing import capture_logs

from dmm_api import config
from dmm_api.resources import dataset


def test_missing_variables_are_listed_together(monkeypatch):
    monkeypatch.delenv("CDD_URL")
    monkeypatch.delenv("QD_URL")
    monkeypatch.setenv("IDD_URL", "")
    with capture_logs() as logs, pytest.raises(SystemExit) as exit_info:
        config.require_configuration()
    assert exit_info.value.code == 1
    assert logs == [
        {
            "event": "missing required configuration",
            "log_level": "critical",
            "missing": ["CDD_URL", "IDD_URL", "QD_URL"],
        }
    ]


def test_app_does_not_start_without_configuration():
    env = {k: v for k, v in os.environ.items() if k not in config.REQUIRED}
    result = subprocess.run(
        [sys.executable, "-c", "import dmm_api.main"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "missing required configuration" in result.stdout


@pytest.mark.parametrize(
    "value", ["https://example.org/service", "https://example.org/service/"]
)
def test_either_slash_form_builds_the_same_url(monkeypatch, value):
    monkeypatch.setenv("CDD_URL", value)
    assert f"{config.service_url('CDD_URL')}search-ap/" == (
        "https://example.org/service/search-ap/"
    )


def test_moma_api_url_is_ignored_with_a_warning(monkeypatch):
    monkeypatch.setenv("MOMA_API_URL", "https://example.org/other")
    with capture_logs() as logs:
        config.require_configuration()
    assert [entry["log_level"] for entry in logs] == ["warning"]


def test_node_properties_come_from_moma_url(monkeypatch):
    requested = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return httpx.Response(200, json={"properties": {"name": "zoo"}})

    monkeypatch.setattr(dataset, "MOMA_URL", "https://moma.example.org/api/")
    real_client = httpx.AsyncClient
    monkeypatch.setattr(
        dataset.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    properties = asyncio.run(dataset.get_node_properties("n1", "token"))
    assert properties == {"name": "zoo"}
    assert requested == ["https://moma.example.org/api/nodes/n1"]
