"""The service's settings, read once at import.

The service URLs used to default to the dev deployment, so a deployment that
missed one silently talked to the dev services. They are now required, and
`require_configuration()` stops the app at startup when any is missing.
"""

import os
import re
import socket
from importlib.metadata import PackageNotFoundError, version

import structlog

logger = structlog.get_logger(__name__)

REQUIRED = (
    "OIDC_ISSUER_URL",
    "MOMA_URL",
    "CDD_URL",
    "IDD_URL",
    "QD_URL",
    "REC_SYS_URL",
)


def _read(name: str) -> str:
    return os.environ.get(name, "").strip()


def service_url(name: str) -> str:
    """Return the URL in `name` with exactly one trailing `/`.

    The callers append paths without a separator, so either form of the value
    has to end up the same.
    """
    value = _read(name)
    return value.rstrip("/") + "/" if value else ""


OIDC_ISSUER_URL = _read("OIDC_ISSUER_URL").rstrip("/")
MOMA_URL = service_url("MOMA_URL")
CDD_URL = service_url("CDD_URL")
IDD_URL = service_url("IDD_URL")
QD_URL = service_url("QD_URL")
REC_SYS_URL = service_url("REC_SYS_URL")

# The release version, from pyproject.toml via the installed package metadata
try:
    APP_VERSION = version("dmm-api")
except PackageNotFoundError:
    APP_VERSION = "dev"

LOGS_DIR = os.environ.get("LOGS_DIR", "/logs")
LOG_ARCHIVE_DIR = os.environ.get("LOG_ARCHIVE_DIR", "/s3/data-model-management/logs")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
# Kubernetes sets HOSTNAME to the pod name. It goes into log file names, which
# use `_` as a separator, so only these characters are kept.
POD_NAME = re.sub(
    r"[^A-Za-z0-9.-]", "-", os.environ.get("HOSTNAME") or socket.gethostname()
)


def require_configuration() -> None:
    """Exit with status 1 if a required variable is missing or empty."""
    missing = [name for name in REQUIRED if not _read(name)]
    if missing:
        logger.critical("missing required configuration", missing=missing)
        raise SystemExit(1)
    if _read("MOMA_API_URL"):
        logger.warning(
            "MOMA_API_URL is no longer read; MOMA_URL is used", moma_url=MOMA_URL
        )
