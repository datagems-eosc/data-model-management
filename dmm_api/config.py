"""Settings the service cannot run without, read once at import.

The service URLs used to default to the dev deployment, so a deployment that
missed one silently talked to the dev services. They are now required, and
`require_configuration()` stops the app at startup when any is missing.
"""

import os

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
