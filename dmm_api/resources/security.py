import time
from typing import Any, Optional

import httpx
import os
from fastapi import Depends, HTTPException, status, APIRouter
from fastapi.security import (
    OAuth2PasswordBearer,
    HTTPAuthorizationCredentials,
    HTTPBearer,
)
from jose import JWTError, jwt
from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr
import structlog

from dmm_api.config import OIDC_ISSUER_URL
from dmm_api.logging_config import set_caller


class FailedDependencyMessage(BaseModel):
    statusCode: int
    source: str
    correlationId: Optional[str] = None
    payload: Optional[Any] = None


class FailedDependencyResponse(BaseModel):
    code: int
    error: str
    message: FailedDependencyMessage


class FailedDependencyException(HTTPException):
    def __init__(
        self,
        source: str,
        status_code: int,
        detail: str,
        correlation_id: Optional[str] = None,
        payload: Optional[Any] = None,
    ):
        self.source = source
        self.downstream_status_code = status_code
        self.correlation_id = correlation_id
        self.downstream_payload = payload
        super().__init__(status_code=status.HTTP_424_FAILED_DEPENDENCY, detail=detail)


logger = structlog.get_logger(__name__)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
_jwks_cache = None
_jwks_cache_expires_at = 0

OIDC_CLIENT_ID: str = os.getenv("OIDC_CLIENT_ID", "data-model-management-api")

OIDC_JWKS_URL = os.getenv(
    "OIDC_JWKS_URL",
    f"{OIDC_ISSUER_URL}/protocol/openid-connect/certs",
)

HTTP_TIMEOUT_SECONDS = 10.0
JWKS_CACHE_TTL_SECONDS = int(os.getenv("JWKS_CACHE_TTL_SECONDS", "300"))
JWT_SIGNING_ALGORITHM = "RS256"

_bearer_scheme = HTTPBearer(auto_error=False)

router = APIRouter()


class AuthTestRequest(BaseModel):
    """Input payload for the basic auth test endpoint."""

    # Enforce exact payload shape: only `query` (string) and `k` (integer).
    model_config = ConfigDict(extra="forbid")

    query: StrictStr
    k: StrictInt


async def require_valid_token(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> dict[str, Any]:
    """FastAPI dependency that enforces bearer-token authentication.

    Returns decoded JWT claims on success, otherwise raises HTTP errors:
    - 401 for missing/invalid token
    - 503 if JWKS cannot be retrieved

    It checks the signature, the issuer and the expiry. The audience is checked
    by `require_app_scope`, and the authorized party (`azp`) is not checked.
    """
    # Require `Authorization: Bearer <token>`.
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
        )

    try:
        # Verify token signature and issuer using the provider public keys.
        # Audience verification is disabled here (`verify_aud=False`), so any
        # token from the same issuer passes; `require_app_scope` checks `aud`.
        payload = jwt.decode(
            credentials.credentials,
            await _get_jwks(),
            algorithms=[JWT_SIGNING_ALGORITHM],
            audience=OIDC_CLIENT_ID,
            issuer=OIDC_ISSUER_URL,
            options={"verify_aud": False},
        )
    except JWTError as exc:
        logger.warning("JWT validation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        ) from exc
    except httpx.HTTPError as exc:
        # Infrastructure/network issue while retrieving verification keys. The
        # exception handler logs `exc`, which can name the login server's host.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not fetch JWKS",
        ) from exc

    set_caller(payload.get("sub"), payload.get("azp"))
    # Dependency output: route handlers can use these claims directly.
    return payload


async def _get_jwks() -> dict[str, Any]:
    """Return JWKS, using a short-lived in-memory cache.

    Implication: signature verification does not perform an external HTTP call for
    every request; however, key rotations can take up to cache TTL to be picked up.
    """
    global _jwks_cache, _jwks_cache_expires_at

    # Fast path: return cached keys if TTL is still valid.
    if _jwks_cache and time.time() < _jwks_cache_expires_at:
        return _jwks_cache

    # Slow path: fetch current keys from IdP.
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
        response = await client.get(OIDC_JWKS_URL)
        response.raise_for_status()
        _jwks_cache = response.json()

    # Cache TTL is controlled by `JWKS_CACHE_TTL_SECONDS`.
    _jwks_cache_expires_at = time.time() + JWKS_CACHE_TTL_SECONDS
    return _jwks_cache


async def require_app_scope(
    token_payload: dict[str, Any] = Depends(require_valid_token),
) -> dict[str, Any]:
    """Validate that token has access to this API via audience claim.

    Checks if OIDC_CLIENT_ID is present in the token's audience (aud) claim.
    The aud claim can be a string or a list of strings.
    Returns decoded JWT claims on success, otherwise raises 403 Forbidden.
    """
    # Extract audience from token - can be a string or list
    aud = token_payload.get("aud", [])

    # Normalize to list for consistent handling
    audiences = aud if isinstance(aud, list) else [aud] if aud else []

    if OIDC_CLIENT_ID not in audiences:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Token missing required audience: {OIDC_CLIENT_ID}",
        )

    return token_payload


@router.post("/authtest")
async def authtest(
    payload: AuthTestRequest,
    token_payload: dict[str, Any] = Depends(require_valid_token),
):
    """Validate caller token and echo minimal identity + request payload."""
    return {
        "authorized": True,
        "message": "Token is valid",
        "request": payload.model_dump(),
        "subject": token_payload.get("sub"),
        "username": token_payload.get("preferred_username"),
    }
