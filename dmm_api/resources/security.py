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
from contextvars import ContextVar
import structlog


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
_oidc_config = None
_jwks_keys = None
_jwks_cache = None
_jwks_cache_expires_at = 0

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default=None)
OIDC_CLIENT_ID: str = os.getenv("OIDC_CLIENT_ID", "data-model-management-api")
OIDC_CLIENT_SECRET = os.getenv("OIDC_CLIENT_SECRET")
OIDC_ISSUER_URL: str = os.getenv(
    "OIDC_ISSUER_URL", "https://datagems-dev.scayle.es/oauth/realms/dev"
)
OIDC_CONFIG_URL: str = f"{OIDC_ISSUER_URL}/.well-known/openid-configuration"

OIDC_JWKS_URL = os.getenv(
    "OIDC_JWKS_URL",
    f"{OIDC_ISSUER_URL}/protocol/openid-connect/certs",
)

CDD_EXCHANGE_SCOPE: str = os.getenv("CDD_EXCHANGE_SCOPE", "cross-dataset-discovery-api")

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


async def get_oidc_config():
    global _oidc_config
    if _oidc_config is None:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(OIDC_CONFIG_URL)
                response.raise_for_status()
                _oidc_config = response.json()
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to fetch OIDC configuration due to HTTP error",
                url=str(e.request.url),
                status_code=e.response.status_code,
                response=e.response.text,
            )
            try:
                payload = e.response.json()
            except Exception:
                payload = e.response.text
            raise FailedDependencyException(
                source="OIDCProvider",
                status_code=e.response.status_code,
                correlation_id=get_correlation_id(),
                payload=payload,
                detail="Authentication service returned an error.",
            )
        except httpx.RequestError as e:
            logger.error(
                "Failed to fetch OIDC configuration due to network error",
                url=OIDC_CONFIG_URL,
                error=str(e),
            )
            raise FailedDependencyException(
                source="OIDCProvider",
                status_code=503,
                correlation_id=get_correlation_id(),
                payload={"error": f"Network error: {type(e).__name__}"},
                detail="Authentication service is unavailable.",
            )
    return _oidc_config


async def get_jwks_keys():
    """Fetches and caches the JSON Web Key Set (JWKS) containing public keys."""
    global _jwks_keys
    if _jwks_keys is None:
        oidc_config = await get_oidc_config()
        jwks_uri = oidc_config.get("jwks_uri")
        if not jwks_uri:
            raise FailedDependencyException(
                source="OIDCProvider",
                status_code=500,
                detail="jwks_uri not found in OIDC config.",
                correlation_id=get_correlation_id(),
            )

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(jwks_uri)
                response.raise_for_status()
                _jwks_keys = response.json()
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to fetch JWKS keys due to HTTP error",
                url=str(e.request.url),
                status_code=e.response.status_code,
                response=e.response.text,
            )
            try:
                payload = e.response.json()
            except Exception:
                payload = e.response.text
            raise FailedDependencyException(
                source="OIDCProvider",
                status_code=e.response.status_code,
                correlation_id=get_correlation_id(),
                payload=payload,
                detail="Could not fetch public keys for token validation.",
            )
        except httpx.RequestError as e:
            logger.error(
                "Failed to fetch JWKS keys due to network error",
                url=jwks_uri,
                error=str(e),
            )
            raise FailedDependencyException(
                source="OIDCProvider",
                status_code=503,
                correlation_id=get_correlation_id(),
                payload={"error": f"Network error: {type(e).__name__}"},
                detail="Could not fetch public keys for token validation.",
            )
    return _jwks_keys


async def _exchange_token_for_cdd(user_token: str) -> str | None:
    """
    Performs the On-Behalf-Of token exchange to get a token for the Gateway.
    """
    log = logger.bind()
    try:
        oidc_config = await get_oidc_config()
        token_endpoint = oidc_config.get("token_endpoint")
        if not token_endpoint:
            log.error(
                "token_endpoint not found in OIDC config. Cannot perform OBO flow."
            )
            return None

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "client_id": OIDC_CLIENT_ID,
            "client_secret": OIDC_CLIENT_SECRET,
            "subject_token": user_token,
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "scope": CDD_EXCHANGE_SCOPE,
            "audience": "cross-dataset-discovery-api",
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(token_endpoint, data=data)
            response.raise_for_status()
            new_token_data = response.json()
            exchanged_token = new_token_data.get("access_token")
            if exchanged_token:
                log.info("Successfully exchanged token for Gateway via OBO flow.")
                return exchanged_token
            else:
                log.error("OBO flow response did not contain an access_token.")
                return None
    except httpx.HTTPStatusError as e:
        log.error(
            "Failed to exchange token via OBO flow due to HTTP error.",
            status_code=e.response.status_code,
            response=e.response.text,
        )
        return None
    except Exception as e:
        log.error(
            "Unexpected error during OBO token exchange.", error=str(e), exc_info=True
        )
        return None


def get_correlation_id() -> str | None:
    """Returns the current correlation ID."""
    return correlation_id_var.get()


async def require_valid_token(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> dict[str, Any]:
    """FastAPI dependency that enforces bearer-token authentication.

    Returns decoded JWT claims on success, otherwise raises HTTP errors:
    - 401 for missing/invalid token
    - 403 for unauthorized `azp`
    - 503 if JWKS cannot be retrieved
    """
    # Require `Authorization: Bearer <token>`.
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing bearer token",
        )

    try:
        # Verify token signature and issuer using the provider public keys.
        # Audience verification is intentionally disabled for now (`verify_aud=False`).
        # This means any token from the same issuer can pass unless constrained by `azp`.
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
        # Infrastructure/network issue while retrieving verification keys.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not fetch JWKS: {exc}",
        ) from exc

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
