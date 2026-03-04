import os
import time
from dataclasses import dataclass
import hashlib
from typing import Any, Dict, Optional

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

"""Authentication utilities for incoming token validation and downstream token exchange.

This module contains:
- Bearer token validation against OIDC issuer + JWKS
- Optional `azp` authorization guard
- Token-exchange helper to obtain downstream service tokens

Most values are configurable via environment variables. Defaults are provided for
the DataGEMS dev realm to keep local/dev usage simple.
"""

# JSON Web Signature algorithm expected for incoming JWT tokens.
# We keep this explicit to avoid algorithm confusion attacks.
JWT_SIGNING_ALGORITHM = "RS256"

# Grant and token-type constants used in OAuth2 token exchange.
TOKEN_EXCHANGE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange"
ACCESS_TOKEN_TYPE_URN = "urn:ietf:params:oauth:token-type:access_token"

# Shared HTTP timeout for OIDC/JWKS network calls.
# 10 seconds is long enough for normal network jitter but short enough to fail fast.
HTTP_TIMEOUT_SECONDS = 10.0

# Safety window for cached exchanged tokens.
# If token expires in less than this window, we fetch a fresh one.
TOKEN_CACHE_SAFETY_WINDOW_SECONDS = 30

# Fallback expiry for token-exchange responses that omit `expires_in`.
DEFAULT_EXCHANGE_EXPIRES_IN_SECONDS = 300

# OIDC issuer URL used to validate who minted the token (`iss` claim).
# If this does not match, the token is rejected even if the signature is valid.
OIDC_ISSUER = os.getenv(
    "OIDC_ISSUER", "https://datagems-dev.scayle.es/oauth/realms/dev"
)

# JWKS endpoint holding the public keys used to verify JWT signatures.
# Defaults to the standard Keycloak certs endpoint for the selected issuer.
OIDC_JWKS_URL = os.getenv(
    "OIDC_JWKS_URL",
    f"{OIDC_ISSUER}/protocol/openid-connect/certs",
)

# Optional authorized-party (`azp`) gate.
# When set, only tokens issued for this client/application are accepted.
OIDC_AUTHORIZED_AZP = os.getenv("OIDC_AUTHORIZED_AZP", "DMM")

# OIDC token endpoint used for service token exchange.
OIDC_TOKEN_URL = os.getenv(
    "OIDC_TOKEN_URL",
    f"{OIDC_ISSUER}/protocol/openid-connect/token",
)

# Service credentials used for token-exchange grant against the IdP.
OIDC_CLIENT_ID = os.getenv("OIDC_CLIENT_ID", "data-model-management-api")
OIDC_CLIENT_SECRET = os.getenv("OIDC_CLIENT_SECRET")

# JWKS cache TTL (seconds). Can be overridden per environment.
# `300` is only the default example value.
JWKS_CACHE_TTL_SECONDS = int(os.getenv("JWKS_CACHE_TTL_SECONDS", "300"))

# FastAPI HTTP bearer extractor.
# `auto_error=False` lets us return our own explicit 401 message when absent/invalid.
_bearer_scheme = HTTPBearer(auto_error=False)

# In-memory cache for JWKS document.
# Global because dependencies are called per-request and we want to reuse keys.
_jwks_cache: Optional[Dict[str, Any]] = None

# Epoch timestamp (seconds) when JWKS cache expires.
# `0.0` is a startup sentinel meaning "no valid cache yet" (forces first fetch).
# The actual value is assigned in `_get_jwks()` as:
# `time.time() + JWKS_CACHE_TTL_SECONDS`.
_jwks_cache_expires_at = 0.0


@dataclass
class _Token:
    """Internal cache entry for exchanged access tokens.

    - value: exchanged bearer token string
    - expires_at: absolute epoch timestamp when token should be considered expired
    """

    value: str
    expires_at: float


# In-memory cache for exchanged tokens.
# Key includes client + scope + hash of subject token (not raw token) to avoid
# storing caller credentials in cache keys while keeping caller isolation.
_exchange_token_cache: Dict[str, _Token] = {}


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
            issuer=OIDC_ISSUER,
            options={"verify_aud": False},
        )
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {exc}",
        ) from exc
    except httpx.HTTPError as exc:
        # Infrastructure/network issue while retrieving verification keys.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Could not fetch JWKS: {exc}",
        ) from exc

    # Optional client-binding check via `azp` claim.
    if OIDC_AUTHORIZED_AZP and payload.get("azp") != OIDC_AUTHORIZED_AZP:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token azp is not authorized",
        )

    # Dependency output: route handlers can use these claims directly.
    return payload


async def require_valid_credentials(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> HTTPAuthorizationCredentials:
    """Return validated bearer credentials for cases that need the raw token value."""
    await require_valid_token(credentials)
    return credentials


async def get_exchanged_access_token(subject_token: str, scope: str) -> str:
    """Exchange caller token for a downstream access token bound to `scope`.

    Args:
        subject_token: Caller bearer token to exchange.
        scope: Target downstream audience/scope (for example CDD API scope).

    Returns:
        A downstream bearer access token.

    Notes:
    - Uses OAuth2 token-exchange flow.
    - Caches exchanged tokens per caller+scope until close to expiry.
    """
    if not OIDC_CLIENT_SECRET:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OIDC_CLIENT_SECRET is not configured",
        )

    token_hash = hashlib.sha256(subject_token.encode("utf-8")).hexdigest()
    cache_key = f"{OIDC_CLIENT_ID}:{scope}:{token_hash}"
    cached = _exchange_token_cache.get(cache_key)
    if cached and cached.expires_at > time.time() + TOKEN_CACHE_SAFETY_WINDOW_SECONDS:
        return cached.value

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
            response = await client.post(
                OIDC_TOKEN_URL,
                auth=(OIDC_CLIENT_ID, OIDC_CLIENT_SECRET),
                data={
                    "grant_type": TOKEN_EXCHANGE_GRANT_TYPE,
                    "subject_token": subject_token,
                    "subject_token_type": ACCESS_TOKEN_TYPE_URN,
                    "requested_token_type": ACCESS_TOKEN_TYPE_URN,
                    "scope": scope,
                },
            )
            response.raise_for_status()
            data = response.json()
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Token exchange failed: {exc}",
        ) from exc

    access_token = data.get("access_token")
    expires_in = int(data.get("expires_in", DEFAULT_EXCHANGE_EXPIRES_IN_SECONDS))
    if not access_token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Token exchange did not return access_token",
        )

    _exchange_token_cache[cache_key] = _Token(
        value=access_token,
        expires_at=time.time() + expires_in,
    )
    return access_token
