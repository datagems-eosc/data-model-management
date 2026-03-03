import os
import time
from typing import Any, Dict, Optional

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

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
    async with httpx.AsyncClient(timeout=10.0) as client:
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
            algorithms=["RS256"],
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
