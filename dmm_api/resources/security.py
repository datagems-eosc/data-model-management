from typing import Any, List, Optional

import httpx
import os
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel
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

correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default=None)
OIDC_CLIENT_ID: str = os.getenv("OIDC_CLIENT_ID", "data-model-management-api")
OIDC_CLIENT_SECRET = os.getenv("OIDC_CLIENT_SECRET")
OIDC_ISSUER_URL: str = os.getenv(
    "OIDC_ISSUER_URL", "https://datagems-dev.scayle.es/oauth/realms/dev"
)
OIDC_CONFIG_URL: str = f"{OIDC_ISSUER_URL}/.well-known/openid-configuration"
CDD_EXCHANGE_SCOPE: str = os.getenv("CDD_EXCHANGE_SCOPE", "cross-dataset-discovery-api")


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
