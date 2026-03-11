"""Auth test endpoints.

This module provides:
- `POST /authtest`: simple token validation smoke test
- `POST /authtest/cdd-search`: authenticated forwarding to CDD search endpoint

Environment-based defaults are intentionally explicit for easier operations/debugging.
"""

import os
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials
import httpx
from pydantic import BaseModel

from dmm_api.security import (
    get_exchanged_access_token,
    require_valid_credentials,
    require_valid_token,
)

router = APIRouter()

# Downstream Cross-Dataset Discovery search endpoint.
# Default points to DataGEMS dev gateway path.
CDD_SEARCH_URL = os.getenv(
    "CDD_SEARCH_URL",
    "https://datagems-dev.scayle.es/cross-dataset-discovery/search/",
)

# Scope requested during token exchange to obtain a token accepted by CDD.
CDD_EXCHANGE_SCOPE = os.getenv("CDD_EXCHANGE_SCOPE", "cross-dataset-discovery-api")

# Timeout for outbound CDD call. Kept higher than OIDC calls because search may
# involve heavier processing than token/JWKS requests.
CDD_REQUEST_TIMEOUT_SECONDS = 30.0


class AuthTestRequest(BaseModel):
    """Input payload for the basic auth test endpoint."""

    query: str
    k: int


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


@router.post("/authtest/cdd-search")
async def authtest_cdd_search(
    payload: AuthTestRequest,
    credentials: HTTPAuthorizationCredentials = Depends(require_valid_credentials),
):
    """Forward exact input payload to CDD search using exchanged credentials.

    Flow:
    1) Validate caller bearer token
    2) Exchange caller token for CDD scope
    3) POST incoming JSON payload unchanged to CDD endpoint
    4) Return downstream status and body as-is (JSON when possible)
    """
    exchanged_token = await get_exchanged_access_token(
        subject_token=credentials.credentials,
        scope=CDD_EXCHANGE_SCOPE,
    )

    async with httpx.AsyncClient(timeout=CDD_REQUEST_TIMEOUT_SECONDS) as client:
        response = await client.post(
            CDD_SEARCH_URL,
            headers={"Authorization": f"Bearer {exchanged_token}"},
            json=payload.model_dump(),
        )

    try:
        response_payload = response.json()
    except ValueError:
        response_payload = {
            "status_code": response.status_code,
            "content": response.text,
        }

    return JSONResponse(status_code=response.status_code, content=response_payload)
