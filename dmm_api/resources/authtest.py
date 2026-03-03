from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from dmm_api.security import require_valid_token

router = APIRouter()


class AuthTestRequest(BaseModel):
    query: str
    k: int


@router.post("/authtest")
async def authtest(
    payload: AuthTestRequest,
    token_payload: dict[str, Any] = Depends(require_valid_token),
):
    return {
        "authorized": True,
        "message": "Token is valid",
        "request": payload.model_dump(),
        "subject": token_payload.get("sub"),
        "username": token_payload.get("preferred_username"),
    }
