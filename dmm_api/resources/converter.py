import json
from typing import Any

import structlog

from fastapi import APIRouter, Depends, File, UploadFile, Query, HTTPException
from fastapi.responses import Response
import dmm_api.resources.security as security
from dmm_api.tools.PG2Croissant.parser import parse_profile
from dmm_api.tools.PG2Croissant.mapper import map_to_croissant
from dmm_api.uploads import read_upload

router = APIRouter()
logger = structlog.get_logger(__name__)


def convertProfile(pgjson):
    datasets = parse_profile(pgjson)
    croissant_dict = map_to_croissant(datasets)
    croissant_jsonld = to_jsonld(croissant_dict)

    return croissant_jsonld


def to_jsonld(croissant_dict: dict) -> str:
    return json.dumps(croissant_dict, indent=2)


@router.post("/convert")
async def convert(
    file: UploadFile = File(...),
    from_format: str = Query(..., alias="from"),
    to_format: str = Query(..., alias="to"),
    token_payload: dict[str, Any] = Depends(security.require_app_scope),
):
    # Validate formats
    if from_format != "moma":
        raise HTTPException(
            status_code=400, detail=f"Unsupported from format: {from_format}"
        )
    if to_format != "croissant":
        raise HTTPException(
            status_code=400, detail=f"Unsupported to format: {to_format}"
        )

    try:
        content = await read_upload(file)
        croissant_jsonld = convertProfile(json.loads(content))
        croissant_dict = json.loads(croissant_jsonld)
        response_data = {
            "message": f"Converted from {from_format} to {to_format} successfully",
            "output": croissant_dict,
        }
        return Response(
            content=json.dumps(response_data), media_type="application/json"
        )
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        raise
