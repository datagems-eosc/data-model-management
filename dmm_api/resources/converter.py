import json
import os
import tempfile
import structlog

from fastapi import APIRouter, File, UploadFile, Query, HTTPException
from fastapi.responses import Response
from dmm_api.tools.PG2Croissant.parser import parse_heavyProfile
from dmm_api.tools.PG2Croissant.mapper import map_to_croissant_heavyProfile

router = APIRouter()
logger = structlog.get_logger(__name__)


def convertHeavyProfile(pgjson_path: str):
    with open(pgjson_path, "r", encoding="utf-8") as f:
        pgjson = json.load(f)

    return convertProfile(pgjson)


def convertProfile(pgjson):
    datasets = parse_heavyProfile(pgjson)
    croissant_dict = map_to_croissant_heavyProfile(datasets)
    croissant_jsonld = to_jsonld(croissant_dict)

    return croissant_jsonld


def to_jsonld(croissant_dict: dict) -> str:
    return json.dumps(croissant_dict, indent=2)


@router.post("/convert")
async def convert(
    file: UploadFile = File(...),
    from_format: str = Query(..., alias="from"),
    to_format: str = Query(..., alias="to"),
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

    temp_dir = tempfile.gettempdir()
    pg_json = os.path.join(temp_dir, file.filename)
    logger.info(f"Saving uploaded file to {pg_json}")

    try:
        with open(pg_json, "wb") as f:
            f.write(await file.read())

        croissant_jsonld = convertHeavyProfile(pgjson_path=pg_json)
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
