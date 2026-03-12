import json
import logging
import os
import tempfile

from fastapi import APIRouter, File, UploadFile
from fastapi.responses import Response
from dmm_api.tools.PG2Croissant.parser import (
    parse_lightProfile,
    parse_heavyProfile,
    parse_dataset,
)
from dmm_api.tools.PG2Croissant.mapper import (
    map_to_croissant_dataset,
    map_to_croissant_lightProfile,
    map_to_croissant_heavyProfile,
)

router = APIRouter()


def convertDataset(pgjson_path: str, output_path: str):
    with open(pgjson_path, "r", encoding="utf-8") as f:
        pgjson = json.load(f)

    datasets = parse_dataset(pgjson)
    croissant_dict = map_to_croissant_dataset(datasets)
    croissant_jsonld = to_jsonld(croissant_dict)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(croissant_jsonld)


def convertLightProfile(pgjson_path: str):
    with open(pgjson_path, "r", encoding="utf-8") as f:
        pgjson = json.load(f)

    datasets = parse_lightProfile(pgjson)
    croissant_dict = map_to_croissant_lightProfile(datasets)
    croissant_jsonld = to_jsonld(croissant_dict)
    return croissant_jsonld


def convertHeavyProfile(pgjson_path: str):
    with open(pgjson_path, "r", encoding="utf-8") as f:
        pgjson = json.load(f)

    datasets = parse_heavyProfile(pgjson)
    croissant_dict = map_to_croissant_heavyProfile(datasets)
    croissant_jsonld = to_jsonld(croissant_dict)

    return croissant_jsonld


# @router.post("/moma2croissant/light")
# async def moma2croissant_light(file: UploadFile = File(...)):
#     """Convert MoMa light profile to Croissant format"""
#     temp_dir = tempfile.gettempdir()
#     pg_json = os.path.join(temp_dir, file.filename)
#     logging.info(f"Saving uploaded file to {pg_json}")
    
#     try:
#         with open(pg_json, "wb") as f:
#             f.write(await file.read())
        
#         croissant_jsonld = convertLightProfile(pgjson_path=pg_json)
#         croissant_dict = json.loads(croissant_jsonld)

#         response_data = {
#             "message": "MoMa light profile converted to Croissant format successfully",
#             "croissant": croissant_dict
#         }
#         return Response(content=json.dumps(response_data), media_type="application/json")
#     except Exception as e:
#         logging.error(f"Error processing file: {str(e)}")
#         raise

def to_jsonld(croissant_dict: dict) -> str:
    return json.dumps(croissant_dict, indent=2)

@router.post("/moma2croissant")
async def moma2croissant(file: UploadFile = File(...)):
    """Convert MoMa profile to Croissant format"""
    temp_dir = tempfile.gettempdir()
    pg_json = os.path.join(temp_dir, file.filename)
    logging.info(f"Saving uploaded file to {pg_json}")

    try:
        with open(pg_json, "wb") as f:
            f.write(await file.read())

        croissant_jsonld = convertHeavyProfile(pgjson_path=pg_json)
        croissant_dict = json.loads(croissant_jsonld)
        response_data = {
            "message": "MoMa profile converted to Croissant format successfully",
            "croissant": croissant_dict
        }
        return Response(
            content=json.dumps(response_data), media_type="application/json"
        )
    except Exception as e:
        logging.error(f"Error processing file: {str(e)}")
        raise
