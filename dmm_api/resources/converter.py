"""Property-graph JSON to Croissant JSON-LD.

`GET /dataset/get/{dataset_id}?format=croissant` converts with
`convertProfile`. The `POST /convert` endpoint is deprecated since v0.3.1;
its route stays so that an old client learns why it no longer converts.
"""

import json

from fastapi import APIRouter, HTTPException, status

from dmm_api.tools.PG2Croissant.parser import parse_profile
from dmm_api.tools.PG2Croissant.mapper import map_to_croissant

router = APIRouter()


def convertProfile(pgjson):
    datasets = parse_profile(pgjson)
    croissant_dict = map_to_croissant(datasets)
    croissant_jsonld = to_jsonld(croissant_dict)

    return croissant_jsonld


def to_jsonld(croissant_dict: dict) -> str:
    return json.dumps(croissant_dict, indent=2)


@router.post(
    "/convert",
    deprecated=True,
    status_code=status.HTTP_410_GONE,
    responses={status.HTTP_410_GONE: {"description": "Deprecated since v0.3.1"}},
)
async def convert():
    """Deprecated since v0.3.1. Answers 410 to any request, with or without a token."""
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail=(
            "POST /convert is deprecated since v0.3.1 and no longer converts. "
            "Use GET /api/v1/dataset/get/{dataset_id}?format=croissant."
        ),
    )
