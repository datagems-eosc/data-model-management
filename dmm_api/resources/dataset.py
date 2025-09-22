from enum import Enum
import os
from fastapi import APIRouter, HTTPException, Query, status
import httpx
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from .query_executor import execute_query_csv
from .data_resolver import resolve_dataset
from .json_format import create_json
from .AP_parser import extract_from_AP, QueryRequest

datasets = {}
query_results = {}


class DatasetSuccessEnvelope(BaseModel):
    code: int
    message: str
    dataset: Dict[str, Any]


class DatasetsSuccessEnvelope(BaseModel):
    code: int
    message: str
    datasets: List[Dict[str, Any]]


class ErrorEnvelope(BaseModel):
    code: int
    error: str


router = APIRouter()


MOMA_URL = os.getenv("MOMA_URL", "http://localhost:8000")


class DatasetType(str, Enum):
    PDF = "PDF"
    RelationalDatabase = "RelationalDatabase"
    CSV = "CSV"
    ImageSet = "ImageSet"
    TextSet = "TextSet"
    Table = "Table"


# Endpoints
@router.get("/dataset", response_model=DatasetsSuccessEnvelope)
async def get_datasets(
    type: Optional[DatasetType] = Query(
        None,
        description="Optional dataset type to filter on. If omitted, all collections are returned.",
    ),
):
    """Return all datasets, with optional filtering"""

    if type:
        url = f"{MOMA_URL}/listCollectionsByType"
        params = {"type": type.value}
        success_msg = f"All datasets of type {type} retrieved successfully"
    else:
        url = f"{MOMA_URL}/listCollections"
        params = {}
        success_msg = "All datasets retrieved successfully"

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return DatasetsSuccessEnvelope(
                code=status.HTTP_200_OK,
                message=success_msg,
                datasets=resp.json(),
            )

        except httpx.HTTPStatusError:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error="Error from MoMa API",
                ).model_dump(),
            )

        except httpx.RequestError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error="Failed to connect to MoMa API",
                ).model_dump(),
            )


@router.get("/dataset/{dataset_id}", response_model=DatasetSuccessEnvelope)
async def get_dataset(dataset_id: str):
    """Return dataset with a specific ID from Neo4j via MoMa API"""
    url = f"{MOMA_URL}/getCollection?id={dataset_id}"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            dataset = response.json()
            if not dataset:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=ErrorEnvelope(
                        code=status.HTTP_404_NOT_FOUND,
                        error=f"Dataset with ID {dataset_id} not found in Neo4j",
                    ).model_dump(),
                )

            return DatasetSuccessEnvelope(
                code=status.HTTP_200_OK,
                message=f"Dataset with ID {dataset_id} retrieved successfully from Neo4j",
                dataset=dataset,
            )

        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Error from MoMa API: {exc.response.status_code}",
                ).model_dump(),
            )

        except httpx.RequestError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error="Failed to connect to MoMa API",
                ).model_dump(),
            )

        except Exception:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error="Unexpected Internal Server error",
                ).model_dump(),
            )


@router.post("/dataset/register", response_model=DatasetSuccessEnvelope)
async def register_dataset(dataset: Dict[str, Any]):
    """Register dataset through MoMa API which stores it in Neo4j"""
    dataset_id = dataset.get("@id")
    ingest_url = f"{MOMA_URL}/ingestProfile2MoMa"
    check_url = f"{MOMA_URL}/getCollection?id={dataset_id}"

    # TODO: use Pydantic Model to validate the JSON
    # if not dataset_id:
    #     raise HTTPException(
    #         status_code=status.HTTP_400_BAD_REQUEST,
    #         detail=ErrorEnvelope(
    #             code=status.HTTP_400_BAD_REQUEST,
    #             error="Dataset must contain an '@id' field",
    #         ).model_dump(),
    #     )

    async with httpx.AsyncClient() as client:
        try:
            # Check if a Dataset with such UUID is already stored in the Neo4j
            check_response = await client.get(check_url)
            check_response.raise_for_status()
            existing_dataset = check_response.json()
            if existing_dataset:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=ErrorEnvelope(
                        code=status.HTTP_409_CONFLICT,
                        error=f"Dataset with ID {dataset_id} already exists in Neo4j",
                    ).model_dump(),
                )

            # If not, register the new dataset
            response = await client.post(ingest_url, json=dataset)
            response.raise_for_status()

            return DatasetSuccessEnvelope(
                code=status.HTTP_201_CREATED,
                message=f"Dataset with ID {dataset_id} registered successfully in Neo4j",
                dataset=dataset,
            )

        except httpx.HTTPStatusError:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error="Error from MoMa API",
                ).model_dump(),
            )

        except httpx.RequestError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error="Failed to connect to MoMa API",
                ).model_dump(),
            )

        except Exception:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error="Unexpected Internal Server error",
                ).model_dump(),
            )


@router.put("/dataset/update")
async def update_dataset(dataset: Dict[str, Any]):
    """Update an existing dataset with new data after profiling"""
    dataset_id = dataset.get("@id")
    ingest_url = f"{MOMA_URL}/ingestProfile2MoMa"
    check_url = f"{MOMA_URL}/getCollection?id={dataset_id}"

    async with httpx.AsyncClient() as client:
        try:
            check_response = await client.get(check_url)
            check_response.raise_for_status()
            existing_dataset = check_response.json()

            if not existing_dataset:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=ErrorEnvelope(
                        code=status.HTTP_404_NOT_FOUND,
                        error=f"Dataset with ID {dataset_id} does not exist in Neo4j",
                    ).model_dump(),
                )

            response = await client.post(ingest_url, json=dataset)
            response.raise_for_status()

            return DatasetSuccessEnvelope(
                code=status.HTTP_200_OK,
                message=f"Dataset with ID {dataset_id} updated successfully in Neo4j",
                dataset=dataset,
            )
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Error from MoMa API: {exc.response.status_code}",
                ).model_dump(),
            )

        except httpx.RequestError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error="Failed to connect to MoMa API",
                ).model_dump(),
            )

        except Exception:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error="Unexpected internal server error",
                ).model_dump(),
            )


@router.post("/dataset/query")
async def execute_query(query_data: QueryRequest):
    """Execute a SQL query on a dataset based on an Analytical Pattern"""
    try:
        extracted_info = extract_from_AP(query_data)
        # dataset_id = extracted_info.get("dataset_id")
        dataset_name = extracted_info.get("dataset_name")
        csv_name = extracted_info.get("csv_name")
        query = extracted_info.get("query")
        software = extracted_info.get("software")
        user_id = extracted_info.get("user_id")

        data_path = resolve_dataset(dataset_name, csv_name)

        # execute_query gets called based on the dataset type
        csv_path, query = execute_query_csv(
            csv_name, query, software, data_path, user_id
        )
        dataset_json = create_json(csv_path, query)

        return dataset_json

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute query: {str(e)}",
        )
