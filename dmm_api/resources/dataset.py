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


class EncodingFormat(str, Enum):
    csv = "text/csv"
    pdf = "application/pdf"


# Endpoints
@router.get("/dataset", response_model=DatasetsSuccessEnvelope)
async def get_all_datasets(
    encoding_format: Optional[List[EncodingFormat]] = Query(
        None, description="Filter by encoding format (e.g., text/csv, application/pdf)"
    ),
):
    """Return all datasets, with optional filtering"""
    query_params = {}
    if encoding_format:
        query_params["encodingFormat"] = [ef.value for ef in encoding_format]

    # Neo4j API endpoint to be created
    url = f"{MOMA_URL}/retrieveMoMaDatasets"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, params=query_params)
            response.raise_for_status()

            datasets_from_neo4j = response.json()

            return DatasetsSuccessEnvelope(
                code=status.HTTP_200_OK,
                message="Datasets retrieved successfully",
                datasets=datasets_from_neo4j,
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
    url = f"{MOMA_URL}/retrieveMoMaMetadata?id={dataset_id}"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            dataset = response.json()
            return DatasetSuccessEnvelope(
                code=status.HTTP_200_OK,
                message=f"Dataset with ID {dataset_id} retrieved successfully from Neo4j",
                dataset=dataset,
            )

        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=ErrorEnvelope(
                        code=status.HTTP_404_NOT_FOUND,
                        error=f"Dataset with ID {dataset_id} not found in Neo4j",
                    ).model_dump(),
                )
            else:
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
    url = f"{MOMA_URL}/ingestProfile2MoMa"
    dataset_id = dataset.get("@id")
    # check_url = f"{MOMA_URL}/retrieveMoMaMetadata?id={dataset_id}"

    # TODO: use Pydantic Model to validate the JSON
    if not dataset_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorEnvelope(
                code=status.HTTP_400_BAD_REQUEST,
                error="Dataset must contain an '@id' field",
            ).model_dump(),
        )

    async with httpx.AsyncClient() as client:
        try:
            # Check if a Dataset with such UUID is already stored in the Neo4j
            # check_response = await client.get(check_url)
            # if check_response.status_code != 404:
            #     raise HTTPException(
            #         status_code=status.HTTP_409_CONFLICT,
            #         detail=ErrorEnvelope(
            #             code=status.HTTP_409_CONFLICT,
            #             error=f"Dataset with ID {dataset_id} already exists in Neo4j",
            #         ).model_dump(),
            #     )

            # If not, register the new dataset
            response = await client.post(url, json=dataset)
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

    if dataset_id not in datasets:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorEnvelope(
                code=status.HTTP_404_NOT_FOUND,
                error=f"Dataset with UUID {dataset_id} does not exists.",
            ).model_dump(),
        )

    datasets[dataset_id] = dataset

    return DatasetSuccessEnvelope(
        code=status.status.HTTP_200_OK,
        message=f"Dataset with ID {dataset_id} updated successfully.",
        dataset=dataset,
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
