from enum import Enum
import os
from fastapi import APIRouter, HTTPException, Query, status
import httpx
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from .query_executor import execute_query_csv
from .data_resolver import resolve_dataset
from .json_format import create_json
from ..tools.parse_AP import extract_from_AP, extract_dataset_from_AP, APRequest
from ..tools.update_AP import update_dataset_field

datasets = {}
query_results = {}


class APSuccessEnvelope(BaseModel):
    code: int
    message: str
    ap: Dict[str, Any]


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
        description="Optional dataset type to filter on. If omitted, all datasets are returned.",
    ),
):
    """Return all datasets, with optional filtering"""

    if type:
        url = f"{MOMA_URL}/listDatasetsByType"
        params = {"type": type.value}
        success_msg = f"All datasets of type {type} retrieved successfully"
    else:
        url = f"{MOMA_URL}/listDatasets"
        params = {}
        success_msg = "All datasets retrieved successfully"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            datasets = data.get("metadata", {}).get("nodes", [])
            return DatasetsSuccessEnvelope(
                code=status.HTTP_200_OK,
                message=success_msg,
                datasets=datasets,
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
    url = f"{MOMA_URL}/getDataset?id={dataset_id}"

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


# TODO: check if dataset with such ID already exists in Neo4j (or using RDF)
@router.post("/dataset/register", response_model=APSuccessEnvelope)
async def register_dataset(ap_payload: APRequest):
    ingest_url = f"{MOMA_URL}/ingestProfile2MoMa"
    # check_url = f"{MOMA_URL}/getDataset?id={dataset_id}"

    try:
        dataset, old_dataset_id = extract_dataset_from_AP(
            ap_payload,
            expected_ap_process="register",
            expected_operator_command="create",
        )
        dataset_id = dataset.get("@id")

        # This check will be removed after we define JSON validation rules
        if not dataset_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="Dataset ID is missing",
                ).model_dump(),
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Unexpected error during the dataset extraction: {type(e).__name__}: {str(e)}",
            ).model_dump(),
        )

    if dataset_id != old_dataset_id:
        try:
            update_dataset_field(ap_payload, old_dataset_id, dataset_id)
        except Exception as e:
            print(f"Warning: Failed to update the Dataset ID: {e}")

    async with httpx.AsyncClient() as client:
        try:
            # Check if a Dataset with such UUID is already stored in the Neo4j
            # check_response = await client.get(check_url)
            # check_response.raise_for_status()
            # existing_dataset = check_response.json()
            # if existing_dataset:
            #     raise HTTPException(
            #         status_code=status.HTTP_409_CONFLICT,
            #         detail=ErrorEnvelope(
            #             code=status.HTTP_409_CONFLICT,
            #             error=f"Dataset with ID {dataset_id} already exists in Neo4j",
            #         ).model_dump(),
            #     )

            # If not, register the new dataset
            response = await client.post(ingest_url, json=dataset)
            response.raise_for_status()

            # Fake forward to AP Storage API
            try:
                print(f"Dataset {dataset_id} sent to the AP Storage API.")
            except Exception as e:
                print(f"AP Storage API not working: {e}")

            return APSuccessEnvelope(
                code=status.HTTP_201_CREATED,
                message=f"Dataset with ID {dataset_id} registered successfully in Neo4j",
                ap=ap_payload,
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


@router.put("/dataset/load")
async def load_dataset(dataset: Dict[str, Any]): ...


@router.put("/dataset/update")
async def update_dataset(dataset: Dict[str, Any]):
    """Update an existing dataset with new data after profiling"""
    dataset_id = dataset.get("@id")
    ingest_url = f"{MOMA_URL}/ingestProfile2MoMa"
    # check_url = f"{MOMA_URL}/getDataset?id={dataset_id}"

    async with httpx.AsyncClient() as client:
        try:
            # check_response = await client.get(check_url)
            # check_response.raise_for_status()
            # existing_dataset = check_response.json()

            # if not existing_dataset:
            #     raise HTTPException(
            #         status_code=status.HTTP_404_NOT_FOUND,
            #         detail=ErrorEnvelope(
            #             code=status.HTTP_404_NOT_FOUND,
            #             error=f"Dataset with ID {dataset_id} does not exist in Neo4j",
            #         ).model_dump(),
            #     )

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
async def execute_query(query_data: APRequest):
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
