from enum import Enum
import os
from pathlib import Path
import shutil
from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile, status
import httpx
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from .query_executor import execute_query_csv

# from .data_resolver import resolve_dataset
# from .json_format import create_json
from ..tools.AP.parse_AP import (
    extract_query_from_AP,
    extract_dataset_from_AP,
    extract_dataset_id_from_AP,
    extract_dataset_path_from_AP,
    APRequest,
)
from ..tools.AP.update_AP import update_dataset_id, update_dataset_archivedAt
from ..tools.AP.generate_AP import generate_update_AP
from ..tools.S3.scratchpad import upload_dataset_to_scratchpad

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


class DatasetState(str, Enum):
    Ready = "Ready"
    Loaded = "Loaded"
    Staged = "Staged"
    Deleted = "Deleted"


class DatasetType(str, Enum):
    PDF = "PDF"
    RelationalDatabase = "RelationalDatabase"
    CSV = "CSV"
    ImageSet = "ImageSet"
    TextSet = "TextSet"
    Table = "Table"


# Endpoints


# Temporary router to upload the dataset to S3/scratchpad
@router.post("/data-workflow", response_model=DatasetSuccessEnvelope)
async def data_workflow(
    file: UploadFile = File(...),
    file_name: str = Form(...),
    dataset_id: str = Form(...),
):
    """Handle data workflow by uploading files and assigning metadata."""
    # Call the upload function to upload the dataset to the scratchpad
    try:
        file_bytes = await file.read()
        s3path = upload_dataset_to_scratchpad(file_bytes, file_name, dataset_id)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Failed to upload dataset to scratchpad: {str(e)}",
            ).model_dump(),
        )

    return DatasetSuccessEnvelope(
        code=status.HTTP_201_CREATED,
        message=f"Dataset {file_name} uploaded successfully with ID {dataset_id} at {s3path}",
        dataset={"id": dataset_id, "name": file_name, "archivedAt": s3path},
    )


# TODO: implement filtering by dataset state "Ready"
@router.get("/dataset", response_model=DatasetsSuccessEnvelope)
async def get_datasets(
    type: Optional[DatasetType] = Query(
        None,
        description="Optional dataset type to filter on. If omitted, only datasets in Ready state are returned.",
    ),
    state: Optional[DatasetState] = Query(
        DatasetState.Ready,
        description="Optional dataset state to filter on.",
    ),
):
    """Return all datasets, with optional filtering"""

    # TODO: implement filtering by dataset state "Ready"
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
    """Register a new dataset in Neo4j"""
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
            update_dataset_id(ap_payload, old_dataset_id, dataset_id)
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
                ap=ap_payload.model_dump(by_alias=True, exclude_defaults=True),
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


# TODO: check if dataset with such ID is already registered and is in "loaded" state
@router.put("/dataset/load")
async def load_dataset(ap_payload: APRequest):
    """Move dataset files from scratchpad to permanent storage"""
    DATASET_DIR = os.getenv("DATASET_DIR")
    try:
        dataset_path = extract_dataset_path_from_AP(
            ap_payload,
            expected_ap_process="load",
            expected_operator_command="update",
        )

        dataset_id = extract_dataset_id_from_AP(ap_payload)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Unexpected error during the dataset path extraction: {type(e).__name__}: {str(e)}",
            ).model_dump(),
        )

    try:
        if not dataset_path.startswith("s3://"):
            raise ValueError("Invalid S3 URI. Must start with s3://")

        path_without_s3_prefix = dataset_path.split("s3://", 1)[1]
        dataset_id = extract_dataset_id_from_AP(ap_payload)

        source_path = Path("/s3") / path_without_s3_prefix
        target_path = Path(DATASET_DIR) / dataset_id
        if not source_path.exists():
            raise FileNotFoundError(
                f"Source dataset not found at expected location: {source_path}"
            )

        if target_path.exists():
            raise FileExistsError(
                f"Target dataset with id {dataset_id} has already been moved to: {target_path}"
            )

        shutil.move(str(source_path), str(target_path))
        new_path = f"s3://dataset/{dataset_id}"

        update_ap = generate_update_AP(ap_payload, new_path)
        await update_dataset(update_ap)

        ap_payload = update_dataset_archivedAt(ap_payload, dataset_id, new_path)

        return APSuccessEnvelope(
            code=status.HTTP_200_OK,
            message=f"Dataset moved from {dataset_path} to {new_path}",
            ap=ap_payload.model_dump(by_alias=True, exclude_defaults=True),
        )
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorEnvelope(
                code=status.HTTP_404_NOT_FOUND,
                error=f"{str(e)}",
            ).model_dump(),
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorEnvelope(
                code=status.HTTP_400_BAD_REQUEST,
                error=f"Invalid input format: {str(e)}",
            ).model_dump(),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Unexpected error during file move: {type(e).__name__}: {str(e)}",
            ).model_dump(),
        )


@router.put("/dataset/update", response_model=APSuccessEnvelope)
async def update_dataset(ap_payload: APRequest):
    """Update a dataset in Neo4j"""
    ingest_url = f"{MOMA_URL}/ingestProfile2MoMa"
    # check_url = f"{MOMA_URL}/getDataset?id={dataset_id}"

    try:
        dataset, dataset_id = extract_dataset_from_AP(
            ap_payload,
            expected_ap_process="update",
            expected_operator_command="update",
        )

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

    async with httpx.AsyncClient() as client:
        try:
            # Check if a Dataset with such UUID is already stored in the Neo4j
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
            # If yes, update the dataset
            response = await client.post(ingest_url, json=dataset)
            response.raise_for_status()

            return APSuccessEnvelope(
                code=status.HTTP_200_OK,
                message=f"Dataset with ID {dataset_id} updated successfully in Neo4j",
                ap=ap_payload.model_dump(by_alias=True, exclude_defaults=True),
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


# TODO: add response model
@router.post("/dataset/query")
async def execute_query(query_data: APRequest):
    """Execute a SQL query on a dataset based on an Analytical Pattern"""
    try:
        query_info = extract_query_from_AP(query_data)
        # query = query_info.get("query")
        software = query_info.get("software")
        # args = query_info.get("args", {})
        query_filled = query_info.get("query_filled")

        result = execute_query_csv(query_filled, software)
        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute query: {str(e)}",
        )
