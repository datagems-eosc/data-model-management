from datetime import date
from enum import Enum
import json
import os
from pathlib import Path
import shutil
import duckdb
from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from .query_executor import execute_query_csv

from ..tools.AP.parse_AP import (
    extract_query_from_AP,
    extract_datasets_from_AP,
    APRequest,
)
from ..tools.AP.update_AP import (
    update_AP_after_query,
    update_dataset_id,
    update_dataset_archivedAt,
    update_output_dataset_id,
)
from ..tools.AP.generate_AP import generate_register_AP_after_query, generate_update_AP
from ..tools.S3.scratchpad import upload_dataset_to_scratchpad
from ..tools.S3.results import upload_csv_to_results, upload_ap_to_results
from ..tools.S3.catalogue import upload_dataset_to_catalogue


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
    details: Optional[Dict[str, Any]] = None


class DatasetType(str, Enum):
    TextSet = "TextSet"
    ImageSet = "ImageSet"
    CSV = "CSV"
    Table = "Table"
    RelationalDatabase = "RelationalDatabase"
    PDF = "PDF"
    Column = "Column"
    FileObject = "FileObject"  # Special value
    FileSet = "FileSet"  # Special value


class DatasetProperty(str, Enum):
    type = "type"
    name = "name"
    archivedAt = "archivedAt"
    description = "description"
    conformsTo = "conformsTo"
    citeAs = "citeAs"
    license = "license"
    url = "url"
    version = "version"
    headline = "headline"
    keywords = "keywords"
    fieldOfScience = "fieldOfScience"
    inLanguage = "inLanguage"
    country = "country"
    datePublished = "datePublished"
    access = "access"
    uploadedBy = "uploadedBy"
    distribution = "distribution"  # Special value
    recordSet = "recordSet"  # Special value


class DatasetOrderBy(str, Enum):
    id = "id"
    type = "type"
    name = "name"
    archivedAt = "archivedAt"
    description = "description"
    conformsTo = "conformsTo"
    citeAs = "citeAs"
    license = "license"
    url = "url"
    version = "version"
    headline = "headline"
    keywords = "keywords"
    fieldOfScience = "fieldOfScience"
    inLanguage = "inLanguage"
    country = "country"
    datePublished = "datePublished"
    access = "access"
    uploadedBy = "uploadedBy"


class DatasetState(str, Enum):
    Ready = "ready"
    Loaded = "loaded"
    Staged = "staged"


router = APIRouter()


MOMA_URL = os.getenv("MOMA_URL", "http://localhost:8000")


async def get_moma_object(
    node_id: str,
    expected_label: str,
    client: Optional[httpx.AsyncClient] = None,
) -> tuple[bool, dict]:
    """
    Get a single object from Neo4j via MoMa API and verify its type.

    Args:
        node_id: The UUID of the node to retrieve
        expected_label: The expected label/class (e.g., 'sc:Dataset')
        client: Optional httpx client to reuse. If None, creates a new one.

    Returns:
        Tuple of (exists: bool, metadata: dict with 'nodes')

    Raises:
        HTTPException: If MoMa API is unreachable or returns an error
    """
    url = f"{MOMA_URL}/getMoMaObject?id={node_id}"

    should_close = client is None
    if client is None:
        client = httpx.AsyncClient()

    try:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
        metadata = data.get("metadata", {})
        nodes = metadata.get("nodes", [])

        if not nodes:
            return (False, {"nodes": []})

        if len(nodes) > 1:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error=f"Expected 1 node but got {len(nodes)} for node_id={node_id}",
                ).model_dump(),
            )

        node = nodes[0]
        # Check if the node has the expected label
        labels = node.get("labels", [])
        if expected_label not in labels:
            return (False, {"nodes": []})

        return (True, {"nodes": [node]})

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
    finally:
        if should_close:
            await client.aclose()


async def get_dataset_metadata(
    dataset_id: str,
    dataset_status: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> tuple[bool, dict]:
    """
    Get dataset metadata from Neo4j via MoMa API with optional status filter.

    Args:
        dataset_id: The UUID of the dataset to retrieve
        status: Optional status filter (e.g., 'staged', 'loaded', 'ready')
        client: Optional httpx client to reuse. If None, creates a new one.

    Returns:
        Tuple of (exists: bool, metadata: dict with 'nodes' and 'edges')
    """
    url = f"{MOMA_URL}/getDatasets?nodeIds={dataset_id}"
    if dataset_status:
        url += f"&status={dataset_status}"

    should_close = client is None
    if client is None:
        client = httpx.AsyncClient()

    try:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
        metadata = data.get("metadata", {})
        nodes = metadata.get("nodes", [])
        edges = metadata.get("edges", [])

        return (len(nodes) > 0, {"nodes": nodes, "edges": edges})

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
    finally:
        if should_close:
            await client.aclose()


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


# TODO: remove metadata from the response
@router.get("/dataset", response_model=DatasetsSuccessEnvelope)
async def get_datasets(
    nodeIds: Optional[List[str]] = Query(
        None,
        description="Filter datasets by their UUIDs.",
    ),
    properties: Optional[List[DatasetProperty]] = Query(
        None,
        description="List of Dataset properties to include.",
    ),
    types: Optional[List[DatasetType]] = Query(
        None,
        description="Filter datasets based on their types.",
    ),
    orderBy: Optional[List[DatasetOrderBy]] = Query(
        None,
        description="List of Dataset properties to sort results.",
    ),
    publishedDateFrom: Optional[date] = Query(
        None, description="Minimum published date (YYYY-MM-DD).", format="YYYY-MM-DD"
    ),
    publishedDateTo: Optional[date] = Query(
        None, description="Maximum published date (YYYY-MM-DD).", format="YYYY-MM-DD"
    ),
    direction: int = Query(
        1,
        description="Direction for sorting: 1 for ascending, -1 for descending.",
        ge=-1,
        le=1,
    ),
    dataset_status: str = Query(
        "ready",
        description="Dataset status to filter on.",
    ),
):
    url = f"{MOMA_URL}/getDatasets"

    params = {}
    if nodeIds:
        params["nodeIds"] = nodeIds
    if properties:
        params["properties"] = [p.value for p in properties]
    if types:
        params["types"] = [t.value for t in types]
    if orderBy:
        params["orderBy"] = [o.value for o in orderBy]

    if publishedDateFrom:
        params["publishedDateFrom"] = publishedDateFrom.strftime("%Y-%m-%d")
    if publishedDateTo:
        params["publishedDateTo"] = publishedDateTo.strftime("%Y-%m-%d")
    params["direction"] = direction
    params["status"] = dataset_status

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            metadata = data.get("metadata")
            if isinstance(metadata, dict):
                datasets = metadata.get("nodes", [])
            else:
                datasets = []

            return DatasetsSuccessEnvelope(
                code=status.HTTP_200_OK,
                message="Datasets retrieved successfully",
                datasets=datasets,
            )

        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Error from MoMa API: {e}",
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


# This endpoint for now does not support filtering
# TODO: implement filtering by dataset state
@router.get("/dataset/{dataset_id}", response_model=DatasetSuccessEnvelope)
async def get_dataset(dataset_id: str):
    """Return dataset with a specific ID from Neo4j via MoMa API"""
    try:
        exists, metadata = await get_dataset_metadata(dataset_id)

        if not exists:
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
            dataset=metadata,
        )

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error="Unexpected Internal Server error",
            ).model_dump(),
        )


@router.post("/dataset/register", response_model=APSuccessEnvelope)
async def register_dataset(ap_payload: APRequest):
    """Register a new dataset in Neo4j"""
    ingest_url = f"{MOMA_URL}/ingestProfile2MoMa"
    try:
        datasets_list, old_dataset_ids = extract_datasets_from_AP(
            ap_payload,
            expected_ap_process="register",
            expected_operator_command="create",
        )
        if len(datasets_list) != 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="Register AP must contain exactly one dataset node.",
                ).model_dump(),
            )
        dataset = datasets_list[0]
        dataset_id = dataset.get("@id")
        old_dataset_id = old_dataset_ids[0]

        # TODO: Validate that the file referenced in dataset's 'archivedAt' property actually exists
        # at the specified S3 path before registering the dataset. This should check that the path
        # is valid and the file is accessible to prevent registering datasets with missing files.
        # Similar validation exists in the load endpoint but should also happen here for consistency.

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
            # Check if dataset already exists with 'staged' status
            exists, _ = await get_dataset_metadata(
                dataset_id,
                status=DatasetState.Staged.value.lower(),
                client=client,
            )

            if exists:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=ErrorEnvelope(
                        code=status.HTTP_409_CONFLICT,
                        error=f"Dataset with ID {dataset_id} already exists in Neo4j",
                    ).model_dump(),
                )

            # Ensure the dataset payload has status set to 'staged' before ingestion
            current_status = dataset.get("status")
            staged_status = DatasetState.Staged.value.lower()
            if not current_status or str(current_status).lower() != staged_status:
                dataset["status"] = staged_status

            # If the dataset was not found, we proceed to register.
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

        except HTTPException:
            raise
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Error from MoMa API on ingestion (Status: {e.response.status_code})",
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
@router.put("/dataset/load", response_model=APSuccessEnvelope)
async def load_dataset(ap_payload: APRequest):
    """Move dataset files from scratchpad to permanent storage"""
    DATASET_DIR = os.getenv("DATASET_DIR")
    try:
        # Extract dataset list once with exact_count=1 to validate cardinality early
        datasets_list, _ = extract_datasets_from_AP(
            ap_payload,
            expected_ap_process="load",
            expected_operator_command="update",
            exact_count=1,
        )

        dataset = datasets_list[0]
        dataset_id = dataset.get("@id")
        dataset_path = dataset.get("archivedAt")

        if not dataset_path:
            raise ValueError("Dataset 'archivedAt' property is missing")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Unexpected error during dataset extraction: {type(e).__name__}: {str(e)}",
            ).model_dump(),
        )

    # Pre-check: Verify dataset exists in Neo4j with the provided status (fallback to 'staged') before moving files
    try:
        # Reuse the dataset node extracted earlier to read status if provided
        dataset_status = dataset.get("status")

        # Use enum-backed default if not provided
        effective_status = (
            str(dataset_status).lower()
            if dataset_status
            else DatasetState.Staged.value.lower()
        )

        exists, _ = await get_dataset_metadata(dataset_id, status=effective_status)

        if not exists:
            msg = f"Dataset with ID {dataset_id} does not exist in Neo4j"
            if effective_status:
                msg += f" with status '{effective_status}'."
            msg += "Please register the dataset first using /dataset/register endpoint."
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorEnvelope(
                    code=status.HTTP_404_NOT_FOUND,
                    error=msg,
                ).model_dump(),
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Failed to verify dataset existence in Neo4j: {type(e).__name__}: {str(e)}",
            ).model_dump(),
        )

    # Validate and move the dataset files
    try:
        if not dataset_path.startswith("s3://"):
            raise ValueError(
                f"Invalid S3 URI. Found '{dataset_path}'. Must start with s3://"
            )

        path_without_s3_prefix = dataset_path.split("s3://", 1)[1]
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

    except FileNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorEnvelope(
                code=status.HTTP_404_NOT_FOUND,
                error=f"{str(e)}",
            ).model_dump(),
        )
    except FileExistsError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=ErrorEnvelope(
                code=status.HTTP_409_CONFLICT,
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

    # Update the dataset metadata in Neo4j
    try:
        update_ap = generate_update_AP(ap_payload, new_path)
        await update_dataset(update_ap)

    except HTTPException as exc:
        # Rollback: Move file back to original location
        rollback_error = None
        try:
            if target_path.exists():
                shutil.move(str(target_path), str(source_path))
        except Exception as e:
            rollback_error = e

        # Extract error details
        error_detail = exc.detail
        if isinstance(error_detail, dict):
            error_msg = error_detail.get("error", str(error_detail))
            error_code = error_detail.get("code", exc.status_code)
        else:
            error_msg = str(error_detail)
            error_code = exc.status_code

        if rollback_error:
            error_msg = f"{error_msg} [ROLLBACK FAILED: {type(rollback_error).__name__}: {str(rollback_error)}. File may be orphaned at {target_path}]"

        raise HTTPException(
            status_code=error_code,
            detail=ErrorEnvelope(
                code=error_code,
                error=f"Dataset load failed during Neo4j update (file rolled back to {dataset_path}): {error_msg}",
            ).model_dump(),
        )
    except Exception as e:
        # Rollback: Move file back to original location
        rollback_error = None
        try:
            if target_path.exists():
                shutil.move(str(target_path), str(source_path))
        except Exception as rollback_exc:
            rollback_error = rollback_exc

        error_msg = f"{type(e).__name__}: {str(e)}"
        if rollback_error:
            error_msg = f"{error_msg} [ROLLBACK FAILED: {type(rollback_error).__name__}: {str(rollback_error)}. File may be orphaned at {target_path}]"

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Dataset load failed during Neo4j update (file rolled back to {dataset_path}): {error_msg}",
            ).model_dump(),
        )

    # Upload dataset to catalogue
    try:
        ap_payload = update_dataset_archivedAt(ap_payload, dataset_id, new_path)

        dataset = datasets_list[0]
        json_dataset = json.dumps(dataset, indent=2)
        upload_dataset_to_catalogue(json_dataset, dataset_id)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Dataset updated in Neo4j but failed to upload to catalogue: {type(e).__name__}: {str(e)}",
            ).model_dump(),
        )

    return APSuccessEnvelope(
        code=status.HTTP_200_OK,
        message=f"Dataset moved from {dataset_path} to {new_path}",
        ap=ap_payload.model_dump(by_alias=True, exclude_defaults=True),
    )


@router.put("/dataset/update", response_model=APSuccessEnvelope)
async def update_dataset(ap_payload: APRequest):
    """Update a dataset in Neo4j"""
    ingest_url = f"{MOMA_URL}/ingestProfile2MoMa"

    try:
        datasets_list, _ = extract_datasets_from_AP(
            ap_payload,
            expected_ap_process="update",
            expected_operator_command="update",
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
        results = []
        errors = {}

        for dataset in datasets_list:
            dataset_id = dataset.get("@id")
            try:
                # Check if dataset exists independently of its status
                exists, _ = await get_moma_object(
                    dataset_id, expected_label="sc:Dataset", client=client
                )

                if not exists:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=ErrorEnvelope(
                            code=status.HTTP_404_NOT_FOUND,
                            error=f"Dataset with ID {dataset_id} does not exist in Neo4j",
                        ).model_dump(),
                    )

                response = await client.post(ingest_url, json=dataset)
                response.raise_for_status()
                results.append(dataset_id)

            except HTTPException:
                raise
            except httpx.HTTPStatusError as e:
                errors[dataset_id] = {
                    "status_code": e.response.status_code,
                    "message": e.response.text or f"HTTP {e.response.status_code}",
                }
            except httpx.RequestError as e:
                errors[dataset_id] = {
                    "status_code": 503,
                    "message": f"Connection Error: {str(e)}",
                }
            except Exception as e:
                errors[dataset_id] = {
                    "status_code": 500,
                    "message": f"{type(e).__name__}: {str(e)}",
                }

        if errors:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Failed to update {len(errors)} dataset(s)",
                    details=errors,
                ).model_dump(),
            )

        return APSuccessEnvelope(
            code=status.HTTP_200_OK,
            message=f"{len(results)} dataset(s) updated successfully in Neo4j",
            ap=ap_payload.model_dump(by_alias=True, exclude_defaults=True),
        )


@router.post("/polyglot/query", response_model=APSuccessEnvelope)
async def execute_query(ap_payload: APRequest):
    """Execute a SQL query on a dataset based on an Analytical Pattern"""
    try:
        query_info = extract_query_from_AP(ap_payload)
        software = query_info.get("software")
        query_filled = query_info.get("query_filled")

        result = execute_query_csv(query_filled, software)

        ap_payload, dataset_id = update_output_dataset_id(ap_payload)
        csv_bytes = result.to_csv(index=False).encode("utf-8")
        upload_path, dataset_id = upload_csv_to_results(csv_bytes, dataset_id)

        AP_query_after = update_AP_after_query(ap_payload, dataset_id, upload_path)
        upload_ap_to_results(
            json.dumps(
                AP_query_after.model_dump(by_alias=True, exclude_defaults=True),
                ensure_ascii=False,
                indent=2,
            ),
            dataset_id,
        )

        register_AP = generate_register_AP_after_query(AP_query_after)
        await register_dataset(register_AP)

        # Fake forward to AP Storage API
        try:
            print(f"Dataset {dataset_id} sent to the AP Storage API.")
        except Exception as e:
            print(f"AP Storage API not working: {e}")

        return APSuccessEnvelope(
            code=status.HTTP_200_OK,
            message=f"Query executed successfully, results stored at {upload_path}",
            ap=AP_query_after.model_dump(by_alias=True, exclude_defaults=True),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute query: {str(e)}",
        )


@router.get("/test-postgres-duckdb")
async def test_postgres_connection():
    DB_HOST = os.getenv("DATAGEMS_POSTGRES_HOST")
    DB_PORT = os.getenv("DATAGEMS_POSTGRES_PORT")
    DB_USER = os.getenv("DS_READER_USER")
    DB_PASSWORD = os.getenv("DS_READER_PS")
    DB_NAME = "ds_era5_land"

    if not all([DB_HOST, DB_PORT, DB_USER, DB_PASSWORD]):
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "error": "Missing one or more required environment variables for DB connection.",
            },
        )

    connection_string = (
        f"dbname={DB_NAME} "
        f"user={DB_USER} "
        f"password={DB_PASSWORD} "
        f"host={DB_HOST} "
        f"port={DB_PORT}"
    )

    con = None
    try:
        con = duckdb.connect()
        con.sql("INSTALL postgres;")
        con.sql("LOAD postgres;")

        attach_query = f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);"
        con.sql(attach_query)

        test_query = "SELECT now() FROM pg_db.information_schema.tables LIMIT 1;"
        con.sql(test_query).fetchall()

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "code": status.HTTP_200_OK,
                "message": f"Successfully connected to PostgreSQL database '{DB_NAME}' via DuckDB and ran a test query.",
            },
        )

    except duckdb.Error as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "code": status.HTTP_503_SERVICE_UNAVAILABLE,
                "error": f"PostgreSQL connection failed via DuckDB: {type(e).__name__}: {str(e)}",
            },
        )

    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "error": f"Unexpected error during connection test: {type(e).__name__}: {str(e)}",
            },
        )

    finally:
        if con:
            con.close()
