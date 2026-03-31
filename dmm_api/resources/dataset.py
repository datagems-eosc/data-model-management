from datetime import date
from enum import Enum
import json
import os
from pathlib import Path
import shutil
from dmm_api.resources.converter import convertProfile

import duckdb
import structlog
from fastapi import (
    APIRouter,
    Body,
    File,
    Form,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
    Depends,
)
from fastapi.responses import JSONResponse
import httpx
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

import dmm_api.resources.security as security

from ..tools.AP.parse_AP import (
    compare_node_properties,
    extract_from_AP,
    APRequest,
    group_datasets_by_components,
)
from ..tools.AP.update_AP import (
    update_dataset_archivedAt,
)
from ..tools.AP.generate_AP import generate_update_AP
from ..tools.S3.scratchpad import upload_dataset_to_scratchpad

# from ..tools.S3.results import upload_csv_to_results, upload_ap_to_results
from ..tools.S3.catalogue import upload_dataset_to_catalogue

logger = structlog.get_logger(__name__)


class WrappedAPRequest(BaseModel):
    ap: APRequest


class APSuccessEnvelope(BaseModel):
    code: int
    message: str
    ap: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None


class APResponseSuccessEnvelope(BaseModel):
    code: int
    message: str
    content: Dict[str, Any]


class DatasetSuccessEnvelope(BaseModel):
    code: int
    message: str
    dataset: Dict[str, Any]


class DatasetsSuccessEnvelope(BaseModel):
    code: int
    message: str
    datasets: List[Dict[str, Any]]
    offset: Optional[int] = None
    count: Optional[int] = None
    total: Optional[int] = None


class ErrorEnvelope(BaseModel):
    code: int
    error: str
    details: Optional[Dict[str, Any]] = None


class DatasetType(str, Enum):
    FileObject = "cr:FileObject"  # Special value
    FileSet = "cr:FileSet"  # Special value
    Field = "cr:Field"
    TextSet = "TextSet"
    ImageSet = "ImageSet"
    CSV = "CSV"
    Table = "Table"
    RelationalDatabase = "RelationalDatabase"
    PDF = "PDF"
    Column = "Column"


class MimeType(str, Enum):
    application_vnd_ms_excel = "application/vnd.ms-excel"
    application_x_ipynb_json = "application/x-ipynb+json"
    application_docx = "application/docx"
    application_pptx = "application/pptx"
    application_pdf = "application/pdf"
    image_jpeg = "image/jpeg"
    image_png = "image/png"
    text_csv = "text/csv"
    text_sql = "text/sql"
    application_xlsx = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    application_docx_ooxml = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )


class DatasetProperty(str, Enum):
    type = "type"
    name = "name"
    archivedAt = "archivedAt"
    description = "description"
    conformsTo = "conformsTo"
    citeAs = "citeAs"
    license = "license"
    url = "url"
    doi = "doi"
    version = "version"
    headline = "headline"
    keywords = "keywords"
    fieldOfScience = "fieldOfScience"
    inLanguage = "inLanguage"
    country = "country"
    datePublished = "datePublished"
    access = "access"
    uploadedBy = "uploadedBy"
    status = "status"
    distribution = "distribution"  # Special value
    recordSet = "recordSet"  # Special value


# TODO: add sorting by Dataset size
class DatasetOrderBy(str, Enum):
    id = "id"
    name = "name"
    license = "license"
    version = "version"
    datePublished = "datePublished"


class DatasetState(str, Enum):
    Ready = "ready"
    Loaded = "loaded"
    Staged = "staged"


router = APIRouter()


MOMA_URL = os.getenv("MOMA_URL", "https://datagems-dev.scayle.es/moma2/v1/api")
CDD_URL = os.getenv("CDD_URL", "https://datagems-dev.scayle.es/cross-dataset-discovery")
IDD_URL = os.getenv("IDD_URL")
CDD_REQUEST_TIMEOUT_SECONDS = 30.0


EXTERNAL_SERVICES = {
    "/cross-dataset-discovery/search": {
        "url": f"{CDD_URL}/search-ap/",
        "name": "Cross-Dataset Discovery",
    },
    "/in-dataset-discovery/text2sql": {
        "url": f"{IDD_URL}/text2sql",
        "name": "In-Dataset Discovery (text2sql)",
    },
}
CDD_EXCHANGE_SCOPE = os.getenv("CDD_EXCHANGE_SCOPE", "cross-dataset-discovery-api")


# TODO: remove this function
async def get_moma_object(
    node_id: str,
    expected_label: str,
    client: Optional[httpx.AsyncClient] = None,
) -> tuple[bool, dict]:
    """
    Get a single object from Neo4j via MoMa API and verify its type.

    Args:
        node_id: The UUID of the node to retrieve
        expected_label: The expected label/class (e.g., 'Dataset')
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
            # Object exists but is not of the expected class/label
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=ErrorEnvelope(
                    code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    error=(
                        f"Label mismatch for MoMa object id={node_id}: "
                        f"expected '{expected_label}' not in labels {labels}"
                    ),
                    details={
                        "id": node_id,
                        "expected_label": expected_label,
                        "labels": labels,
                    },
                ).model_dump(),
            )

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
    token: str,
    dataset_status: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> tuple[bool, dict]:
    """
    Get dataset metadata from Neo4j via MoMa API with optional status filter.

    Args:
        dataset_id: The UUID of the dataset to retrieve
        token: The authorization token for the MoMa API
        dataset_status: Optional status filter (e.g., 'staged', 'loaded', 'ready') for Dataset nodes
        client: Optional httpx client to reuse. If None, creates a new one.

    Returns:
        Tuple of (exists: bool, metadata: dict with 'nodes' and 'edges')
    """
    url = f"{MOMA_URL}/datasets/{dataset_id}"
    params = {"status": dataset_status} if dataset_status else {}
    
    should_close = client is None
    if client is None:
        client = httpx.AsyncClient(follow_redirects=True)

    try:
        response = await client.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {token}"}
        )
        
        if response.status_code == 404:
            return (False, {"nodes": [], "edges": []})
        
        response.raise_for_status()
        data = response.json()
        
        nodes = data.get("nodes", [])
        edges = data.get("edges", [])
        dataset_exists = any(node.get("id") == dataset_id for node in nodes)
        
        return (dataset_exists, {"nodes": nodes, "edges": edges})
        
    except httpx.HTTPStatusError as e:
        logger.error(f"MoMa API error: {e.response.status_code}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_dataset_metadata: {e}", exc_info=True)
        raise
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


@router.get("/dataset")
async def dataset_home():
    return {
        "endpoints": {
            "search": "/dataset/search",
            "by_id": "/dataset/get/{dataset_id}",
        },
        "description": "Endpoints to retrieve and search datasets stored in MoMa",
    }


@router.get("/dataset/search", response_model=DatasetsSuccessEnvelope)
async def search_datasets(
    nodeIds: Optional[List[str]] = Query(
        None, description="Filter datasets by their UUIDs."
    ),
    properties: Optional[List[DatasetProperty]] = Query(
        None, description="List of Dataset properties to include."
    ),
    types: Optional[List[DatasetType]] = Query(
        None, description="Filter datasets based on their types."
    ),
    orderBy: Optional[List[DatasetOrderBy]] = Query(
        None, description="List of Dataset properties to sort results."
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
    dataset_status: Optional[str] = Query(
        None,
        description="Optional dataset status to filter on. If not provided, returns all statuses.",
    ),
    offset: Optional[int] = Query(
        None, description="Number of results to skip for pagination.", ge=0
    ),
    count: Optional[int] = Query(
        None, description="Maximum number of results to return.", ge=1
    ),
    mimeTypes: Optional[List[MimeType]] = Query(
        None,
        description="Filter datasets by file MIME types (from FileObject encodingFormat).",
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
    if dataset_status is not None:
        params["status"] = dataset_status
    if offset is not None:
        params["offset"] = offset
    if count is not None:
        params["count"] = count
    if mimeTypes:
        params["mimeTypes"] = [m.value for m in mimeTypes]

    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            metadata = data.get("metadata")
            if isinstance(metadata, dict):
                all_nodes = metadata.get("nodes", [])
                all_edges = metadata.get("edges", [])
            else:
                all_nodes = []
                all_edges = []

            response_offset = data.get("offset")
            response_count = data.get("count")
            response_total = data.get("total")

            datasets = group_datasets_by_components(all_nodes, all_edges)
            return DatasetsSuccessEnvelope(
                code=status.HTTP_200_OK,
                message="Datasets retrieved successfully",
                datasets=datasets,
                offset=response_offset,
                count=response_count,
                total=response_total,
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
@router.get("/dataset/get/{dataset_id}", response_model=DatasetSuccessEnvelope)
async def get_dataset(
    dataset_id: str, 
    format: str = Query(None, alias="format"),
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope),
):
    """Return dataset with a specific ID from Neo4j via MoMa API"""
    try:
        exists, metadata = await get_dataset_metadata(dataset_id, token=token)

        if not exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=ErrorEnvelope(
                    code=status.HTTP_404_NOT_FOUND,
                    error=f"Dataset with ID {dataset_id} not found in Neo4j",
                ).model_dump(),
            )

        if format == "croissant":
            croissant_jsonld = convertProfile(pgjson=metadata)
            metadata = json.loads(croissant_jsonld)

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


@router.post(
    "/dataset/register",
    response_model=APSuccessEnvelope,
    response_model_exclude_none=True
)
async def register_dataset(
    wrapped: WrappedAPRequest, 
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope)):
    """
    Register a new dataset in Neo4j by:
    1. Extracting the Dataset node from the AP
    2. Checking it does not already exist → 409 if it does
    3. Creating it via POST /datasets
    """
    ap_payload = wrapped.ap

    try:
        # Extract only Dataset nodes
        filtered_nodes, filtered_edges = extract_from_AP(
            ap_payload, target_labels={"sc:Dataset"}
        )

        if not filtered_nodes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="No Dataset node found in AP",
                ).model_dump(),
            )

        if len(filtered_nodes) != 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="Register AP must contain exactly one Dataset node.",
                ).model_dump(),
            )

        dataset_node = filtered_nodes[0]
        dataset_id = dataset_node.get("id")

    # TODO: Validate that the file referenced in dataset's 'archivedAt' property actually exists
    # at the specified S3 path before registering the dataset. This should check that the path
    # is valid and the file is accessible to prevent registering datasets with missing files.

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

    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            # Check if dataset already exists
            exists, _ = await get_dataset_metadata(dataset_id, token=token, client=client)
            
            if exists:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=ErrorEnvelope(
                        code=status.HTTP_409_CONFLICT,
                        error=f"Dataset with ID {dataset_id} already exists in Neo4j",
                    ).model_dump(),
                )

            # Create the dataset node via POST /datasets
            post_url = f"{MOMA_URL}/datasets/"
            post_data = {"nodes": filtered_nodes, "edges": filtered_edges}
            
            response = await client.post(
                post_url,
                json=post_data,
                headers={"Authorization": f"Bearer {token}"}
            )
            
            response.raise_for_status()

            # Fake forward to AP Storage API
            logger.info(f"Dataset {dataset_id} would be sent to AP Storage API")
            
            result = APSuccessEnvelope(
                code=status.HTTP_201_CREATED,
                message=f"Dataset with ID {dataset_id} registered successfully in Neo4j",
                ap=ap_payload.model_dump(by_alias=True, exclude_defaults=True),
            )
            return result

        except HTTPException:
            raise
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Error from MoMa API (Status: {e.response.status_code})",
                ).model_dump(),
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error=f"Unexpected error: {type(e).__name__}: {str(e)}",
                ).model_dump(),
            )
        

# TODO: check if dataset with such ID is already registered and is in "loaded" state
@router.put(
    "/dataset/load", response_model=APSuccessEnvelope, response_model_exclude_none=True
)
async def load_dataset(
    wrapped: WrappedAPRequest,
    force: bool = Query(False),
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope),
):
    """Move dataset files from scratchpad to permanent storage and update Neo4j"""
    DATASET_DIR = os.getenv("DATASET_DIR")
    ap_payload = wrapped.ap

    try:
        # Extract only Dataset nodes from AP
        filtered_nodes, _ = extract_from_AP(
            ap_payload, target_labels={"sc:Dataset"}
        )

        if not filtered_nodes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="No Dataset node found in AP",
                ).model_dump(),
            )

        if len(filtered_nodes) != 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error=f"Expected exactly 1 Dataset node, found {len(filtered_nodes)}",
                ).model_dump(),
            )

        dataset_node = filtered_nodes[0]
        dataset_id = dataset_node.get("id")
        dataset_props = dataset_node.get("properties", {})
        dataset_path = dataset_props.get("archivedAt")  
        dataset_status = dataset_props.get("status")    

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
        # Use enum-backed default if not provided
        effective_status = (
            str(dataset_status).lower()
            if dataset_status
            else DatasetState.Staged.value.lower()
        )

        exists, _ = await get_dataset_metadata(
            dataset_id, token=token, dataset_status=effective_status
        )

        if not exists:
            msg = f"Dataset with ID {dataset_id} does not exist in Neo4j"
            if effective_status:
                msg += f" with status '{effective_status}'."
            msg += (
                "Please register the dataset first using /dataset/register endpoint."
            )
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
            # If force is True and source and target resolve to the same path, skip the move
            if force and source_path.resolve() == target_path.resolve():
                pass
            else:
                raise FileExistsError(
                    f"Target dataset with id {dataset_id} has already been moved to: {target_path}"
                )
        else:
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

    # Update the dataset metadata in Neo4j via PATCH /nodes/{id}
    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            response = await client.patch(
                f"{MOMA_URL}/nodes/{dataset_id}",
                json={
                    "archivedAt": new_path,             
                    "status": DatasetState.Loaded.value,
                },
                headers={"Authorization": f"Bearer {token}"},
            )
            response.raise_for_status()

            # # Update the ap_payload's dataset node with new values
            # for node in filtered_nodes:
            #     if node.get("id") == dataset_id:
            #         node["properties"]["archivedAt"] = new_path
            #         node["properties"]["status"] = DatasetState.Loaded.value
            #         break

        except HTTPException:
            raise
        except httpx.HTTPStatusError as e:
            # Rollback: Move file back to original location
            rollback_error = None
            try:
                if target_path.exists():
                    shutil.move(str(target_path), str(source_path))
            except Exception as rollback_exc:
                rollback_error = rollback_exc

            error_msg = f"Error from MoMa API (Status: {e.response.status_code})"
            if rollback_error:
                error_msg += f" [ROLLBACK FAILED: {type(rollback_error).__name__}: {str(rollback_error)}. File may be orphaned at {target_path}]"

            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
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
                error_msg += f" [ROLLBACK FAILED: {type(rollback_error).__name__}: {str(rollback_error)}. File may be orphaned at {target_path}]"

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

        # Reconstruct dataset for catalogue
        dataset_for_catalogue = {
            "id": dataset_id,
            "archivedAt": new_path,
            **dataset_props,
        }

        json_dataset = json.dumps(dataset_for_catalogue, indent=2)
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


@router.put(
    "/dataset/update",
    response_model=APSuccessEnvelope,
    response_model_exclude_none=True,
)
async def update_dataset(
    wrapped: WrappedAPRequest,
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope),
):
    """
    Update datasets in Neo4j by:
    1. Extracting all nodes/edges from AP
    2. Verifying the root Dataset node(s) already exist → 404 if not
    3. Fetching current state for each dataset using GET /datasets/{id}
    4. Updating nodes via PATCH /nodes/{id}
    5. Creating new nodes via POST /nodes/
    6. Creating edges via POST /edges/
    """
    ap_payload = wrapped.ap
    try:
        filtered_nodes, filtered_edges = extract_from_AP(ap_payload)

        if not filtered_nodes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="No Dataset/FileObject/RecordSet nodes found in AP",
                ).model_dump(),
            )

        dataset_ids = [
            node["id"]
            for node in filtered_nodes
            if "sc:Dataset" in node.get("labels", [])
        ]

        if not dataset_ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="No Dataset nodes found in AP",
                ).model_dump(),
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Failed to parse AP: {type(e).__name__}: {str(e)}",
            ).model_dump(),
        )

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # Step 1: Fetch current state for each dataset using GET /datasets/{id}
        current_nodes = {}
        current_edges = set()
        
        try:
            for dataset_id in dataset_ids:
                # Use the working single-dataset endpoint
                exists, metadata = await get_dataset_metadata(dataset_id, token=token, client=client)
                
                if not exists:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=ErrorEnvelope(
                            code=status.HTTP_404_NOT_FOUND,
                            error=(
                                f"Dataset with ID {dataset_id} not found in Neo4j. "
                                "Please register the dataset first using /dataset/register."
                            ),
                        ).model_dump(),
                    )
                
                # Store current nodes and edges
                for node in metadata.get("nodes", []):
                    node_id = node.get("id")
                    if node_id:
                        current_nodes[node_id] = node
                
                for edge in metadata.get("edges", []):
                    edge_from = edge.get("from")
                    edge_to = edge.get("to")
                    edge_labels = tuple(edge.get("labels", []) or [])
                    if edge_from and edge_to:
                        current_edges.add((edge_from, edge_to, edge_labels))

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error=f"Failed to fetch current state: {type(e).__name__}: {str(e)}",
                ).model_dump(),
            )

        # Step 2: Calculate what needs to be added/updated
        nodes_to_create = []
        nodes_to_update = []
        edges_to_create = []
        
        has_record_set = any(
            "cr:RecordSet" in node.get("labels", []) for node in filtered_nodes
        )
        
        # Process nodes
        for node in filtered_nodes:
            node_id = node["id"]
            is_dataset = "sc:Dataset" in node.get("labels", [])
            node_props = node.get("properties", {}).copy()
            
            # If RecordSet is present, force status to 'ready' on Dataset nodes
            if has_record_set and is_dataset:
                node_props["status"] = DatasetState.Ready.value
            
            if node_id not in current_nodes:
                nodes_to_create.append(node)
            else:
                # Check if properties changed
                current_props = current_nodes[node_id].get("properties", {})
                changed_props = {}
                
                for key, value in node_props.items():
                    if current_props.get(key) != value:
                        changed_props[key] = value
                
                if changed_props:
                    nodes_to_update.append({"id": node_id, "properties": changed_props})
        
        # Process edges
        for edge in filtered_edges:
            edge_from = edge["from"]
            edge_to = edge["to"]
            edge_labels = tuple(edge.get("labels", []) or [])
            edge_key = (edge_from, edge_to, edge_labels)
            
            if edge_key not in current_edges:
                edges_to_create.append(edge)
        
        # Step 3: Create new nodes (if any)
        if nodes_to_create:
            try:
                response = await client.post(
                    f"{MOMA_URL}/nodes/",
                    json={"nodes": nodes_to_create},
                    headers={"Authorization": f"Bearer {token}"},
                )
                response.raise_for_status()
                logger.info(f"Created {len(nodes_to_create)} node(s)")
            except Exception as e:
                logger.error(f"Failed to create nodes: {e}")
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=ErrorEnvelope(
                        code=status.HTTP_502_BAD_GATEWAY,
                        error=f"Failed to create nodes: {str(e)}",
                    ).model_dump(),
                )
        
        # Step 4: Update existing nodes (if any)
        if nodes_to_update:
            try:
                for node in nodes_to_update:
                    response = await client.patch(
                        f"{MOMA_URL}/nodes/{node['id']}",
                        json=node["properties"],
                        headers={"Authorization": f"Bearer {token}"},
                    )
                    response.raise_for_status()
                logger.info(f"Updated {len(nodes_to_update)} node(s)")
            except Exception as e:
                logger.error(f"Failed to update nodes: {e}")
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=ErrorEnvelope(
                        code=status.HTTP_502_BAD_GATEWAY,
                        error=f"Failed to update nodes: {str(e)}",
                    ).model_dump(),
                )
        
        # Step 5: Create new edges (if any)
        if edges_to_create:
            try:
                response = await client.post(
                    f"{MOMA_URL}/edges/",
                    json={"edges": edges_to_create},
                    headers={"Authorization": f"Bearer {token}"},
                )
                response.raise_for_status()
                logger.info(f"Created {len(edges_to_create)} edge(s)")
            except Exception as e:
                logger.error(f"Failed to create edges: {e}")
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=ErrorEnvelope(
                        code=status.HTTP_502_BAD_GATEWAY,
                        error=f"Failed to create edges: {str(e)}",
                    ).model_dump(),
                )
        
        # Step 6: Build summary
        summary_parts = []
        if nodes_to_create:
            summary_parts.append(f"{len(nodes_to_create)} node(s) created")
        if nodes_to_update:
            summary_parts.append(f"{len(nodes_to_update)} node(s) updated")
        if edges_to_create:
            summary_parts.append(f"{len(edges_to_create)} edge(s) added")
        if has_record_set:
            summary_parts.append("dataset status set to 'ready'")
        
        if not summary_parts:
            summary_parts.append("No changes detected")
        
        message = f"Dataset update completed: {', '.join(summary_parts)}"
        
        # Build AP response
        ap_data = ap_payload.model_dump(by_alias=True, exclude_defaults=True)
        
        metadata = {
            "summary": {
                "nodes_created": len(nodes_to_create),
                "nodes_updated": len(nodes_to_update),
                "edges_added": len(edges_to_create),
                "record_set_detected": has_record_set,
                "datasets_processed": dataset_ids,
            }
        }
        
        return APSuccessEnvelope(
            code=status.HTTP_200_OK,
            message=message,
            ap=ap_data,
            metadata=metadata,
        )


@router.put(
    "/dataset/update",
    response_model=APSuccessEnvelope,
    response_model_exclude_none=True,
)
async def update_dataset(
    wrapped: WrappedAPRequest,
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope),
):
    """
    Update datasets in Neo4j by:
    1. Extracting all nodes/edges from AP
    2. Verifying the root Dataset node(s) already exist → 404 if not
    3. Fetching current state from MoMa using search endpoint
    4. Sending everything to MoMa via POST /datasets/
    5. Returning report of what was added/updated
    """
    ap_payload = wrapped.ap
    try:
        filtered_nodes, filtered_edges = extract_from_AP(ap_payload)

        if not filtered_nodes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="No Dataset/FileObject/RecordSet nodes found in AP",
                ).model_dump(),
            )

        dataset_ids = [
            node["id"]
            for node in filtered_nodes
            if "sc:Dataset" in node.get("labels", [])
        ]

        if not dataset_ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="No Dataset nodes found in AP",
                ).model_dump(),
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Failed to parse AP: {type(e).__name__}: {str(e)}",
            ).model_dump(),
        )

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # Step 1: Fetch current state for all datasets at once using search
        current_nodes = {}
        current_edges = set()
        
        try:
            # Use the search endpoint with multiple dataset IDs
            url = f"{MOMA_URL}/datasets/"
            params = {"nodeIds": dataset_ids}
            
            response = await client.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {token}"}
            )
            response.raise_for_status()
            
            data = response.json()
            
            all_nodes = data.get("nodes", [])
            all_edges = data.get("edges", [])
            
            # Check if all requested datasets exist
            found_dataset_ids = set()
            for node in all_nodes:
                if "sc:Dataset" in node.get("labels", []):
                    node_id = node.get("id")
                    if node_id:
                        found_dataset_ids.add(node_id)
                        current_nodes[node_id] = node
            
            # Verify all datasets exist
            missing_datasets = set(dataset_ids) - found_dataset_ids
            if missing_datasets:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=ErrorEnvelope(
                        code=status.HTTP_404_NOT_FOUND,
                        error=(
                            f"Datasets with IDs {', '.join(missing_datasets)} not found in Neo4j. "
                            "Please register them first using /dataset/register."
                        ),
                    ).model_dump(),
                )
            
            # Store all current nodes by ID for comparison
            for node in all_nodes:
                node_id = node.get("id")
                if node_id:
                    current_nodes[node_id] = node
            
            # Store all current edges for comparison
            for edge in all_edges:
                edge_from = edge.get("from")
                edge_to = edge.get("to")
                edge_labels = tuple(edge.get("labels", []) or [])
                if edge_from and edge_to:
                    current_edges.add((edge_from, edge_to, edge_labels))

        except HTTPException:
            raise
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Failed to fetch current state: HTTP {e.response.status_code}",
                ).model_dump(),
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error=f"Failed to connect to MoMa API: {str(e)}",
                ).model_dump(),
            )

        # Step 2: Calculate what will be added/updated
        nodes_to_add = []
        nodes_to_update = []
        edges_to_add = []
        
        # Check which nodes are new or need updating
        for node in filtered_nodes:
            node_id = node["id"]
            if node_id not in current_nodes:
                nodes_to_add.append(node)
            else:
                # Check if properties changed
                current_node = current_nodes[node_id]
                current_props = current_node.get("properties", {})
                new_props = node.get("properties", {})
                
                if current_props != new_props:
                    nodes_to_update.append(node)
        
        # Check which edges are new
        for edge in filtered_edges:
            edge_from = edge["from"]
            edge_to = edge["to"]
            edge_labels = tuple(edge.get("labels", []) or [])
            edge_key = (edge_from, edge_to, edge_labels)
            
            if edge_key not in current_edges:
                edges_to_add.append(edge)
        
        has_record_set = any(
            "cr:RecordSet" in node.get("labels", []) for node in filtered_nodes
        )
        
        # Step 3: Inject 'ready' status into Dataset nodes when a RecordSet is present
        if has_record_set:
            for node in filtered_nodes:
                if "sc:Dataset" in node.get("labels", []):
                    node.setdefault("properties", {})["dg:status"] = DatasetState.Ready.value
        
        # Step 4: Send everything to MoMa
        try:
            response = await client.post(
                f"{MOMA_URL}/datasets/",
                json={"nodes": filtered_nodes, "edges": filtered_edges},
                headers={"Authorization": f"Bearer {token}"},
            )
            response.raise_for_status()

        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Failed to upsert dataset: HTTP {e.response.status_code}",
                ).model_dump(),
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error=f"Failed to connect to MoMa API: {str(e)}",
                ).model_dump(),
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error=f"Unexpected error during upsert: {type(e).__name__}: {str(e)}",
                ).model_dump(),
            )
        
        # Step 5: Build detailed summary of what changed
        summary_parts = []
        if nodes_to_add:
            summary_parts.append(f"{len(nodes_to_add)} node(s) added")
        if nodes_to_update:
            summary_parts.append(f"{len(nodes_to_update)} node(s) updated")
        if edges_to_add:
            summary_parts.append(f"{len(edges_to_add)} edge(s) added")
        if has_record_set:
            summary_parts.append("dataset status set to 'ready'")
        
        if not summary_parts:
            summary_parts.append("No changes detected")
        
        message = f"Dataset update completed: {', '.join(summary_parts)}"
        
        # Build AP response
        ap_data = ap_payload.model_dump(by_alias=True, exclude_defaults=True)
        if has_record_set:
            for node in ap_data["nodes"]:
                if "sc:Dataset" in node.get("labels", []):
                    node.setdefault("properties", {})["dg:status"] = DatasetState.Ready.value
        
        # Include detailed change info in metadata
        metadata = {
            "summary": {
                "nodes_added": len(nodes_to_add),
                "nodes_updated": len(nodes_to_update),
                "edges_added": len(edges_to_add),
                "record_set_detected": has_record_set,
                "datasets_processed": list(found_dataset_ids) if 'found_dataset_ids' in locals() else dataset_ids,
            }
        }
        
        return APSuccessEnvelope(
            code=status.HTTP_200_OK,
            message=message,
            ap=ap_data,
            metadata=metadata,
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


@router.post("/in-dataset-discovery/text2sql", response_model=APResponseSuccessEnvelope)
@router.post(
    "/cross-dataset-discovery/search", response_model=APResponseSuccessEnvelope
)
async def execute_and_store(
    request: Request,
    file: Optional[UploadFile] = File(None),
    body: Optional[WrappedAPRequest] = Body(None),
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope),
) -> APResponseSuccessEnvelope:
    """Generic handler: forward AP to the appropriate service, store it, return full response.

    Accepts AP in either format:
    - Multipart form with file upload: file=@path/to/file.json
    - JSON body: {"ap": {...}}
    """
    # Strip the API prefix to get the route path
    route_path = request.url.path.replace("/api/v1", "", 1)
    if route_path not in EXTERNAL_SERVICES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorEnvelope(
                code=status.HTTP_404_NOT_FOUND,
                error=f"Unknown endpoint: {route_path}",
            ).model_dump(),
        )
    service = EXTERNAL_SERVICES[route_path]

    # Parse payload from either file or JSON body
    payload_data = None

    if file:
        # Read and parse the uploaded JSON file
        content = await file.read()
        try:
            payload_data = json.loads(content)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error=f"Invalid JSON in uploaded file: {str(e)}",
                ).model_dump(),
            )
    elif body:
        # Use JSON body directly (automatic FastAPI parsing)
        payload_data = {"ap": body.ap.model_dump()}
    else:
        # Fallback: manually try to parse JSON body if automatic parsing didn't work
        try:
            body_content = await request.body()
            if body_content:
                payload_data = json.loads(body_content)
        except (json.JSONDecodeError, ValueError):
            pass

    if not payload_data or "ap" not in payload_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorEnvelope(
                code=status.HTTP_400_BAD_REQUEST,
                error="Request must include either a JSON file upload or JSON body with 'ap' field",
            ).model_dump(),
        )

    # Extract ap and metadata from the uploaded file to store only ap
    ap = payload_data.get("ap", {})
    try:
        print(f"[{service['name']}] Storing AP in AP Storage:")
        print(json.dumps(ap, indent=2))
        print(f"[{service['name']}] AP stored successfully.")
    except Exception as e:
        print(f"[{service['name']}] AP Storage failed: {e}")

    # exchanged_token = await _exchange_token_for_cdd(
    #     user_token=token,
    # )
    # if not exchanged_token:
    #     logger.warning(
    #         "Could not obtain a cdd token. The request to the external service will be made without authentication, which may lead to failure if the service requires a valid token."
    #     )
    #     return APResponseSuccessEnvelope(
    #         code=status.HTTP_200_OK,
    #         message=f"AP stored successfully, but failed to obtain token for {service['name']}. Request sent without authentication.",
    #         content={
    #             "warning": "Failed to obtain token for external service. Request sent without authentication."
    #         },
    #     )

    async with httpx.AsyncClient(
        timeout=CDD_REQUEST_TIMEOUT_SECONDS, follow_redirects=True
    ) as client:
        response = await client.post(
            service["url"],
            headers={"Authorization": f"Bearer {token}"},
            json=payload_data,
        )

    try:
        response_payload = response.json()
    except ValueError:
        response_payload = {
            "status_code": response.status_code,
            "content": response.text,
        }

    # If response is not successful, raise an error with context-aware messages
    if response.status_code >= 400:
        logger.error(
            f"Error from {service['name']}",
            status_code=response.status_code,
            response_text=response.text,
        )

        # Extract error message and format it with service name and status code
        if isinstance(response_payload, dict):
            cdd_error_msg = response_payload.get("error", json.dumps(response_payload))
        else:
            cdd_error_msg = response.text

        # Build context-specific error messages based on status code
        status_messages = {
            status.HTTP_401_UNAUTHORIZED: "Authentication failed. The token is invalid, expired, or missing.",
            status.HTTP_403_FORBIDDEN: "Authorization failed. You lack the required role to perform this action.",
            status.HTTP_424_FAILED_DEPENDENCY: "The service failed to communicate with a required dependency (OIDC provider, database, etc.).",
            status.HTTP_500_INTERNAL_SERVER_ERROR: "An unexpected error occurred in the service while processing the request.",
            status.HTTP_503_SERVICE_UNAVAILABLE: "The service is not ready. A core component may have failed during initialization.",
        }

        context_msg = status_messages.get(response.status_code, "")
        error_message = (
            f"{service['name']} returned error {response.status_code}: {cdd_error_msg}"
        )
        if context_msg:
            error_message = f"{error_message} — {context_msg}"

        raise HTTPException(
            status_code=response.status_code,
            detail=ErrorEnvelope(
                code=response.status_code,
                error=error_message,
            ).model_dump(),
        )

    return APResponseSuccessEnvelope(
        code=response.status_code,
        message=f"{service['name']} completed successfully",
        content=response_payload,
    )
