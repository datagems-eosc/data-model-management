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

# TODO: remove unused imports and functions
from ..tools.AP.parse_AP import (
    compare_node_properties,
    extract_from_AP,
    APRequest,
    group_datasets_by_components,
)
from ..tools.AP.update_AP import (
    update_dataset_archivedAt,
    add_sql_operators_to_ap,
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
IDD_URL = os.getenv("IDD_URL", "https://datagems-dev.scayle.es/in-dataset-discovery")
MOMA_REQUEST_TIMEOUT_SECONDS = 300.0
CDD_REQUEST_TIMEOUT_SECONDS = 30.0
IDD_TIMEOUT_SECONDS = 30.0


EXTERNAL_SERVICES = {
    "/cross-dataset-discovery/search": {
        "url": f"{CDD_URL}/search-ap/",
        "name": "Cross-Dataset Discovery",
    },
    "/in-dataset-discovery/text2sql": {
        "url": f"{IDD_URL}/text2sql4ap",
        "name": "In-Dataset Discovery (text2sql)",
    },
}
CDD_EXCHANGE_SCOPE = os.getenv("CDD_EXCHANGE_SCOPE", "cross-dataset-discovery-api")


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
    logger.info(
        "Requesting dataset metadata from MoMa",
        dataset_id=dataset_id,
        dataset_status=dataset_status,
        timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
    )

    should_close = client is None
    if client is None:
        client = httpx.AsyncClient(
            timeout=MOMA_REQUEST_TIMEOUT_SECONDS,
            follow_redirects=True,
        )

    try:
        response = await client.get(
            url,
            params=params,
            headers={"Authorization": f"Bearer {token}"}
        )
        logger.info(
            "Received dataset metadata response from MoMa",
            dataset_id=dataset_id,
            status_code=response.status_code,
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
        logger.error(
            "MoMa API HTTP error while fetching dataset metadata",
            dataset_id=dataset_id,
            status_code=e.response.status_code,
            response_text=e.response.text,
        )
        raise
    except httpx.RequestError as e:
        logger.error(
            "MoMa API request error while fetching dataset metadata",
            dataset_id=dataset_id,
            error_type=type(e).__name__,
            error=str(e),
            timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
        )
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
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope),
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
        None, description="Minimum published date (YYYY-MM-DD).", alias="publishedFrom", format="YYYY-MM-DD",
    ),
    publishedDateTo: Optional[date] = Query(
        None, description="Maximum published date (YYYY-MM-DD).", alias="publishedTo", format="YYYY-MM-DD",
    ),
    direction: int = Query(
        1,
        description="Direction for sorting: 1 for ascending, -1 for descending.",
        ge=-1,
        le=1,
    ),
    dataset_status: Optional[str] = Query(
        None,
        description="Optional dataset status to filter on.",
        alias="status"
    ),
    offset: Optional[int] = Query(
        None, description="Number of results to skip for pagination.", ge=0
    ),
    count: Optional[int] = Query(
        None, description="Maximum number of results to return.", ge=1
    ),
    mimeTypes: Optional[List[MimeType]] = Query(
        None,
        description="Filter datasets by file MIME types.",
    ),
):
    """
    Search datasets using MoMa API.
    """
    url = f"{MOMA_URL}/datasets/"

    # Build params as list of tuples to correctly handle repeated keys (e.g. nodeIds)
    params: list[tuple[str, Any]] = []

    if nodeIds:
        params += [("nodeIds", nid) for nid in nodeIds]

    if properties:
        params += [("properties", p.value) for p in properties]

    if types:
        params += [("types", t.value) for t in types]

    if orderBy:
        params += [("orderBy", o.value) for o in orderBy]

    if publishedDateFrom:
        params.append(("publishedFrom", publishedDateFrom.strftime("%Y-%m-%d")))

    if publishedDateTo:
        params.append(("publishedTo", publishedDateTo.strftime("%Y-%m-%d")))

    if dataset_status is not None:
        params.append(("status", dataset_status))

    # Convert offset/count to page/pageSize for MoMa (max pageSize is 100)
    page_size = min(count, 100) if count is not None else 25
    params.append(("pageSize", page_size))

    if offset is not None:
        params.append(("page", (offset // page_size) + 1))
    else:
        params.append(("page", 1))

    # Direction: MoMa expects 'asc' or 'desc'
    params.append(("direction", "asc" if direction >= 0 else "desc"))

    if mimeTypes:
        params += [("mimeTypes", m.value) for m in mimeTypes]

    logger.info(
        "Searching datasets in MoMa",
        node_ids_count=len(nodeIds) if nodeIds else 0,
        page_size=page_size,
        offset=offset,
        dataset_status=dataset_status,
        timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
    )

    async with httpx.AsyncClient(
        timeout=MOMA_REQUEST_TIMEOUT_SECONDS,
        follow_redirects=True,
    ) as client:
        try:
            response = await client.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {token}"}
            )
            response.raise_for_status()

            data = response.json()

            datasets = data.get("datasets", [])
            response_page = data.get("page", 1)
            response_page_size = data.get("pageSize", page_size)
            response_total = data.get("total", len(datasets))
            response_offset = (response_page - 1) * response_page_size
            logger.info(
                "Dataset search completed",
                status_code=response.status_code,
                returned_count=len(datasets),
                total=response_total,
                page=response_page,
                page_size=response_page_size,
            )

            return DatasetsSuccessEnvelope(
                code=status.HTTP_200_OK,
                message=(
                    "No datasets found matching the search criteria"
                    if not datasets
                    else f"{len(datasets)} datasets retrieved successfully"
                ),
                datasets=datasets,
                offset=response_offset,
                count=len(datasets),
                total=response_total,
            )
        
        except httpx.HTTPStatusError as e:
            logger.error(
                "MoMa API HTTP error during dataset search",
                status_code=e.response.status_code,
                response_text=e.response.text,
            )

            if e.response.status_code == status.HTTP_404_NOT_FOUND:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=ErrorEnvelope(
                        code=status.HTTP_404_NOT_FOUND,
                        error="Dataset search target not found in MoMa",
                        details={
                            "moma_status_code": e.response.status_code,
                            "moma_response": e.response.text,
                        },
                    ).model_dump(),
                )

            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Error from MoMa API: {e.response.status_code}",
                    details={
                        "moma_status_code": e.response.status_code,
                        "moma_response": e.response.text,
                    },
                ).model_dump(),
            )
        except httpx.RequestError as e:
            logger.error(
                "MoMa API request error during dataset search",
                error_type=type(e).__name__,
                error=str(e),
                timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error="Failed to connect to MoMa API",
                    details={
                        "request_error_type": type(e).__name__,
                        "request_error": str(e),
                    },
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
    logger.info(
        "Fetching dataset from MoMa",
        dataset_id=dataset_id,
        output_format=format,
        timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
    )
    async with httpx.AsyncClient(
        timeout=MOMA_REQUEST_TIMEOUT_SECONDS,
        follow_redirects=True,
    ) as client:
        try:
            response = await client.get(
                f"{MOMA_URL}/datasets/{dataset_id}",
                headers={"Authorization": f"Bearer {token}"},
            )

            if response.status_code == status.HTTP_404_NOT_FOUND:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=ErrorEnvelope(
                        code=status.HTTP_404_NOT_FOUND,
                        error=f"Dataset with ID {dataset_id} not found in Neo4j",
                    ).model_dump(),
                )

            response.raise_for_status()
            metadata = response.json()
            logger.info(
                "Dataset fetch completed",
                dataset_id=dataset_id,
                status_code=response.status_code,
                nodes_count=len(metadata.get("nodes", [])) if isinstance(metadata, dict) else None,
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
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Error from MoMa API: {e.response.status_code}",
                    details={
                        "dataset_id": dataset_id,
                        "moma_status_code": e.response.status_code,
                        "moma_response": e.response.text,
                    },
                ).model_dump(),
            )
        except httpx.RequestError as e:
            logger.error(
                "MoMa API request error during dataset fetch",
                dataset_id=dataset_id,
                timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
                exc_info=True,
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error="Failed to connect to MoMa API",
                    details={
                        "dataset_id": dataset_id,
                        "request_error_type": type(e).__name__,
                        "request_error": str(e),
                    },
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
    logger.info(
        "Register dataset request received",
        timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
    )
    
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
        logger.info("Parsed dataset registration payload", dataset_id=dataset_id)

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

    async with httpx.AsyncClient(
        timeout=MOMA_REQUEST_TIMEOUT_SECONDS,
        follow_redirects=True,
    ) as client:
        # Check if dataset already exists
        try:
            exists, _ = await get_dataset_metadata(dataset_id, token=token, client=client)

            if exists:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=ErrorEnvelope(
                        code=status.HTTP_409_CONFLICT,
                        error=f"Dataset with ID {dataset_id} already exists in Neo4j",
                    ).model_dump(),
                )
        except HTTPException:
            raise
        except httpx.HTTPStatusError as e:
            logger.error(
                "MoMa API HTTP error while checking dataset existence during register",
                dataset_id=dataset_id,
                status_code=e.response.status_code,
                response_text=e.response.text,
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error="MoMa API error while checking dataset existence",
                    details={
                        "dataset_id": dataset_id,
                        "moma_status_code": e.response.status_code,
                        "moma_response": e.response.text,
                    },
                ).model_dump(),
            )
        except httpx.RequestError as e:
            logger.error(
                "MoMa API request error while checking dataset existence during register",
                dataset_id=dataset_id,
                error_type=type(e).__name__,
                error=str(e),
                timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error="Failed to connect to MoMa API while checking dataset existence",
                    details={
                        "dataset_id": dataset_id,
                        "request_error_type": type(e).__name__,
                        "request_error": str(e),
                    },
                ).model_dump(),
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error=f"Unexpected error while checking dataset existence: {type(e).__name__}: {str(e)}",
                ).model_dump(),
            )

        # Create the dataset node via POST /datasets
        try:
            post_url = f"{MOMA_URL}/datasets/"
            post_data = {"nodes": filtered_nodes, "edges": filtered_edges}
            logger.info(
                "Creating dataset in MoMa",
                dataset_id=dataset_id,
                nodes_count=len(filtered_nodes),
                edges_count=len(filtered_edges),
            )

            response = await client.post(
                post_url,
                json=post_data,
                headers={"Authorization": f"Bearer {token}"}
            )

            response.raise_for_status()
            logger.info(
                "Dataset created in MoMa",
                dataset_id=dataset_id,
                status_code=response.status_code,
            )

            # Fake forward AP to AP Storage API
            logger.info("AP be sent to AP Storage API")

            return APSuccessEnvelope(
                code=status.HTTP_201_CREATED,
                message=f"Dataset with ID {dataset_id} registered successfully in Neo4j",
                ap=ap_payload.model_dump(by_alias=True, exclude_defaults=True),
            )

        except HTTPException:
            raise
        except httpx.HTTPStatusError as e:
            logger.error(
                "MoMa API HTTP error during dataset register",
                dataset_id=dataset_id,
                status_code=e.response.status_code,
                response_text=e.response.text,
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Error from MoMa API (Status: {e.response.status_code})",
                    details={
                        "dataset_id": dataset_id,
                        "moma_status_code": e.response.status_code,
                        "moma_response": e.response.text,
                    },
                ).model_dump(),
            )
        except httpx.RequestError as e:
            logger.error(
                "MoMa API request error during dataset register",
                dataset_id=dataset_id,
                error_type=type(e).__name__,
                error=str(e),
                timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error="Failed to connect to MoMa API",
                    details={
                        "dataset_id": dataset_id,
                        "request_error_type": type(e).__name__,
                        "request_error": str(e),
                    },
                ).model_dump(),
            )
        except Exception as e:
            logger.error(
                "Unexpected error during dataset register",
                dataset_id=dataset_id,
                error_type=type(e).__name__,
                error=str(e),
                exc_info=True,
            )
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
    logger.info(
        "Load dataset request received",
        force=force,
        timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
    )

    try:
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
        logger.info(
            "Parsed dataset load payload",
            dataset_id=dataset_id,
            dataset_status=dataset_status,
            archived_at=dataset_path,
        )

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

    # Pre-check: Verify dataset exists in Neo4j with the provided status (fallback to 'staged')
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
        logger.info(
            "Dataset existence check completed",
            dataset_id=dataset_id,
            effective_status=effective_status,
            exists=exists,
        )

        if not exists:
            msg = (
                f"Dataset with ID {dataset_id} does not exist in Neo4j"
                + (f" with status '{effective_status}'." if effective_status else ".")
                + " Please register the dataset first using /dataset/register endpoint."
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
    except httpx.HTTPStatusError as e:
        logger.error(
            "MoMa API HTTP error while checking dataset existence during load",
            dataset_id=dataset_id,
            status_code=e.response.status_code,
            response_text=e.response.text,
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=ErrorEnvelope(
                code=status.HTTP_502_BAD_GATEWAY,
                error="MoMa API error while verifying dataset existence",
                details={
                    "dataset_id": dataset_id,
                    "moma_status_code": e.response.status_code,
                    "moma_response": e.response.text,
                },
            ).model_dump(),
        )
    except httpx.RequestError as e:
        logger.error(
            "MoMa API request error while checking dataset existence during load",
            dataset_id=dataset_id,
            error_type=type(e).__name__,
            error=str(e),
            timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=ErrorEnvelope(
                code=status.HTTP_503_SERVICE_UNAVAILABLE,
                error="Failed to connect to MoMa API while verifying dataset existence",
                details={
                    "dataset_id": dataset_id,
                    "request_error_type": type(e).__name__,
                    "request_error": str(e),
                },
            ).model_dump(),
        )
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
    async with httpx.AsyncClient(
        timeout=MOMA_REQUEST_TIMEOUT_SECONDS,
        follow_redirects=True,
    ) as client:
        try:
            logger.info(
                "Updating dataset node in MoMa",
                dataset_id=dataset_id,
                new_archived_at=new_path,
                new_status=DatasetState.Loaded.value,
            )
            response = await client.patch(
                f"{MOMA_URL}/nodes/{dataset_id}",
                json={
                    "archivedAt": new_path,
                    "status": DatasetState.Loaded.value,
                },
                headers={"Authorization": f"Bearer {token}"},
            )
            response.raise_for_status()
            logger.info(
                "Dataset node updated in MoMa",
                dataset_id=dataset_id,
                status_code=response.status_code,
            )

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

            logger.error(
                "MoMa API HTTP error during dataset load",
                dataset_id=dataset_id,
                status_code=e.response.status_code,
                response_text=e.response.text,
                rollback_failed=rollback_error is not None,
            )

            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Dataset load failed during Neo4j update (file rolled back to {dataset_path}): {error_msg}",
                    details={
                        "dataset_id": dataset_id,
                        "moma_status_code": e.response.status_code,
                        "moma_response": e.response.text,
                        "rollback_failed": rollback_error is not None,
                    },
                ).model_dump(),
            )
        except httpx.RequestError as e:
            rollback_error = None
            try:
                if target_path.exists():
                    shutil.move(str(target_path), str(source_path))
            except Exception as rollback_exc:
                rollback_error = rollback_exc

            logger.error(
                "MoMa API request error during dataset load",
                dataset_id=dataset_id,
                error_type=type(e).__name__,
                error=str(e),
                timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
                rollback_failed=rollback_error is not None,
            )

            error_msg = "Failed to connect to MoMa API"
            if rollback_error:
                error_msg += f" [ROLLBACK FAILED: {type(rollback_error).__name__}: {str(rollback_error)}. File may be orphaned at {target_path}]"

            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error=f"Dataset load failed during Neo4j update (file rolled back to {dataset_path}): {error_msg}",
                    details={
                        "dataset_id": dataset_id,
                        "request_error_type": type(e).__name__,
                        "request_error": str(e),
                        "rollback_failed": rollback_error is not None,
                    },
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

            logger.error(
                "Unexpected error during dataset load MoMa update",
                dataset_id=dataset_id,
                error_type=type(e).__name__,
                error=str(e),
                rollback_failed=rollback_error is not None,
                exc_info=True,
            )

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
    1. Extracting Dataset nodes from AP
    2. Verifying each Dataset node exists (lightweight check)
    3. Sending everything to MoMa via POST /datasets/ (MoMa handles upsert)
    """
    ap_payload = wrapped.ap
    logger.info(
        "Update dataset request received",
        timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
    )
    
    try:
        # Extract all nodes and edges
        filtered_nodes, filtered_edges = extract_from_AP(ap_payload)

        if not filtered_nodes:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="No Dataset/FileObject/RecordSet nodes found in AP",
                ).model_dump(),
            )

        # Get Dataset node IDs for existence check
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

        logger.info(
            "Parsed dataset update payload",
            dataset_ids=dataset_ids,
            nodes_count=len(filtered_nodes),
            edges_count=len(filtered_edges),
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

    async with httpx.AsyncClient(
        timeout=MOMA_REQUEST_TIMEOUT_SECONDS,
        follow_redirects=True,
    ) as client:
        # Step 1: Check if each Dataset exists (lightweight search)
        try:
            for dataset_id in dataset_ids:
                search_url = f"{MOMA_URL}/datasets/"
                search_params = {
                    "nodeIds": dataset_id,
                    "pageSize": 1,
                    "properties": "id"
                }
                
                response = await client.get(
                    search_url,
                    params=search_params,
                    headers={"Authorization": f"Bearer {token}"}
                )
                response.raise_for_status()
                
                data = response.json()
                datasets = data.get("datasets", [])
                exists = len(datasets) > 0
                
                logger.info(
                    "Verified dataset exists before update",
                    dataset_id=dataset_id,
                    exists=exists,
                )
                
                if not exists:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=ErrorEnvelope(
                            code=status.HTTP_404_NOT_FOUND,
                            error=(
                                f"Dataset with ID {dataset_id} not found in Neo4j. "
                                "Please register it first using /dataset/register."
                            ),
                        ).model_dump(),
                    )
                    
        except HTTPException:
            raise
        except httpx.HTTPStatusError as e:
            logger.error(
                "MoMa API HTTP error while verifying dataset existence during update",
                dataset_ids=dataset_ids,
                status_code=e.response.status_code,
                response_text=e.response.text,
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error="MoMa API error while verifying dataset existence",
                    details={
                        "dataset_ids": dataset_ids,
                        "moma_status_code": e.response.status_code,
                        "moma_response": e.response.text,
                    },
                ).model_dump(),
            )
        except httpx.RequestError as e:
            logger.error(
                "MoMa API request error while verifying dataset existence during update",
                dataset_ids=dataset_ids,
                error_type=type(e).__name__,
                error=str(e),
                timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error="Failed to connect to MoMa API while verifying dataset existence",
                    details={
                        "dataset_ids": dataset_ids,
                        "request_error_type": type(e).__name__,
                        "request_error": str(e),
                    },
                ).model_dump(),
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error=f"Failed to verify dataset existence: {type(e).__name__}: {str(e)}",
                ).model_dump(),
            )
        
        # Step 2: Ensure every node has a properties field (required by MoMa)
        for node in filtered_nodes:
            if "properties" not in node:
                node["properties"] = {}

        # Step 3: Inject 'ready' status if RecordSet is present
        has_record_set = any(
            "cr:RecordSet" in node.get("labels", []) for node in filtered_nodes
        )
        
        if has_record_set:
            for node in filtered_nodes:
                if "sc:Dataset" in node.get("labels", []):
                    node.setdefault("properties", {})["status"] = DatasetState.Ready.value

        # Step 4: Send everything to MoMa (upsert)
        try:
            logger.info(
                "Upserting datasets in MoMa",
                dataset_ids=dataset_ids,
                nodes_count=len(filtered_nodes),
                edges_count=len(filtered_edges),
            )
            
            response = await client.post(
                f"{MOMA_URL}/datasets/",
                json={"nodes": filtered_nodes, "edges": filtered_edges},
                headers={"Authorization": f"Bearer {token}"},
            )
            response.raise_for_status()
            
            logger.info(
                "Datasets upsert completed in MoMa",
                status_code=response.status_code,
                dataset_ids=dataset_ids,
            )

        except HTTPException:
            raise
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to upsert dataset",
                dataset_ids=dataset_ids,
                status_code=e.response.status_code,
                response_body=e.response.text,
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Failed to upsert dataset: HTTP {e.response.status_code}",
                    details={
                        "dataset_ids": dataset_ids,
                        "moma_status_code": e.response.status_code,
                        "moma_response": e.response.text,
                    },
                ).model_dump(),
            )
        except httpx.RequestError as e:
            logger.error(
                "MoMa API request error during dataset update",
                dataset_ids=dataset_ids,
                error_type=type(e).__name__,
                error=str(e),
                timeout_seconds=MOMA_REQUEST_TIMEOUT_SECONDS,
            )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    error="Failed to connect to MoMa API",
                    details={
                        "dataset_ids": dataset_ids,
                        "request_error_type": type(e).__name__,
                        "request_error": str(e),
                    },
                ).model_dump(),
            )
        except Exception as e:
            logger.error(
                "Unexpected error during dataset update",
                dataset_ids=dataset_ids,
                error_type=type(e).__name__,
                error=str(e),
                exc_info=True,
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error=f"Unexpected error during upsert: {type(e).__name__}: {str(e)}",
                ).model_dump(),
            )

    return APSuccessEnvelope(
        code=status.HTTP_200_OK,
        message="Dataset update completed",
        ap=ap_payload.model_dump(by_alias=True, exclude_defaults=True),
    )
    

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




def execute_query_csv(query, software):
    software = software.lower()
    DATASET_DIR = os.getenv("DATASET_DIR", "/s3/dataset")
    try:
        # Query with DuckDB
        if software == "duckdb":
            s3_query = query
            # Match S3 paths with or without quotes
            s3_paths = re.findall(r"'?s3://dataset/[^\s,;'\"]+(?:')?", s3_query)
            for s3_path in s3_paths:
                # Remove quotes if present
                cleaned_path = s3_path.replace("'", "")
                local_folder = cleaned_path.replace("s3://dataset/", f"{DATASET_DIR}/")
                replacement = f"read_csv_auto('{local_folder}')"
                s3_query = s3_query.replace(s3_path, replacement)
            con = duckdb.connect(database=":memory:")
            result_df = con.execute(s3_query).fetchdf()
            con.close()
            return result_df
        else:
            raise Exception(f"Unsupported software: {software}")
    except Exception as e:
        raise Exception(f"Query execution failed: {str(e)}")


def execute_query_postgres(query, duckdb_connection):
    """Execute query on PostgreSQL via DuckDB"""
    try:
        # Install and load postgres extension
        duckdb_connection.sql("INSTALL postgres;")
        duckdb_connection.sql("LOAD postgres;")

        # Attach PostgreSQL database
        db_host = os.getenv("DATAGEMS_POSTGRES_HOST")
        db_port = os.getenv("DATAGEMS_POSTGRES_PORT")
        db_user = os.getenv("DS_READER_USER")
        db_password = os.getenv("DS_READER_PS")
        db_name = query["db_name"]

        # Check if required environment variables are set
        missing_vars = []
        if not db_host:
            missing_vars.append("DATAGEMS_POSTGRES_HOST")
        if not db_port:
            missing_vars.append("DATAGEMS_POSTGRES_PORT")
        if not db_user:
            missing_vars.append("DS_READER_USER")
        if not db_password:
            missing_vars.append("DS_READER_PS")

        if missing_vars:
            raise ValueError(
                f"Missing PostgreSQL environment variables: {', '.join(missing_vars)}. "
                f"Please ensure these are set in your docker run command or environment."
            )

        connection_string = (
            f"dbname={db_name} user={db_user} password={db_password} "
            f"host={db_host} port={db_port}"
        )

        duckdb_connection.sql(f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);")
        result_df = duckdb_connection.execute(query.get("query")).fetchdf()

        return result_df

    except Exception as e:
        raise Exception(
            f"PostgreSQL connection failed: {str(e)}. "
            f"Check that: 1) Host is reachable (host={os.getenv('DATAGEMS_POSTGRES_HOST')}), "
            f"2) Port is correct (port={os.getenv('DATAGEMS_POSTGRES_PORT')}), "
            f"3) Credentials are valid (user={os.getenv('DS_READER_USER')}), "
            f"4) Database exists (db={query.get('db_name')})"
        )


def execute_query_csv_postgres(query, software, args_sources=None, db_name=None):
    """Execute query with mixed CSV and PostgreSQL sources via DuckDB"""
    software = software.lower()
    DATASET_DIR = os.getenv("DATASET_DIR", "/s3/dataset")
    if args_sources is None:
        args_sources = {}
    try:
        if software == "duckdb":
            # Create DuckDB connection
            con = duckdb.connect(database=":memory:")

            # If any args are from postgres, attach the database
            if any(source == "text/sql" for source in args_sources.values()):
                con.sql("INSTALL postgres;")
                con.sql("LOAD postgres;")

                db_host = os.getenv("DATAGEMS_POSTGRES_HOST")
                db_port = os.getenv("DATAGEMS_POSTGRES_PORT")
                db_user = os.getenv("DS_READER_USER")
                db_password = os.getenv("DS_READER_PS")
                db_name = query["db_name"]

                # Check if required environment variables are set
                missing_vars = []
                if not db_host:
                    missing_vars.append("DATAGEMS_POSTGRES_HOST")
                if not db_port:
                    missing_vars.append("DATAGEMS_POSTGRES_PORT")
                if not db_user:
                    missing_vars.append("DS_READER_USER")
                if not db_password:
                    missing_vars.append("DS_READER_PS")

                if missing_vars:
                    raise ValueError(
                        f"Missing PostgreSQL environment variables: {', '.join(missing_vars)}"
                    )

                connection_string = (
                    f"dbname={db_name} user={db_user} password={db_password} "
                    f"host={db_host} port={db_port}"
                )
                con.sql(f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);")

            # Handle CSV paths in query (convert S3 paths to local paths)
            processed_query = query.get("query", "")
            s3_paths = re.findall(r"'?s3://dataset/[^\s,;'\"]+(?:')?", processed_query)

            for s3_path in s3_paths:
                cleaned_path = s3_path.replace("'", "")
                local_folder = cleaned_path.replace("s3://dataset/", f"{DATASET_DIR}/")
                replacement = f"read_csv_auto('{local_folder}')"
                processed_query = processed_query.replace(s3_path, replacement)

            # Execute the mixed query
            result_df = con.execute(processed_query).fetchdf()
            con.close()

            return result_df

        else:
            raise Exception(f"Unsupported software: {software}")

    except Exception as e:
        raise Exception(f"Mixed query execution failed: {str(e)}")


def query_rewriting(
    query: str, args_map: Dict[str, Any], args_sources: Dict[str, str] = None
) -> str:
    """
    Rewrite query placeholders with actual values.

    For PostgreSQL tables: {{arg1}} -> table_name (without quotes)
    For CSV files: {{arg1}} -> 's3://dataset/path/file.csv' (with quotes)
    """
    if args_sources is None:
        args_sources = {}

    rewritten_query = query

    for arg_name, arg_value in args_map.items():
        if isinstance(arg_value, str) and arg_value:
            source_type = args_sources.get(arg_name, "")
            # Replace both {{arg_name}} and {arg_name} patterns
            if source_type == "text/sql":  # PostgreSQL table reference
                replacement = arg_value  # No quotes, no s3://dataset/ prefix
            elif source_type == "text/csv":  # CSV file path
                replacement = f"'{arg_value}'"  # With quotes for CSV paths
            elif source_type == "text/json" :
                replacement = f"'{arg_value}'"  # With quotes for JSON paths
            else:
                replacement = f"'{arg_value}'"  # Default with quotes

            # Replace {{arg_name}} pattern
            rewritten_query = re.sub(
                r"\{\{\s*" + re.escape(arg_name) + r"\s*\}\}",
                replacement,
                rewritten_query,
            )
            # Replace {arg_name} pattern (alternative syntax)
            rewritten_query = re.sub(
                r"\{\s*" + re.escape(arg_name) + r"\s*\}",
                replacement,
                rewritten_query,
            )

    return rewritten_query


async def extract_query_from_AP(
    ap_payload: APRequest,
    token: str,
    expected_ap_process: Optional[str] = None,
    expected_operator_command: Optional[str] = None,
) -> Dict[str, Any]:
    try:
        G = json_to_graph(ap_payload)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse the Analytical Pattern: {str(e)}",
        )

    (
        AP_nodes,
        operator_nodes,
        dataset_nodes,
        file_object_nodes,
        user_nodes,
        db_connection_nodes,
    ) = (
        [],
        [],
        [],
        [],
        [],
        [],
    )

    for node_id, attributes in G.nodes(data=True):
        labels = attributes.get("labels", [])
        if "Analytical_Pattern" in labels:
            AP_nodes.append(node_id)
        elif "SQL_Operator" in labels:
            operator_nodes.append(node_id)
        elif "sc:Dataset" in labels:
            dataset_nodes.append(node_id)
        elif "User" in labels:
            user_nodes.append(node_id)
        elif "cr:FileObject" in labels:
            file_object_nodes.append(node_id)
        elif "dg:DatabaseConnection" in labels:
            db_connection_nodes.append(node_id)

    if len(AP_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'Analytical_Pattern' node.",
        )

    if len(operator_nodes) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain at least one 'SQL_Operator' node.",
        )

    if len(dataset_nodes) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain at least one 'Dataset' node.",
        )

    if len(user_nodes) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain exactly one 'User' node.",
        )
    if len(db_connection_nodes) > 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The Analytical Pattern must contain at most one 'DatabaseConnection' node, because only one database can be queried at a time.",
        )

    # Get properties of datasets and database connections from MoMa and add them to the graph, as they are needed to execute the query

    for node_id in db_connection_nodes:
        dbc_properties = await get_node_properties(node_id, token=token)
        current_properties = G.nodes[node_id].get("properties", {})
        current_properties.update(dbc_properties)
        G.nodes[node_id].update({"properties": current_properties})

    AP_id = AP_nodes[0]
    AP_properties = G.nodes[AP_id].get("properties", {})
    AP_process = AP_properties.get("Process")

    if expected_ap_process and AP_process != expected_ap_process:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Expected Analytical Pattern 'Process'='{expected_ap_process}', but found '{AP_process}'. AP node ID: '{AP_id}', properties: {AP_properties}",
        )

    query_info: Dict[str, Any] = {}
    query_info["software"] = (
        G.nodes[operator_nodes[0]]
        .get("properties", {})
        .get("name", "Unknown Software")
        .split(" ")[0]
    )
    query_info["query"] = (
        G.nodes[operator_nodes[0]].get("properties", {}).get("query", "No query found")
    )

    if query_info["software"] not in ["DuckDB", "Ontop"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported software: {query_info['software']}. Supported software are 'DuckDB' and 'Ontop'.",
        )

    args_sources = {}
    args_map = {
        data.get("properties", {}).get("argname"): u
        for u, v, data in G.edges(data=True)
        if "argname" in data.get("properties", {})
    }

    for argname in args_map.keys():
        node_id = args_map[argname]
        file_object_properties = await get_node_properties(node_id, token=token)

        G.nodes[node_id].update({"properties": file_object_properties})
        logger.info(f"Arg '{argname}' is updated)")

    for argname, node_id in args_map.items():
        args_sources[argname] = (
            G.nodes[node_id].get("properties", {}).get("encodingFormat", "")
        )

    # Store args_sources in query_info for later use
    query_info["args_sources"] = args_sources

    # Map source types to connection types
    db_types = set()
    for source in args_sources.values():
        if source == "text/sql":
            db_types.add("postgres")
        elif source == "text/csv":
            db_types.add("csv")

    query_info["db_connection"] = (
        "mixed"
        if len(db_types) > 1
        else next(iter(db_types))
        if db_types
        else "unknown"
    )
    # Extract database name from DatabaseConnection node
    db_name = None
    if db_connection_nodes:
        db_properties = G.nodes[db_connection_nodes[0]].get("properties", {})
        db_name = db_properties.get("name", "ds_era5_land")
        query_info["db_name"] = db_name

    for argname, source in args_sources.items():
        if source == "text/sql":
            args_map[argname] = "pg_db.public." + G.nodes[args_map[argname]].get(
                "properties", {}
            ).get("name", "")
        elif source == "text/csv":
            node_id = args_map[argname]
            args_map[argname] = (
                G.nodes[args_map[argname]].get("properties", {}).get("contentUrl", "")
            )
            # ## If the contentUrl as been generated locally, we miss the dataset_id, so we need to get it from the distribution edge
            # if re.match(r"^s3:/?[^/]+\.(csv|json)$", args_map[argname]):
            #     dataset_id = next(
            #         (
            #             from_node
            #             for from_node, to_node, edge_data in G.in_edges(
            #                 node_id, data=True
            #             )
            #             if "distribution" in edge_data.get("labels", [])
            #         ),
            #         None,
            #     )
            #     # Strip the s3:/ or s3:// prefix from the filename, then rebuild with dataset_id
            #     filename = re.sub(r"^s3:/?/?", "", args_map[argname])
            #     args_map[argname] = f"s3://dataset/{dataset_id}/{filename}"

    query_info["query"] = query_rewriting(query_info["query"], args_map, args_sources)
    return query_info


# To be implemented
def execute_query_xml(csv_name, query, software, xml_path):
    return {"message": "XML query execution not implemented yet"}, 501


# Get the properties of a node from MoMa given its ID. This is needed to get the contentUrl of the datasets, which is required to execute the query
async def get_node_properties(node_id, token: Optional[str] = None) -> Dict[str, Any]:
    """Fetch node properties from MoMa2 API"""
    moma_api_url = os.getenv(
        "MOMA_API_URL", "https://datagems-dev.scayle.es/moma2/v1/api"
    )
    endpoint = f"{moma_api_url}/nodes/{node_id}"
    try:
        async with httpx.AsyncClient(
            timeout=MOMA_REQUEST_TIMEOUT_SECONDS, follow_redirects=True
        ) as client:
            response = await client.get(
                endpoint,
                headers={"Authorization": f"Bearer {token if token else 'NO_TOKEN'}"},
            )

        # Check if node was not found
        if response.status_code == 404:
            logger.error(
                f"Node with ID '{node_id}' not found in MoMa2. Response: {response.text}"
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The fileObject/databaseConnection have not been found in MoMa (node ID: '{node_id}'). Please make sure that the dataset is well onboarded.",
            )

        # Check for other HTTP errors
        if not response.is_success:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"MoMa2 API error {response.status_code} for node '{node_id}': {response.text}",
            )

        try:
            response_payload = response.json()
        except ValueError:
            response_payload = {
                "status_code": response.status_code,
                "content": response.text,
            }

        properties = response_payload.get("properties", {})
        return properties

    except httpx.ConnectError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Cannot reach MoMa2 API at {moma_api_url}. Check MOMA_API_URL environment variable.",
        )
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"MoMa2 API request timed out. Node: {node_id}",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching node properties from MoMa2: {str(e)}",
        )


@router.post(
    "/polyglot/query",
    response_model=APSuccessEnvelope,
    response_model_exclude_none=True,
)
async def polyglot_query(
    wrapped: WrappedAPRequest,
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope)
):
    """Execute a SQL query on a dataset based on an Analytical Pattern"""
    try:
        executed_ap, upload_path = await execute_query(wrapped, token=token)
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
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"MoMa2 API request timed out. Node: {node_id}",
        )


async def execute_query(wrapped: WrappedAPRequest, token: str):
    from ..resources.dataset import register_dataset

    ap_payload = wrapped.ap
    query_info = await extract_query_from_AP(ap_payload, token=token)
    software = query_info.get("software")
    query_filled = query_info.get("query")
    db_connection = query_info.get("db_connection", "Unknown DB Connection")

    if db_connection == "postgres":
        result = execute_query_postgres(
            query_info, duckdb.connect(database=":memory:")
        )
    elif db_connection == "csv":
        result = execute_query_csv(query_filled, software)
    elif db_connection == "mixed":
        result = execute_query_csv_postgres(
            query_info,
            software,
            args_sources=query_info.get("args_sources", {}),
            db_name=query_info.get("db_name"),
        )
    logger.info(f"Query executed successfully with software '{software}' and database connection type '{db_connection}'")
    ap_payload, dataset_id = generate_dataset_node(ap_payload)
    csv_bytes = result.to_csv(index=False).encode("utf-8")
    logger.info(f"Query results converted to CSV bytes, size: {len(csv_bytes)} bytes")
    upload_path, dataset_id = upload_csv_to_results(csv_bytes, dataset_id)
    logger.info(f"CSV results uploaded to results storage at path: {upload_path}")
    
    AP_query_after = update_AP_after_query(ap_payload, dataset_id, upload_path)
    logger.info(f"AP updated with new dataset ID and properties after query execution. Dataset ID: {dataset_id}")
    upload_ap_to_results(
        json.dumps(
            AP_query_after.model_dump(by_alias=True, exclude_defaults=True),
            ensure_ascii=False,
            indent=2,
        ),
        dataset_id,
    )
    logger.info(f"Updated AP uploaded to results storage for dataset ID: {dataset_id}")

    register_AP = generate_register_AP_after_query(AP_query_after)
    logger.info(f"Register AP generated for registering the new dataset in MoMa2. Register AP nodes: {len(register_AP.nodes)}, edges: {len(register_AP.edges)}")
    await register_dataset(WrappedAPRequest(ap=register_AP))

    # TODO: AP Storage

    return AP_query_after, upload_path

@router.post("/in-dataset-discovery/text2sql", response_model=APResponseSuccessEnvelope)
async def execute_and_store_idd(
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
    logger.info(
        f"Received AP for {service['name']}",
        ap=ap,
    )
    ap = add_sql_operators_to_ap(ap)
    logger.info(
        f"Updated AP",
        ap=ap,
    )
    payload_data["ap"] = ap.model_dump(by_alias=True, exclude_defaults=True)

    try:
        print(f"[{service['name']}] Storing AP in AP Storage:")
        print(json.dumps(ap, indent=2))
        print(f"[{service['name']}] AP stored successfully.")
    except Exception as e:
        print(f"[{service['name']}] AP Storage failed: {e}")

    async with httpx.AsyncClient(
        timeout=IDD_TIMEOUT_SECONDS, follow_redirects=True
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
            error_msg = response_payload.get("error", json.dumps(response_payload))
        else:
            error_msg = response.text

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
            f"{service['name']} returned error {response.status_code}: {error_msg}"
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
    else:
        ## Query execution
        try:
            execute_query_response = await execute_query(
                WrappedAPRequest(ap=APRequest.model_validate(response_payload.get("ap", {}))),
                token=token,
            )
            return execute_query_response

        except Exception as e:
            # Build a partial success envelope
            return APResponseSuccessEnvelope(
                code=status.HTTP_207_MULTI_STATUS,   # or 200 if you prefer
                message=f"AP stored successfully, but query execution failed: {str(e)}",
                content={
                    "ap": response_payload.get("ap", {}),
                    "query_error": str(e),
                },
            )


    return APResponseSuccessEnvelope(
        code=response.status_code,
        message=f"{service['name']} completed successfully",
        content=response_payload,
    )
