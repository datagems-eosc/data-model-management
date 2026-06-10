from datetime import date
from enum import Enum
import json
import os
import re
import time
from pathlib import Path
import shutil
from dmm_api.resources.converter import convertProfile
import requests
import sqlglot
from sqlglot.optimizer import optimize
from sqlglot import expressions as exp



from dmm_api.tools.AP.log_AP import (
    Grafeo_to_AP,
    Grafeo_to_AP_node,
    store_AP_in_grafeo,
    grafeo_begin,
    grafeo_execute,
    grafeo_commit,
    grafeo_rollback,
)
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
    json_to_graph,
)

from ..tools.AP.update_AP import (
    update_dataset_archivedAt,
    add_sql_operators_to_ap,
    generate_dataset_node,
    update_fileObject_id,
    update_fileObject_properties,
    update_AP_after_query, 
    update_endTime, 
    update_endTime_node,

)
from ..tools.AP.generate_AP import (
    generate_update_AP,
    generate_register_AP_after_query
) 

from ..tools.AP.generate_AP import generate_update_AP
from ..tools.S3.scratchpad import upload_dataset_to_scratchpad

from ..tools.S3.results import upload_csv_to_results, upload_ap_to_results, get_results_uuid
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

class APlogSuccessEnvelope(BaseModel):
    code: int
    message: str
    aplogs: List[Dict[str, Any]]
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
    application_xlsx = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    application_docx_ooxml = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )
    application_json = "application/json"
    application_jsonl = "application/jsonl"
    application_pptx_ooxml = "application/vnd.openxmlformats-officedocument.presentationml.presentation", 
    application_xml = "application/xml"
    image_jpeg = "image/jpeg"
    image_png = "image/png"
    image_gif = "image/gif"
    image_webp = "image/webp"
    image_bmp = "image/bmp"
    image_tiff = "image/tiff"
    text_csv = "text/csv"
    text_sql = "text/sql"
    text_html = "text/html"
    text_markdown = "text/markdown"
    text_plain = "text/plain"
    
class OPERATOR_LABELS(str, Enum):
    SQL_OPERATOR = "SQL_Operator"
    NLQ_OPERATOR = "NLQ_Operator"
    QUERY_OPERATOR = "Query_Operator"
    CDD_OPERATOR = "CDD_Operator"
    QUERY_DISAMBIGUATION_OPERATOR = "Query_Disambiguation_Operator"


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
QD_URL = os.getenv("QD_URL", "https://datagems-dev.scayle.es/query-disambiguation")
REC_SYS_URL = os.getenv("REC_SYS_URL", "https://datagems-dev.scayle.es/dataset-recsys")
MOMA_REQUEST_TIMEOUT_SECONDS = 300.0
REQUEST_TIMEOUT_SECONDS = 30.0
GRAFEO_URL = os.getenv("GRAFEO_URL", "http://localhost:7474")


EXTERNAL_SERVICES = {
    "/cross-dataset-discovery/search": {
        "url": f"{CDD_URL}/search-ap/",
        "name": "Cross-Dataset Discovery",
    },
    "/in-dataset-discovery/text2sql": {
        "url": f"{IDD_URL}/text2sql4ap",
        "name": "In-Dataset Discovery (text2sql)",
    },
    "/dataset-recsys/recommend": {
        "url": f"{REC_SYS_URL}/recommend/ap",
        "name": "Dataset Recommendation System",
    }, 
    "/query-disambiguation": {
        "url": f"{QD_URL}/query_disambiguation4ap",
        "name": "Query Disambiguation",
    }


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
            ).model_dump(exclude_none=True),
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
                    ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                    ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
            )
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error="Unexpected Internal Server error",
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
            )

        if len(filtered_nodes) != 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="Register AP must contain exactly one Dataset node.",
                ).model_dump(exclude_none=True),
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
            ).model_dump(exclude_none=True),
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
                    ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error=f"Unexpected error while checking dataset existence: {type(e).__name__}: {str(e)}",
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
            )

        if len(filtered_nodes) != 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error=f"Expected exactly 1 Dataset node, found {len(filtered_nodes)}",
                ).model_dump(exclude_none=True),
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
            ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
            ).model_dump(exclude_none=True),
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
            ).model_dump(exclude_none=True),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Failed to verify dataset existence in Neo4j: {type(e).__name__}: {str(e)}",
            ).model_dump(exclude_none=True),
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
            ).model_dump(exclude_none=True),
        )
    except FileExistsError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=ErrorEnvelope(
                code=status.HTTP_409_CONFLICT,
                error=f"{str(e)}",
            ).model_dump(exclude_none=True),
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorEnvelope(
                code=status.HTTP_400_BAD_REQUEST,
                error=f"Invalid input format: {str(e)}",
            ).model_dump(exclude_none=True),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Unexpected error during file move: {type(e).__name__}: {str(e)}",
            ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
            ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
            ).model_dump(exclude_none=True),
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
                        ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    error=f"Failed to verify dataset existence: {type(e).__name__}: {str(e)}",
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
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
            ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
            )
    elif body:
        # Use JSON body directly (automatic FastAPI parsing)
        payload_data = {"ap": body.ap.model_dump(exclude_none=True)}
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
            ).model_dump(exclude_none=True),
        )
    ap_obj = APRequest.model_validate(payload_data["ap"])
    payload_data["ap"] = ap_obj.model_dump(by_alias=True, exclude_defaults=True)
    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT_SECONDS, follow_redirects=True
    ) as client:
        response = await client.post(
            service["url"],
            headers={"Authorization": f"Bearer {token}"},
            json=payload_data,
        )

    try:
        response_payload = response.json()
        ap_obj = APRequest.model_validate(response_payload.get("ap", {}))
        ap_obj = update_endTime(ap_obj)
        response_payload["ap"] = ap_obj.model_dump(by_alias=True, exclude_defaults=True)
        ## AP storage in Grafeo
        try: 
            store_AP_in_grafeo(ap_obj)
        except Exception as e:
            logger.info(f"[{service['name']}] AP Storage failed: {e}")
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
            ).model_dump(exclude_none=True),
        )

    return APResponseSuccessEnvelope(
        code=response.status_code,
        message=f"{service['name']} completed successfully",
        content=response_payload,
    )

@router.post(
    "/query-disambiguation", response_model=APResponseSuccessEnvelope
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
            ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
            )
    elif body:
        # Use JSON body directly (automatic FastAPI parsing)
        payload_data = {"ap": body.ap.model_dump(exclude_none=True)}
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
            ).model_dump(exclude_none=True),
        )
    ap_obj = APRequest.model_validate(payload_data["ap"])
    
    payload_data["ap"] = ap_obj.model_dump(by_alias=True, exclude_defaults=True)
    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT_SECONDS, follow_redirects=True
    ) as client:
        response = await client.post(
            service["url"],
            headers={"Authorization": f"Bearer {token}"},
            json=payload_data,
        )

    try:
        response_payload = response.json()
        ap_obj = APRequest.model_validate(response_payload.get("ap", {}))
        ap_obj = update_endTime(ap_obj)
        response_payload["ap"] = ap_obj.model_dump(by_alias=True, exclude_defaults=True)
        ## AP storage in Grafeo
        try: 
            store_AP_in_grafeo(ap_obj)
        except Exception as e:
            logger.info(f"[{service['name']}] AP Storage failed: {e}")
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
            ).model_dump(exclude_none=True),
        )

    return APResponseSuccessEnvelope(
        code=response.status_code,
        message=f"{service['name']} completed successfully",
        content=response_payload,
    )
    
    

@router.post(
    "/dataset-recsys/recommend", response_model=APResponseSuccessEnvelope
)
async def execute_and_store_dataset_recommendations(
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
            ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
            )
    elif body:
        # Use JSON body directly (automatic FastAPI parsing)
        payload_data = {"ap": body.ap.model_dump(exclude_none=True)}
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
            ).model_dump(exclude_none=True),
        )
    
    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT_SECONDS, follow_redirects=True
    ) as client:
        response = await client.post(
            service["url"],
            headers={"Authorization": f"Bearer {token}"},
            json=payload_data,
        )

    try:
        response_payload = response.json()
        ## AP storage in Grafeo
        try: 
            store_AP_in_grafeo(APRequest.model_validate(response_payload.get("ap", {})))
        except Exception as e:
            print(f"[{service['name']}] AP Storage failed: {e}")
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
            ).model_dump(exclude_none=True),
        )

    return APResponseSuccessEnvelope(
        code=response.status_code,
        message=f"{service['name']} completed successfully",
        content=response_payload,
    )


def execute_query_postgres(query_builder):
    """Execute query on PostgreSQL via DuckDB"""
    duckdb_connection = duckdb.connect(database=":memory:")
    t0 = time.perf_counter()
    try:
        # Install and load postgres extension
        duckdb_connection.sql("INSTALL postgres;")
        duckdb_connection.sql("LOAD postgres;")

        db_name = None
        for argname, arg_info in query_builder.get("args_map", {}).items():
            if arg_info.get("mimeType") == "text/sql":
                db_name = arg_info.get("dbConnection", {}).get("name", "Unknown DB")

        # Attach PostgreSQL database
        db_host = os.getenv("DATAGEMS_POSTGRES_HOST")
        db_port = os.getenv("DATAGEMS_POSTGRES_PORT")
        db_user = os.getenv("DS_READER_USER")
        db_password = os.getenv("DS_READER_PS")
        

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
        logger.info(f"[TIMER] DuckDB connection: {time.perf_counter() - t0:.4f}s")

        query = query_builder.get("query", "")
        query = query_rewriting(query_builder)
        duckdb_connection.sql(f"ATTACH '{connection_string}' AS pg_db (TYPE postgres);")
        t1 = time.perf_counter()
        try: 
            result_df = duckdb_connection.execute(
                "SELECT * FROM postgres_query('pg_db', ?)",
                [query]
            ).fetchdf()  
        finally:
            duckdb_connection.close()      
        logger.info(f"[TIMER] Query execution: {time.perf_counter() - t1:.4f}s")
        return result_df

    except Exception as e:
        raise Exception(
            f"PostgreSQL connection failed: {str(e)}."
        )



def execute_query_mixed(query_builder):
    """Execute query with mixed CSV and PostgreSQL sources via DuckDB"""
    DATASET_DIR = os.getenv("DATASET_DIR", "/s3/dataset")
    
    try:
        con = duckdb.connect(database=":memory:")
        db_connections = []
        view_map = {}
        for argname, arg_info in query_builder.get("args_map", {}).items():
            if arg_info.get("mimeType") == "text/sql":
                content_url = arg_info.get("contentUrl", "")
                db_conn_props = arg_info.get("dbConnection") or {}
                db_connection = db_conn_props.get("name") or content_url.split(".")[0]
                if db_connection not in db_connections:
                    db_connections.append(db_connection)
                view_name = f"pg_{argname}"
                view_map[argname] = view_name
                arg_info["view_name"] = view_name  
            elif arg_info.get("mimeType") == "text/csv":
                
                view_name = f"csv_{argname}"
                view_map[argname] = view_name
                arg_info["view_name"] = view_name
                
            else:
                logger.error(f"Argument '{argname}' has unsupported mimeType: {arg_info.get('mimeType')}")

        processed_query = query_rewriting_views(query_builder["query"], view_map)
        args_map = extract_alias(processed_query, query_builder["args_map"])
        args_map, query_executable = write_views_minimal_extraction(processed_query, args_map)

        if db_connections:
            con.sql("INSTALL postgres;")
            con.sql("LOAD postgres;")
            con.sql("SET pg_experimental_filter_pushdown = true;")
            con.sql("SET pg_use_binary_copy = true;")
            con.sql("SET pg_use_ctid_scan = true;")
            db_host = os.getenv("DATAGEMS_POSTGRES_HOST")
            db_port = os.getenv("DATAGEMS_POSTGRES_PORT")
            db_user = os.getenv("DS_READER_USER")
            db_password = os.getenv("DS_READER_PS")
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
            for db_connection in db_connections:
                db_name = db_connection
                connection_string = (
                    f"dbname={db_name} user={db_user} password={db_password} "
                    f"host={db_host} port={db_port}"
                )
                con.sql(f"ATTACH '{connection_string}' AS {db_name} (TYPE postgres);")

        for argname, arg_info in query_builder.get("args_map", {}).items():
            view  = arg_info.get("view")
            if view:
                con.sql(view)

        try:
            result_df = con.execute(query_executable).fetchdf()
        finally:
            con.close()

        return result_df

    except Exception as e:
        raise Exception(f"{str(e)}")

def query_rewriting_views(query, view_map):
    rewritten_query = query
    for argname, view_name in view_map.items():
        rewritten_query = re.sub(
            r"\{\{\s*" + re.escape(argname) + r"\s*\}\}",
            view_name,
            rewritten_query,
        )
        rewritten_query = re.sub(
            r"\{\s*" + re.escape(argname) + r"\s*\}",
            view_name,
            rewritten_query,
        )
    return rewritten_query

def query_rewriting(
    query_builder: Dict[str, Any]
) -> str:
    """
    Rewrite query placeholders with actual values.
    For PostgreSQL tables: {{arg1}} -> table_name (without quotes)
    For CSV files: {{arg1}} -> 's3://dataset/path/file.csv' (with quotes)
    """

    rewritten_query = query_builder.get("query", "")
    for arg_name, arg_value in query_builder.get("args_map", {}).items():
        if arg_value.get("mimeType") == "text/sql":
            replacement = arg_value.get("contentUrl", "")  # Extract table name from contentUrl
            rewritten_query = re.sub(
                r"\{\{\s*" + re.escape(arg_name) + r"\s*\}\}",
                replacement,
                rewritten_query,
            )
            rewritten_query = re.sub(
                r"\{\s*" + re.escape(arg_name) + r"\s*\}",
                replacement,
                rewritten_query,
            )
        elif arg_value["mimeType"] == "text/csv":
        # For CSV sources, we expect the arg_value to be the S3 path
            replacement = "'" + arg_value.get("contentUrl", "") + "'"
            rewritten_query = re.sub(
                r"\{\{\s*" + re.escape(arg_name) + r"\s*\}\}",
                replacement,
                rewritten_query,
            )
            rewritten_query = re.sub(
                r"\{\s*" + re.escape(arg_name) + r"\s*\}",
                replacement,
                rewritten_query,
            )

    return rewritten_query


async def extract_query_from_AP(ap_payload, token
) -> Dict[str, Any]:
    """
    The query builder returns a dictionary containing:
    - query: the SQL query with placeholders (e.g. {{arg1}})
    - type: mixed or postgres
    - software: the software to execute the query (e.g. duckdb, ontop)
    - args_map: a mapping of argument names to their values and metadata, e.g.:
        {
            "arg1": {
                "mimeType": "",
                "contentUrl": "",
                "dbConnection": {
                    "host": "...",
                    "port": "...",
                    ...
                }
            },
            ...
    """
    try:
        if isinstance(ap_payload, APRequest):
            request = ap_payload
        else:
            data = json.loads(ap_payload)
            ap_data = data.get("ap", data)
            request = APRequest(**ap_data)
        G = json_to_graph(request)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse the Analytical Pattern: {str(e)}",
        )

    AP_nodes, operator_nodes, dataset_nodes, file_object_nodes, user_nodes, db_connection_nodes = [], [], [], [], [], []

    ## CHECKS about the AP 
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

    ## Get the query 
    query_builder = {}
    query_builder["query"] = G.nodes[operator_nodes[0]].get("properties", {}).get("query", "")

    ## Get the software 
    query_builder["software"] = G.nodes[operator_nodes[0]].get("properties", {}).get("name", "Unknown Software")

    ## Get the arg_map 
    args_map = {
        data.get("properties", {}).get("argname"): u
        for u, v, data in G.edges(data=True)
        if "argname" in data.get("properties", {})
    }
    mimeTypes = set()
    for argname in args_map.keys():
        node_id = args_map[argname]
        file_object_properties = await get_node_properties(node_id, token=token)
        mimeType = file_object_properties.get("encodingFormat", "")
        args_map[argname] = {
            "mimeType" : mimeType,
            "contentUrl": file_object_properties.get("contentUrl", ""), 
            "name" : file_object_properties.get("name", "")
        }
        mimeTypes.add(mimeType)
        db_connection_properties = None
        for u, v, data in G.edges(node_id, data=True):
            if "contained_in" in data.get("labels", []):
                db_connection_node_id = v
                db_connection_properties = await get_node_properties(db_connection_node_id, token=token)
        args_map[argname]["dbConnection"] = db_connection_properties
    query_builder["args_map"] = args_map
    if db_connection_nodes and len(db_connection_nodes) == 1:
        if any(m != "text/sql" for m in mimeTypes):
            query_builder["type"] = "mixed"
        else:
            query_builder["type"] = "postgres"
    else:
        query_builder["type"] = "mixed"
    return query_builder


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
        try: 
            store_AP_in_grafeo(executed_ap)
        except Exception as e:
            print(f"AP Storage failed: {e}")
        return APSuccessEnvelope(
            code=status.HTTP_200_OK,
            message=f"Query executed successfully, results stored at {upload_path}",
            ap=executed_ap.model_dump(by_alias=True, exclude_defaults=True),
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
    query_builder = await extract_query_from_AP(ap_payload, token=token)
    if query_builder["software"].split(" ")[0].lower() == "duckdb":
        if query_builder["type"] == "postgres":
            result = execute_query_postgres(query_builder)
        elif query_builder["type"] == "mixed":
            result = execute_query_mixed(query_builder)
        elif query_builder["type"] == "unknown":
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Could not determine the MIME type of one or more FileObjects in the AP. Please ensure all FileObjects have a valid encodingFormat.",
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unsupported query type '{query_builder['type']}'. Expected 'postgres' or 'mixed'.",
            )
    else:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unsupported software '{query_builder['software']}'. Only DuckDB-based operators are supported.",
        )
    
        
    ap_payload, dataset_id = generate_dataset_node(ap_payload)
    t2 = time.perf_counter()
    csv_bytes = result.to_csv(index=False).encode("utf-8")
    upload_path, dataset_id = upload_csv_to_results(csv_bytes, dataset_id)
    logger.info(f"[TIMER] Results printed in s3: {time.perf_counter() - t2:.4f}s")    
    AP_query_after = update_AP_after_query(ap_payload, dataset_id, upload_path)
    logger.info(f"AP updated with new dataset ID and properties after query execution. Dataset ID: {dataset_id}")
    t3 = time.perf_counter()
    upload_ap_to_results(
        json.dumps(
            AP_query_after.model_dump(by_alias=True, exclude_defaults=True),
            ensure_ascii=False,
            indent=2,
        ),
        dataset_id,
    )
    logger.info(f"[TIMER] AP uploaded: {time.perf_counter() - t3:.4f}s")
    logger.info(f"Updated AP uploaded to results storage for dataset ID: {dataset_id}")



    return AP_query_after, upload_path


@router.get("/polyglot/query/result/{dataset_id}")
async def get_query_result(
        dataset_id: str,
        token: str = Depends(security.oauth2_scheme),
        lines: Optional[int] = Query(None, alias="lines")
    ):
    """Endpoint to retrieve query results by dataset ID"""
    try:
        results = get_results_uuid(dataset_id, line=lines)
        return results
    except FileNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Result for dataset ID '{dataset_id}' not found.",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve query result: {str(e)}",
        )


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
            ).model_dump(exclude_none=True),
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
                ).model_dump(exclude_none=True),
            )
    elif body:
        # Use JSON body directly (automatic FastAPI parsing)
        payload_data = {"ap": body.ap.model_dump(exclude_none=True)}
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
            ).model_dump(exclude_none=True),
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
        store_AP_in_grafeo(ap)
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
            ).model_dump(exclude_none=True),
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


@router.get("/grafeo/test")
async def grafeo_test():
    return run_grafeo_query("RETURN 1 as ok")

def run_grafeo_query(query: str):
    url = "http://grafeo:7474/cypher"   # adjust to your endpoint
    payload = {"query": query}
    headers = {"Content-Type": "application/json"}
    resp = requests.post(url, json=payload, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    if "columns" in data and "rows" in data:
        return [dict(zip(data["columns"], row)) for row in data["rows"]]
    else:
        return data.get("rows", data)


def _run_grafeo_query_in_tx(txId, query: str):
    """Run a Cypher query inside an existing transaction and parse the columnar result."""
    data = grafeo_execute(txId, query)
    if "columns" in data and "rows" in data:
        return [dict(zip(data["columns"], row)) for row in data["rows"]]
    return data.get("rows", data)

@router.post("/grafeo/query")
async def grafeo_query(
    body: dict,
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope)
    ):
    query = body["query"]
    return run_grafeo_query(query)

@router.post("/aplog/store", 
             status_code=status.HTTP_201_CREATED,)
async def ap_storage(
    body: str | None = Form(None),
    file: UploadFile | None = File(None),
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope),
    
):
    parsed_request: Optional[WrappedAPRequest] = None

    # Ensure at least one input is provided
    if body is None and file is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorEnvelope(
                code=status.HTTP_400_BAD_REQUEST,
                error="You must provide either 'body' or 'file'.",
            ).model_dump(exclude_none=True),
        )

    if body is not None:
        try:
            ap_dict = json.loads(body)
            parsed_request = WrappedAPRequest.model_validate(ap_dict)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error=f"Invalid JSON: {e}",
                ).model_dump(exclude_none=True),
            )
    elif file is not None:
        content = await file.read()
        try:
            ap_dict = json.loads(content)
            parsed_request = WrappedAPRequest.model_validate(ap_dict)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error=f"Invalid file content: {e}",
                ).model_dump(exclude_none=True),
            )

    if parsed_request is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorEnvelope(
                code=status.HTTP_400_BAD_REQUEST,
                error="Unable to parse request payload.",
            ).model_dump(exclude_none=True),
        )

    payload_data = parsed_request.ap
    try:
        store_AP_in_grafeo(payload_data)
        return {
            "code": status.HTTP_201_CREATED,
            "message": "AP successfully stored in Grafeo",
        }
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorEnvelope(
                code=status.HTTP_400_BAD_REQUEST,
                error=str(e),
            ).model_dump(exclude_none=True),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorEnvelope(
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error=f"Failed to store AP in Grafeo: {str(e)}",
            ).model_dump(exclude_none=True),
        )

    
@router.get("/aplog/get/{ap_id}")
async def get_aplog(
    ap_id: str, 
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope)):

    ap_graph = get_full_aplog(ap_id, token=token)
    return APResponseSuccessEnvelope(
        code=200,
        message="success",
        content=ap_graph
    )

def get_full_aplog(ap_id: str, token, txId=None):
    gql_query = f"""
            MATCH (ap:Analytical_Pattern)
            WHERE ap.id = '{ap_id}'

            MATCH (u:User)-[req:request]->(t:Task)-[acc:is_accomplished]->(ap)

            WITH ap,
            COLLECT(DISTINCT u) AS users,
            COLLECT(DISTINCT t) AS tasks

            OPTIONAL MATCH (ap)-[:consist_of|distribution|input|output|follows|contained_in*0..5]-(n)
            WITH ap, users, tasks, COLLECT(DISTINCT n) AS downstream

            WITH ap, users + tasks + downstream + [ap] AS all_nodes_raw
            UNWIND all_nodes_raw AS n
            WITH ap, COLLECT(DISTINCT n) AS all_nodes

            OPTIONAL MATCH (a)-[r]-(b)
            WHERE a IN all_nodes AND b IN all_nodes

            RETURN ap, all_nodes, COLLECT(DISTINCT r) AS all_rels
    """
    if txId is None:
        txId = grafeo_begin()
    try:
        rows = _run_grafeo_query_in_tx(txId, gql_query)

        if not rows:
            raise HTTPException(status_code=404, detail=f"AP with id '{ap_id}' not found in Grafeo.")

        row = rows[0]
        node_ids = row["all_nodes"]
        rel_ids  = row["all_rels"]

        nodes_dict = fetch_nodes_by_ids(node_ids, txId=txId)
        edges_dict = fetch_rels_by_ids(rel_ids, txId=txId)

        grafeo_commit(txId)
        txId = None
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve AP log '{ap_id}' from Grafeo: {str(e)}",
        )
    finally:
        if txId is not None:
            grafeo_rollback(txId)

    ap_graph = Grafeo_to_AP(
        {"ap": {"nodes": nodes_dict, "edges": edges_dict}}
    )
    return ap_graph

@router.get("/aplog/search")
async def search_APs(
        userId:Optional[List[str]] = Query(None),
        startDate:Optional[List[str]] = Query(None),
        endDate:Optional[List[str]] = Query(None),
        operator:Optional[List[str]] = Query(None),
        datasetId:Optional[List[str]] = Query(None),
        fileObjectId:Optional[List[str]] = Query(None),
        limit: Optional[int] = Query(20),
        token: str = Depends(security.oauth2_scheme),
        token_payload: dict[str, Any] = Depends(security.require_app_scope),
    ):

    ## Check of the parameters added 
    if startDate and len(startDate) > 1:
        raise HTTPException(status_code=400, detail="Only one startDate value is allowed")
    if endDate and len(endDate) > 1:
        raise HTTPException(status_code=400, detail="Only one endDate value is allowed")
    if operator:
        allowed = [e.value for e in OPERATOR_LABELS]

        for op in operator:
            if op not in allowed:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "code": 400,
                        "message": f"Invalid operator value: '{op}'",
                        "allowed_values": allowed
                    }
                )
    gql_query = get_aps(userId=userId, startDate=startDate, endDate=endDate, operator=operator, datasetId=datasetId, fileObjectId=fileObjectId)
    txId = grafeo_begin()
    rows = _run_grafeo_query_in_tx(txId, gql_query)
    total = len(rows)
    nb = 0
    if any([operator, datasetId, fileObjectId]):
        ## Get the full AP log
        response = []
        for row in rows: 
            nb += 1
            if nb > limit:
                break
            response.append(get_full_aplog(row["ap"]["id"], token=token, txId=txId))
    
    else:
        ## Return only User -> Task -> AP
        response = []
        for row in rows:
            nb += 1
            if nb > limit:
                break
            edges = []
            
            nodes = [ Grafeo_to_AP_node(row["ap"]), Grafeo_to_AP_node(row["u"]), Grafeo_to_AP_node(row["t"]) ]
            edges.append({"from": row["u"].get("id"), "to": row["t"].get("id"), "labels": ["request"]})
            edges.append({"from": row["t"].get("id"), "to": row["ap"].get("id"), "labels": ["is_accomplished"]})
            ap_graph = {"ap": {"nodes": nodes, "edges": edges}}
            response.append(ap_graph)
    if txId is not None:
        grafeo_rollback(txId)
    if len(response) == 0 :
        raise HTTPException(status_code=404, detail=f"No AP logs found with the input parameters.")
    return APlogSuccessEnvelope(
        code=200,
        message=f"APlogs retrieved successfully with the input parameters.", 
        aplogs= response,
        count= len(response),
        total=total

    )


    # return Grafeo_to_AP(result)

        
def get_aps(
    userId: Optional[List[str]] = None,
    startDate: Optional[List[str]] = None,
    endDate: Optional[List[str]] = None,
    operator: Optional[List[str]] = None,
    datasetId: Optional[List[str]] = None,
    fileObjectId: Optional[List[str]] = None,
    limit: Optional[int] = 20,
    max_depth: int = 5
) -> str:
    """
    Return a Grafeo-compatible Cypher query that:
      - matches upstream User->Task->AP chains (optionally filtered by userId, startDate and endDate)
      - 
    """

    where_clauses = []
    extended_query = []
    cypher = """ MATCH (u:User)-[:request]->(t:Task)-[:is_accomplished]->(ap:Analytical_Pattern) """
    if userId: 
        where_clauses.append("u.id IN [" + ", ".join(repr(x) for x in userId) + "]")
    if startDate:
        where_clauses.append(f"t.startTime >= date('{startDate[0]}')")
    if endDate:
        where_clauses.append(f"t.startTime <= date('{endDate[0]}')")
    if operator:
        extended_query.append(f"""
            MATCH (ap)-[:consist_of]->(op:Operator)
            WHERE ANY(label IN labels(op) WHERE label IN {operator})
        """)

    if fileObjectId:
        ## A FileObject is an input or an output of an Operator
        extended_query.append(f"""
            MATCH (ap)-[:consist_of]->()-[:input|output]->(fo:FileObject)
            WHERE fo.id IN {fileObjectId}
        """)

    if datasetId:
        ## A Dataset is an input/output of an Operator, or it's connected to a FileObject that is an input/output of an Operator
        extended_query.append(f"""
            MATCH (ap)-[:consist_of]->(op:Operator)
            WHERE 
                EXISTS {{
                    MATCH (op)-[:input|output]->(ds:Dataset)
                    WHERE ds.id IN {datasetId}
                }}
                OR EXISTS {{
                    MATCH (op)-[:input|output]->(fo:FileObject)<-[:distribution]-(ds:Dataset)
                    WHERE ds.id IN {datasetId}
                }}
        """)

    cypher = cypher + "WHERE " + " AND ".join(where_clauses) + " " if where_clauses else cypher
    cypher = cypher + " ".join(extended_query) + " " if extended_query else cypher
    cypher = cypher + "WITH ap, u, t " + " ".join(extended_query) + " "
    cypher = cypher + "ORDER BY ap.startTime DESC "
    cypher = cypher + "RETURN ap, u, t"

    return cypher 


def fetch_nodes_by_ids(node_ids: List[int], txId=None) -> Dict[int, dict]:
    if not node_ids:
        return {}
    ids_str = ', '.join(str(i) for i in node_ids)
    query = f"MATCH (n) WHERE id(n) IN [{ids_str}] RETURN id(n) AS internal_id, n"
    result = _run_grafeo_query_in_tx(txId, query) if txId is not None else run_grafeo_query(query)
    return {row["internal_id"]: row["n"] for row in result}

def fetch_rels_by_ids(rel_ids: List[int], txId=None) -> Dict[int, dict]:
    if not rel_ids:
        return {}
    ids_str = ', '.join(str(i) for i in rel_ids)
    query = f"MATCH ()-[r]-() WHERE id(r) IN [{ids_str}] RETURN id(r) AS internal_id, r"
    result = _run_grafeo_query_in_tx(txId, query) if txId is not None else run_grafeo_query(query)
    return {row["internal_id"]: row["r"] for row in result}

def extract_alias(sql, args_map):
    args_map = args_map

    parsed = sqlglot.parse_one(sql)

    # Reverse view_map for quick lookup: view_name → argname
    reverse_view_map = {}
    for argname, info in args_map.items():
        view_name = info.get("view_name", "")
        if view_name:
            reverse_view_map[view_name] = argname

    # Find all table references in the SQL
    for table in parsed.find_all(exp.Table):
        view_name = table.name  # e.g. "pg_arg2"
        alias = table.alias     # e.g. "p"

        if alias and view_name in reverse_view_map:
            argname = reverse_view_map[view_name]
            args_map[argname]["alias"] = alias

    return args_map

def write_views_minimal_extraction(query:str, args_maps:dict[str, dict]):
    tree = sqlglot.parse_one(query)
    optimized = optimize(tree)

    filters_by_table = extract_filters_per_tables(optimized)

    for argname, arg_info in args_maps.items():
        alias = arg_info.get("alias", argname)
        view_name = arg_info.get("view_name", argname)
        conds = filters_by_table.get(alias, [])
        if not conds:
            if arg_info.get("mimeType") == "text/sql":
                db_connection = arg_info.get("dbConnection", {}).get("name", "Unknown DB")
                pg_sql = f"SELECT * FROM {arg_info.get('contentUrl', '')}"
                pg_sql_escaped = pg_sql.replace("'", "''")
                view = f"""CREATE OR REPLACE VIEW {view_name} AS
                    SELECT *
                    FROM postgres_query(
                        '{db_connection}',
                        '{pg_sql_escaped}'
                    );"""
            if arg_info.get("mimeType") == "text/csv":
                local_path = arg_info.get("contentUrl", "").replace("s3://dataset/", f"/s3/dataset/")
                view = f"""CREATE OR REPLACE VIEW {view_name} AS
                    SELECT *
                    FROM read_csv_auto('{local_path}');"""
        else:    
            where_clause = " AND ".join(c for c in conds).replace(f'"{arg_info.get("alias")}".', "")
            if arg_info.get("mimeType") == "text/sql":
                db_connection = arg_info.get("dbConnection", {}).get("name", "Unknown DB")
                pg_sql = f"SELECT * FROM {arg_info.get('contentUrl', '')} WHERE {where_clause}"
                pg_sql_escaped = pg_sql.replace("'", "''")
                view = f"""CREATE OR REPLACE VIEW {view_name} AS
                    SELECT *
                    FROM postgres_query(
                        '{db_connection}',
                        '{pg_sql_escaped}'
                    );"""
            if arg_info.get("mimeType") == "text/csv":
                local_path = arg_info.get("contentUrl", "").replace("s3://dataset/", f"/s3/dataset/")
                view = f"""CREATE OR REPLACE VIEW {view_name} AS
                    SELECT *
                    FROM read_csv_auto('{local_path}')
                    WHERE {where_clause};"""
        args_maps[argname]["view"] = view
    return args_maps, optimized.sql()

def extract_filters_per_tables(tree):
    filters = {}
    for where in tree.find_all(exp.Where):
        for condition in split_conditions(where.this):
            tables = {col.table for col in condition.find_all(exp.Column)}
            if len(tables) == 1:
                for table in tables:
                    if table not in filters:
                        filters[table] = []
                    print("Condition:", condition.sql(), "Table:", table)
                    filters[table].append(condition.sql())
    # JOIN filters
    for join in tree.find_all(exp.Join):
        on = join.args.get("on")
        if on:
            for cond in split_conditions(on):
                tables = {col.table for col in cond.find_all(exp.Column)}
                if len(tables) == 1:
                    for t in tables:
                        if t not in filters:
                            filters[t] = []
                        print("Join Condition:", cond.sql(), "Table:", t)
                        filters[t].append(cond.sql())
    return filters
def split_conditions(expr):
    if isinstance(expr, exp.And):
        yield from split_conditions(expr.left)
        yield from split_conditions(expr.right)
    else:
        yield expr

@router.delete("/aplog/delete/{ap_id}", status_code=status.HTTP_200_OK)
async def delete_aplog(
    ap_id: str,
    token: str = Depends(security.oauth2_scheme),
    token_payload: dict[str, Any] = Depends(security.require_app_scope)
):
    fetch_query = f"""
        MATCH (ap:Analytical_Pattern)
        WHERE ap.id = '{ap_id}'
        MATCH (u:User)-[req:request]->(t:Task)-[acc:is_accomplished]->(ap)
        WITH ap, COLLECT(DISTINCT u) AS users, COLLECT(DISTINCT t) AS tasks
        OPTIONAL MATCH (ap)-[:consist_of|distribution|input|output|follows|contained_in*0..5]-(n)
        WITH ap, users, tasks, COLLECT(DISTINCT n) AS downstream
        WITH ap, users + tasks + downstream + [ap] AS all_nodes_raw
        UNWIND all_nodes_raw AS n
        WITH ap, COLLECT(DISTINCT n) AS all_nodes
        OPTIONAL MATCH (a)-[r]-(b) WHERE a IN all_nodes AND b IN all_nodes
        RETURN ap, all_nodes, COLLECT(DISTINCT r) AS all_rels
    """
    PROTECTED = {"User", "sc__Dataset", "cr__FileObject", "dg__DatabaseConnection"}  
    txId = grafeo_begin()
    try:
        rows = _run_grafeo_query_in_tx(txId, fetch_query)
        if not rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"AP with id '{ap_id}' not found in Grafeo.",
            )

        row = rows[0]

        # --- Delete relationships first to avoid constraint violations ---
        for rel_id in row["all_rels"]:
            _run_grafeo_query_in_tx(
                txId, f"MATCH ()-[r]-() WHERE id(r) = {rel_id} DELETE r"
            )

        # --- Delete non-protected nodes ---
        for node_id in row["all_nodes"]:
            label_result = _run_grafeo_query_in_tx(
                txId, f"MATCH (n) WHERE id(n) = {node_id} RETURN labels(n) AS labels"
            )
            labels = set(label_result[0]["labels"]) if label_result else set()
            if labels & PROTECTED:
                continue
            _run_grafeo_query_in_tx(
                txId, f"MATCH (n) WHERE id(n) = {node_id} DETACH DELETE n"
            )

        grafeo_commit(txId)
        txId = None

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete AP log with id '{ap_id}': {str(e)}",
        )
    finally:
        if txId is not None:
            grafeo_rollback(txId)

    return {
        "code": 200,
        "message": f"AP log with id '{ap_id}' deleted successfully.",
    }