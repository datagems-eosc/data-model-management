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
    extract_dataset_id_from_AP,
    extract_dataset_path_from_AP,
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
    Ready = "Ready"
    Loaded = "Loaded"
    Staged = "Staged"


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
        description="List of Dataset properties to include. Special values 'distribution' and 'recordSet' include connected nodes.",
    ),
    types: Optional[List[DatasetType]] = Query(
        None,
        description="Filter datasets connected to nodes with these labels.",
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
        description="Traversal direction for sorting: 1 for ascending, -1 for descending.",
        ge=-1,
        le=1,
    ),
    status: str = Query(
        "ready",
        description="Dataset status to filter on.",
    ),
):
    url = f"{MOMA_URL}/getDataset"

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
    params["status"] = status

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            datasets = data.get("metadata", {}).get("nodes", data.get("metadata", []))

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
    url = f"{MOMA_URL}/getDataset?nodeIds={dataset_id}"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            metadata = data.get("metadata", {})
            nodes = metadata.get("nodes", [])
            edges = metadata.get("edges", [])
            if not data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=ErrorEnvelope(
                        code=status.HTTP_404_NOT_FOUND,
                        error=f"Dataset with ID {dataset_id} not found in Neo4j",
                    ).model_dump(),
                )

            dataset = {"nodes": nodes, "edges": edges}
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

    check_url = f"{MOMA_URL}/getDataset?nodeIds={dataset_id}&status=loaded"

    async with httpx.AsyncClient() as client:
        try:
            check_response = await client.get(check_url)
            if check_response.status_code == 200:
                metadata = check_response.json().get("metadata", {})
                nodes = metadata.get("nodes", [])

                if nodes:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=ErrorEnvelope(
                            code=status.HTTP_409_CONFLICT,
                            error=f"Dataset with ID {dataset_id} already exists in Neo4j",
                        ).model_dump(),
                    )

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

        datasets_list, _ = extract_datasets_from_AP(
            ap_payload,
            expected_ap_process="load",
            expected_operator_command="update",
        )

        if len(datasets_list) != 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=ErrorEnvelope(
                    code=status.HTTP_400_BAD_REQUEST,
                    error="Load AP must contain exactly one dataset node.",
                ).model_dump(),
            )

        dataset = datasets_list[0]
        json_dataset = json.dumps(dataset, indent=2)
        upload_dataset_to_catalogue(json_dataset, dataset_id)

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
        failed = []

        for dataset in datasets_list:
            dataset_id = dataset.get("@id")
            try:
                check_url = f"{MOMA_URL}/getDataset?nodeIds={dataset_id}"
                check_response = await client.get(check_url)

                if check_response.status_code == 200:
                    metadata = check_response.json().get("metadata", {})
                    nodes = metadata.get("nodes", [])

                    if not nodes:
                        raise HTTPException(
                            status_code=status.HTTP_404_NOT_FOUND,
                            detail=ErrorEnvelope(
                                code=status.HTTP_404_NOT_FOUND,
                                error=f"Dataset with ID {dataset_id} does not exist in Neo4j",
                            ).model_dump(),
                        )
                else:
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
            except httpx.HTTPStatusError:
                failed.append(dataset_id)
            except httpx.RequestError:
                failed.append(dataset_id)
            except Exception:
                failed.append(dataset_id)

        if failed:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    code=status.HTTP_502_BAD_GATEWAY,
                    error=f"Failed to update some datasets: {failed}",
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
