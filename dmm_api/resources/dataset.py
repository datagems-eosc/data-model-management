from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Dict, Any, List

from .query_executor import execute_query_csv
from .data_resolver import resolve_dataset

datasets = {}
query_results = {}


# Pydantic models
class QueryRequest(BaseModel):
    nodes: List[Dict[str, Any]]


class QueryResponse(BaseModel):
    message: str
    dataset_id: str
    query: str
    json_metadata_path: str


router = APIRouter()


# Endpoints
@router.get("/dataset")
async def get_all_datasets():
    """Return all datasets"""
    return datasets


@router.get("/dataset/{dataset_id}")
async def get_dataset(dataset_id: str):
    """Return dataset with a specific ID"""
    if dataset_id not in datasets:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found"
        )
    return datasets[dataset_id]


@router.post("/dataset/register")
async def register_dataset(dataset: Dict[str, Any]):
    """Receive and store a dataset"""
    try:
        if "@id" not in dataset:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required field '@id'",
            )

        dataset_id = dataset["@id"]
        if dataset_id in datasets:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Dataset with this ID already exists",
            )

        datasets[dataset_id] = dataset
        return dataset

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to store dataset: {str(e)}",
        )


@router.put("/dataset/update")
async def update_dataset(dataset: Dict[str, Any]):
    """Update an existing dataset with new data after profiling"""
    try:
        if "@id" not in dataset:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required field '@id'",
            )

        dataset_id = dataset["@id"]
        if dataset_id not in datasets:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Dataset with this ID does not exist",
            )

        datasets[dataset_id] = dataset
        return dataset

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update dataset: {str(e)}",
        )


@router.post("/dataset/query", response_model=QueryResponse)
async def execute_query(query_data: QueryRequest):
    """Execute a SQL query on a dataset based on an Analytical Pattern"""
    try:
        # Extract information from the JSON data
        dataset_id = None
        dataset_name = None
        database_name = None
        query = None
        software = None

        for node in query_data.nodes:
            if node.get("labels") == ["CSVDB"]:
                database_name = node.get("properties", {}).get("Name")
            if node.get("labels") == ["CSV"]:
                dataset_id = node.get("id")
                dataset_name = node.get("properties", {}).get("Name")
            elif node.get("labels") == ["SQL_Operator"]:
                query = node.get("properties", {}).get("Query")
                software_info = node.get("properties", {}).get("Software", {})
                software = (
                    software_info.get("name")
                    if isinstance(software_info, dict)
                    else None
                )

        if not dataset_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Dataset node not found.",
            )
        if not query:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Query not found."
            )
        if not software:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Database software must be specified in the Operator node.",
            )

        path = resolve_dataset(dataset_id, database_name)

        # execute_query gets called based on the dataset type
        query_result = execute_query_csv(dataset_name, query, software, path)

        query_results[dataset_id] = {
            "dataset_id": dataset_id,
            "query": query_result["query"],
            "result": query_result["result"],
            "json_metadata_path": query_result["json_metadata_path"],
        }

        return QueryResponse(
            message="The query results have been saved in CSV format. The CSV path is available in the JSON metadata file.",
            dataset_id=dataset_id,
            query=query_result["query"],
            json_metadata_path=query_result["json_metadata_path"],
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute query: {str(e)}",
        )


@router.get("/dataset/query")
async def get_all_query_results():
    """Get all available query results"""
    return {
        "available_dataset_ids": list(query_results.keys()),
        "message": "Access specific dataset at /api/v1/dataset/query/{dataset_id}",
    }


@router.get("/dataset/query/{dataset_id}")
async def get_query_results(dataset_id: str):
    """Get query results for a specific dataset"""
    if dataset_id not in query_results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No query results available for dataset {dataset_id}",
        )

    return {
        "dataset_id": dataset_id,
        "query": query_results[dataset_id]["query"],
        "result": query_results[dataset_id]["result"],
    }
