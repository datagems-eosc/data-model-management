from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import Dict, Any, List

from .query_executor import execute_query_csv
from .data_resolver import resolve_dataset
from .json_format import create_json

datasets = {}
query_results = {}


# Pydantic models
class QueryRequest(BaseModel):
    nodes: List[Dict[str, Any]]


class QueryResponse(BaseModel):
    path: str


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


# @router.post("/dataset/query", response_model=QueryResponse)
@router.post("/dataset/query")
async def execute_query(query_data: QueryRequest):
    """Execute a SQL query on a dataset based on an Analytical Pattern"""
    try:
        # Extract information from the JSON data
        dataset_id = None
        dataset_name = None
        csv_name = None
        query = None
        software = None
        user_id = None

        for node in query_data.nodes:
            if node.get("labels") == ["Dataset"]:
                dataset_name = node.get("properties", {}).get("name")
            if node.get("labels") == ["FileObject"]:
                dataset_id = node.get("id")
                csv_name = node.get("properties", {}).get("name")
            if node.get("labels") == ["SQL_Operator"]:
                query = node.get("properties", {}).get("Query")
                software = node.get("properties", {}).get("Software", {}).get("name")
            elif node.get("labels") == ["User"]:
                user_id = node.get("id")

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

        path = resolve_dataset(dataset_name, csv_name)

        # execute_query gets called based on the dataset type
        csv_path = execute_query_csv(csv_name, query, software, path, user_id)
        dataste_json = create_json(csv_path)

        return dataste_json

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
