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


class Status(BaseModel):
    code: int
    message: str


class DatasetSuccessEnvelope(BaseModel):
    status: Status
    dataset: Dict[str, Any]


class DatasetsSuccessEnvelope(BaseModel):
    status: Status
    datasets: List[Dict[str, Any]]


class ErrorEnvelope(BaseModel):
    status: Status
    errors: List[str]


router = APIRouter()


# Endpoints
@router.get("/dataset", response_model=DatasetsSuccessEnvelope)
async def get_all_datasets():
    """Return all datasets"""
    return DatasetsSuccessEnvelope(
        status=Status(
            code=status.HTTP_200_OK, message="Datasets retrieved successfully."
        ),
        datasets=list(datasets.values()),
    )


@router.get("/dataset/{dataset_id}", response_model=DatasetSuccessEnvelope)
async def get_dataset(dataset_id: str):
    """Return dataset with a specific ID"""
    if dataset_id not in datasets:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorEnvelope(
                status=Status(
                    code=status.HTTP_404_NOT_FOUND, message="Dataset not found."
                ),
                errors=[f"Dataset with UUID {dataset_id} not found."],
            ).model_dump(),
        )

    return DatasetSuccessEnvelope(
        status=Status(
            code=status.HTTP_200_OK,
            message=f"Dataset with UUID {dataset_id} retrieved successfully.",
        ),
        dataset=datasets[dataset_id],
    )


@router.post("/dataset/register", response_model=DatasetSuccessEnvelope)
async def register_dataset(dataset: Dict[str, Any]):
    """Receive and store a dataset"""
    dataset_id = dataset["@id"]

    if dataset_id in datasets:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=ErrorEnvelope(
                status=Status(
                    code=status.HTTP_409_CONFLICT, message="Dataset already exists."
                ),
                errors=[f"Dataset with UUID {dataset_id} already exists."],
            ).model_dump(),
        )

    datasets[dataset_id] = dataset

    return DatasetSuccessEnvelope(
        status=Status(
            code=status.HTTP_201_CREATED,
            message=f"Dataset with UUID {dataset_id} uploaded successfully.",
        ),
        dataset=dataset,
    )


@router.put("/dataset/update")
async def update_dataset(dataset: Dict[str, Any]):
    """Update an existing dataset with new data after profiling"""
    dataset_id = dataset["@id"]

    if dataset_id not in datasets:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=ErrorEnvelope(
                status=Status(
                    code=status.HTTP_404_NOT_FOUND, message="Dataset does not exist."
                ),
                errors=[f"Dataset with UUID {dataset_id} does not exists."],
            ).model_dump(),
        )

    datasets[dataset_id] = dataset

    return DatasetSuccessEnvelope(
        status=Status(
            code=status.HTTP_201_CREATED,
            message=f"Dataset with UUID {dataset_id} updated successfully.",
        ),
        dataset=dataset,
    )


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
