from fastapi import APIRouter, HTTPException, status
import httpx
from pydantic import UUID4, BaseModel, Field
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


# TODO: Use it
class DatasetRegistration(BaseModel):
    id: UUID4 = Field(..., alias="@id", description="The unique UUID for the dataset.")


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
    """Register dataset through MoMa API which stores it in Neo4j"""
    url = "http://localhost:8000/ingestProfile2MoMa"
    dataset_id = dataset.get("@id")
    check_url = f"http://localhost:8000/retrieveMoMaMetadata?id={dataset_id}"

    # TODO: use Pydantic Model to validate the JSON
    if not dataset_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorEnvelope(
                status=Status(
                    code=status.HTTP_400_BAD_REQUEST, message="Dataset ID missing"
                ),
                errors=["Dataset must contain an '@id' field"],
            ).model_dump(),
        )

    async with httpx.AsyncClient() as client:
        try:
            # Check if a Dataset with such UUID is already stored in the Neo4j
            check_response = await client.get(check_url)
            if check_response.status_code == 200:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=ErrorEnvelope(
                        status=Status(
                            code=status.HTTP_409_CONFLICT,
                            message="Dataset already exists",
                        ),
                        errors=[
                            f"Dataset with ID {dataset_id} already exists in Neo4j"
                        ],
                    ).model_dump(),
                )

            # If not, register the new dataset
            response = await client.post(url, json=dataset)
            response.raise_for_status()

            return DatasetSuccessEnvelope(
                status=Status(
                    code=status.HTTP_201_CREATED,
                    message=f"Dataset with ID {dataset_id} registered successfully in Neo4j",
                ),
                dataset=dataset,
            )

        except httpx.HTTPStatusError as exc:
            error_detail = exc.response.json().get("detail", exc.response.text)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=ErrorEnvelope(
                    status=Status(
                        code=status.HTTP_502_BAD_GATEWAY, message="MoMa API error"
                    ),
                    errors=[f"Error from MoMa API: {error_detail}"],
                ).model_dump(),
            )

        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=ErrorEnvelope(
                    status=Status(
                        code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        message="Service unavailable",
                    ),
                    errors=[f"Failed to connect to MoMa API: {str(exc)}"],
                ).model_dump(),
            )

        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorEnvelope(
                    status=Status(
                        code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        message="Internal server error",
                    ),
                    errors=[f"Unexpected error: {str(exc)}"],
                ).model_dump(),
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
        dataset=dataset.model_dump(by_alias=True),
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
