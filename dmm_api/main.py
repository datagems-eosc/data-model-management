import os
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
import uvicorn

from dmm_api.resources.authtest import router as authtest_router
from dmm_api.resources.dataset import router as dataset_router
from dmm_api.resources.converter import router as converter_router

app = FastAPI(
    title="Dataset API",
    description="API for data and model management",
    version="1.0.0",
    openapi_url="/api/v1/openapi.json",
    docs_url="/api/v1/swagger",
    redoc_url="/api/v1/redoc",
    root_path=os.getenv("ROOT_PATH", ""),
)


# TODO: check if we need to change the API path prefix or not
app.include_router(dataset_router, prefix="/api/v1")
app.include_router(converter_router, prefix="/api/v1")
app.include_router(authtest_router, prefix="/api/v1")


# Root
@app.get("/", include_in_schema=False)
async def home():
    """Root endpoint to redirect to the API home"""
    return RedirectResponse(url="/api/v1")


# API
@app.get("/api/v1")
async def api_home():
    """API root endpoint showing available endpoints"""
    app_version = os.getenv("APP_VERSION", "dev")
    return (
        {
            "message": f"API V1 is running (version: {app_version})",
            "endpoints": {
                "dataset": {
                    "description": "Get information about dataset endpoints",
                    "methods": ["GET"],
                    "url": "/api/v1/dataset",
                },
                "dataset_search": {
                    "description": "Search and filter datasets",
                    "methods": ["GET"],
                    "url": "/api/v1/dataset/search",
                },
                "dataset_get": {
                    "description": "Get a specific dataset by ID",
                    "methods": ["GET"],
                    "url": "/api/v1/dataset/get/{dataset_id}",
                },
                "dataset_register": {
                    "description": "Register a new dataset",
                    "methods": ["POST"],
                    "url": "/api/v1/dataset/register",
                },
                "dataset_load": {
                    "description": "Move a dataset from the scratchpad",
                    "methods": ["PUT"],
                    "url": "/api/v1/dataset/load",
                },
                "dataset_update": {
                    "description": "Update an existing dataset after light profiling",
                    "methods": ["PUT"],
                    "url": "/api/v1/dataset/update",
                },
                "polyglot_query": {
                    "description": "Execute queries on datasets",
                    "methods": ["POST"],
                    "url": "/api/v1/polyglot/query",
                },
                "swagger": {
                    "description": "Interactive API documentation (Swagger UI)",
                    "methods": ["GET"],
                    "url": "/api/v1/swagger",
                },
                "MoMa2Croissant": {
                    "description": "Convert MoMa light profile to Croissant format",
                    "methods": ["POST"],
                    "url": "/api/v1/convert",
                },
                "authtest": {
                    "description": "Test endpoint requiring a valid bearer token",
                    "methods": ["POST"],
                    "url": "/api/v1/authtest",
                },
                "authtest_cdd_search": {
                    "description": "Forward payload to CDD search using exchanged token",
                    "methods": ["POST"],
                    "url": "/api/v1/authtest/cdd-search",
                },
            },
        },
    )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
