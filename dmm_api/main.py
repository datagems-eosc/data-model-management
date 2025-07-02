from fastapi import FastAPI
from fastapi.responses import RedirectResponse
import uvicorn

from dmm_api.resources.dataset import router as dataset_router

app = FastAPI(
    title="Dataset API",
    description="API for data and model management",
    version="1.0.0",
    docs_url="/api/v1/docs",
    redoc_url="/api/v1/redoc",
)

app.include_router(dataset_router, prefix="/api/v1")


# Root
@app.get("/", include_in_schema=False)
async def home():
    """Root endpoint to redirect to API home"""
    return RedirectResponse(url="/api/v1")


# API
@app.get("/api/v1")
async def api_home():
    """API root endpoint showing available endpoints"""
    return {
        "message": "API V1 is running",
        "endpoints": {
            "dataset": {
                "description": "Get all datasets or specific dataset by ID",
                "methods": ["GET"],
                "url": "/api/v1/dataset",
                "example_url": "/api/v1/dataset/dataset_id",
            },
            "dataset_register": {
                "description": "Register a new dataset (POST)",
                "methods": ["POST"],
                "url": "/api/v1/dataset/register",
            },
            "dataset_update": {
                "description": "Update an existing dataset after profiling (PUT)",
                "methods": ["PUT"],
                "url": "/api/v1/dataset/update",
            },
            "dataset_query": {
                "description": "Execute queries on datasets",
                "methods": ["GET", "POST"],
                "url": "/api/v1/dataset/query",
            },
        },
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
