import asyncio
import os
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
import structlog
import uvicorn

from dmm_api.config import APP_VERSION, require_configuration
from dmm_api.logging_config import (
    configure_logging,
    correlation_id_from_header,
    get_correlation_id,
    request_context,
    write_log_files,
)
from dmm_api.resources.dataset import router as dataset_router
from dmm_api.resources.converter import router as converter_router
from dmm_api.resources.security import router as security_router

configure_logging()
require_configuration()

logger = structlog.get_logger(__name__)

CORRELATION_HEADER = "x-tracking-correlation"


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Kubernetes stops the pod; write what is still buffered before it goes.
    await asyncio.to_thread(write_log_files)


app = FastAPI(
    title="Dataset API",
    description="API for data and model management",
    version=APP_VERSION,
    openapi_url="/api/v1/openapi.json",
    docs_url="/api/v1/swagger",
    redoc_url="/api/v1/redoc",
    root_path=os.getenv("ROOT_PATH", ""),
    lifespan=lifespan,
)


# TODO: check if we need to change the API path prefix or not
app.include_router(dataset_router, prefix="/api/v1")
app.include_router(converter_router, prefix="/api/v1")
app.include_router(security_router, prefix="/api/v1")


@app.middleware("http")
async def log_request(request: Request, call_next):
    """Give the request a correlation ID and log one summary entry for it."""
    correlation_id = correlation_id_from_header(request.headers.get(CORRELATION_HEADER))
    started = time.perf_counter()
    with request_context(correlation_id):
        try:
            response = await call_next(request)
        except Exception:
            logger.exception("unhandled exception")
            response = JSONResponse(
                status_code=500,
                content={
                    "code": 500,
                    "error": "Internal server error",
                    "correlationId": correlation_id,
                },
            )
        status_code = response.status_code
        # 424 means a downstream service failed, not the caller.
        if status_code >= 500 or status_code == 424:
            outcome, log = "server_error", logger.error
        elif status_code >= 400:
            outcome, log = "client_error", logger.warning
        else:
            outcome, log = "success", logger.info
        log(
            "request finished",
            method=request.method,
            path=request.url.path,
            status_code=status_code,
            duration_ms=round((time.perf_counter() - started) * 1000, 1),
            outcome=outcome,
        )
    response.headers[CORRELATION_HEADER] = correlation_id
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Return the detail payload directly, without FastAPI's {"detail": ...} wrapper."""
    detail = exc.detail
    if isinstance(detail, dict):
        body = {**detail}
        body.setdefault("correlationId", get_correlation_id())
    else:
        body = {
            "code": exc.status_code,
            "error": str(detail),
            "correlationId": get_correlation_id(),
        }
    return JSONResponse(status_code=exc.status_code, content=body)


# Root
@app.get("/", include_in_schema=False)
async def home():
    """Root endpoint to redirect to the API home"""
    return RedirectResponse(url="/api/v1")


# API
@app.get("/api/v1")
async def api_home():
    """API root endpoint showing available endpoints"""
    return {
        "message": f"API V1 is running (version: {APP_VERSION})",
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
            "cross-dataset-discovery/search": {
                "description": "Forward AP to CDD search endpoint using",
                "methods": ["POST"],
                "url": "/api/v1/cross-dataset-discovery/search",
            },
            "in-dataset-discovery/text2sql": {
                "description": "Forward AP to IDD text2sql endpoint",
                "methods": ["POST"],
                "url": "/api/v1/in-dataset-discovery/text2sql",
            },
            "in-dataset-discovery/explore": {
                "description": "Forward AP to IDD explore endpoint",
                "methods": ["POST"],
                "url": "/api/v1/in-dataset-discovery/explore",
            },
            "query-disambiguation": {
                "description": "Forward AP to query disambiguation endpoint",
                "methods": ["POST"],
                "url": "/api/v1/query-disambiguation",
            },
            "dataset-recsys/recommend": {
                "description": "Forward AP to REC_SYS recommendation endpoint",
                "methods": ["POST"],
                "url": "/api/v1/dataset-recsys/recommend",
            },
            "MoMa2Croissant": {
                "description": "Convert MoMa profile to Croissant format",
                "methods": ["POST"],
                "url": "/api/v1/convert",
            },
            "authtest": {
                "description": "Test endpoint requiring a valid bearer token",
                "methods": ["POST"],
                "url": "/api/v1/authtest",
            },
            "polyglot/query/result": {
                "description": "Get the result of a polyglot query",
                "methods": ["GET"],
                "url": "/api/v1/polyglot/query/result/{dataset_id}",
            },
            "grafeo/test": {
                "description": "Test endpoint for Grafeo integration",
                "methods": ["GET"],
                "url": "/api/v1/grafeo/test",
            },
            "grafeo/query": {
                "description": "Query endpoint for Grafeo integration",
                "methods": ["POST"],
                "url": "/api/v1/grafeo/query",
            },
            "aplog/store": {
                "description": "Store AP in Grafeo",
                "methods": ["POST"],
                "url": "/api/v1/aplog/store",
            },
            "aplog/search": {
                "description": "Get APs from Grafeo",
                "methods": ["GET"],
                "url": "/api/v1/aplog/search",
            },
            "aplog/get": {
                "description": "Get a specific AP by ID",
                "methods": ["GET"],
                "url": "/api/v1/aplog/get/{ap_id}",
            },
            "aplog/delete": {
                "description": "Delete a specific AP by ID",
                "methods": ["DELETE"],
                "url": "/api/v1/aplog/delete/{ap_id}",
            },
            "dataset-linking/register": {
                "description": "Register a new dataset linking",
                "methods": ["POST"],
                "url": "/api/v1/dataset-linking/register",
            },
            "dataset-linking/get": {
                "description": "Get a specific dataset linking by ID",
                "methods": ["GET"],
                "url": "/api/v1/dataset-linking/get/{dataset_linking_id}",
            },
            # "dataset-linking/delete": {
            #     "description": "Delete a specific dataset linking by ID",
            #     "methods": ["DELETE"],
            #     "url": "/api/v1/dataset-linking/{id}",
            # },
            "dataset-linking/list": {
                "description": "Get dataset linking elements for a specific dataset",
                "methods": ["GET"],
                "url": "/api/v1/dataset-linking/list/{dataset_id}",
            },
        },
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
