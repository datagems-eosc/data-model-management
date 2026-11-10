# Service Architecture

The service is a [FastAPI](https://fastapi.tiangolo.com/) application, run by uvicorn in a single container. All its endpoints are under `/api/v1`.

## Analytical Patterns

Most requests and responses are Analytical Patterns (APs): JSON documents that describe an operation on datasets and, once it has run, carry its result. The API parses an incoming AP, runs it or forwards it to the service that runs it, and stores the resulting AP. [AP_LOGS.md](https://github.com/datagems-eosc/data-model-management/blob/main/AP_LOGS.md) shows how stored APs are read and searched.

## What it works with

| Component | What the service uses it for |
| - | - |
| MoMa | The DataGEMS metadata store, where dataset metadata and links between datasets are registered, read and updated |
| Grafeo | A graph database deployed with the service, which stores the AP logs |
| S3 storage | Dataset files, query results and the catalogue, on the platform's S3 storage mounted at `/s3` |
| PostgreSQL | Tables that SQL queries read, with a read-only account |
| DuckDB | Runs SQL queries over CSV files, inside the service |
| Cross-dataset discovery, in-dataset discovery, query disambiguation, dataset recommendation | The DataGEMS services the API forwards APs to |
| DataGEMS AAI | Issues the tokens the API validates |
| DataGEMS logging service | Collects the service's log entries |

[Data Stores](datastore.md) describes what is kept where.

## A request

1. The caller sends a request with a bearer token, which the API validates, as [Security](security.md) describes.
2. The API takes the correlation ID from the request's `x-tracking-correlation` header, or creates one. [Logging](logging.md) explains how to find a request by it.
3. It runs the operation, reading and writing the data stores. When it calls MoMa or another DataGEMS service, it passes on the caller's token and the correlation ID.
4. It answers with the resulting AP or JSON document, or with an error body as described in [Status & Error Codes](error-codes.md).
