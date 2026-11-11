# API Overview

All endpoints are under `/api/v1`. The [OpenAPI Reference](openapi.md) lists them with their parameters; a running service also serves it as Swagger UI at `/api/v1/swagger` and as ReDoc at `/api/v1/redoc`.

| Group | Endpoints | What they do |
| - | - | - |
| Datasets | `GET /dataset/search`, `GET /dataset/get/{dataset_id}`, `POST /dataset/register`, `PUT /dataset/load`, `PUT /dataset/update`, `POST /data-workflow` | Search, read, register, load and update datasets in MoMa; upload a dataset's files with its metadata |
| Queries | `POST /polyglot/query`, `GET /polyglot/query/result/{dataset_id}` | Run SQL over a dataset's CSV files and PostgreSQL tables, and read the stored result |
| DataGEMS services | `POST /cross-dataset-discovery/search`, `POST /in-dataset-discovery/explore`, `POST /in-dataset-discovery/text2sql`, `POST /query-disambiguation`, `POST /dataset-recsys/recommend` | Forward an Analytical Pattern to the service, store the result, and return it |
| AP logs | `POST /aplog/store`, `GET /aplog/get/{ap_id}`, `GET /aplog/search`, `DELETE /aplog/delete/{ap_id}` | Store, read, search and delete Analytical Pattern logs |
| Dataset linking | `POST /dataset-linking/register`, `GET /dataset-linking/{id}`, `GET /dataset-linking/list/{dataset_id}`, `DELETE /dataset-linking/{id}` | Register, read, list and delete links between datasets |
| Conversion | `GET /dataset/get/{dataset_id}?format=croissant` | Return a dataset in Croissant JSON-LD. `POST /convert` is deprecated since v0.3.1 and answers `410 Gone` |
| Graph | `POST /grafeo/query`, `GET /grafeo/test` | Run a Cypher query on Grafeo; check that Grafeo answers |
| Service | `GET /api/v1`, `POST /authtest` | Report the running version; check a token |

## Every request

- **Token:** send `Authorization: Bearer <token>`, as [Security](security.md) describes.
- **Correlation ID:** every response carries an `x-tracking-correlation` header. Send your own ID in that header to find your requests in the logs under it; see [Logging](logging.md).
- **Errors:** [Status & Error Codes](error-codes.md) describes the error responses.

## Examples

[API_USAGE.md](https://github.com/datagems-eosc/data-model-management/blob/main/API_USAGE.md) has curl examples for the endpoints, including how to get a token, and [AP_LOGS.md](https://github.com/datagems-eosc/data-model-management/blob/main/AP_LOGS.md) for the AP log endpoints.
