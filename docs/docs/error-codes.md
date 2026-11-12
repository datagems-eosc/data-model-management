# Status & Error Codes

The [OpenAPI Reference](openapi.md) gives the responses of each endpoint. This page describes the status codes the service uses and the body of an unsuccessful response.

## Status codes

| Status | When |
| - | - |
| 200 | The request succeeded |
| 400 | The request is invalid, for example a malformed Analytical Pattern or a missing field |
| 401 | The token is missing or invalid |
| 403 | The token's audience does not include this service |
| 404 | The dataset, query result or AP log does not exist, or no endpoint matches the path |
| 409 | The dataset already exists, or its files have already been moved |
| 410 | `POST /convert`, deprecated since v0.3.1 |
| 413 | The request body or an uploaded file is larger than the service accepts |
| 422 | The request does not match the endpoint's schema |
| 500 | The service, or one of its data stores, failed |
| 502 | MoMa answered with an error |
| 503 | MoMa, or the keys that validate tokens, could not be reached |
| 504 | MoMa did not answer in time |

When one of the DataGEMS services the API forwards to fails (cross-dataset discovery, in-dataset discovery, query disambiguation, dataset recommendation), the API answers with that service's status. That service's own message stays in the service's logs.

## Error body

An unsuccessful response carries a JSON body like this one:

```json
{
    "code": 404,
    "error": "Dataset with ID 729f8932-8300-4ac5-a71e-b41904ca7433 not found in Neo4j",
    "correlationId": "5b1e0c9a-3f4d-4e2b-9a71-0c2d8e6f1a33"
}
```

| Field | Content |
| - | - |
| `code` | The HTTP status, repeated |
| `error` | What went wrong. For a server error it carries no internal detail, which stays in the service's logs |
| `details` | When a service the API called failed: `source`, that service, and `statusCode`, the status it answered with, if it answered. Some errors add more fields, such as the dataset ID |
| `correlationId` | The request's correlation ID, also returned in the `x-tracking-correlation` header |

Two kinds of response keep the framework's body, `{"detail": ...}`, with the correlation ID in the header only: `422`, whose `detail` lists what does not match the schema, and `404` for a path that matches no endpoint.

When you report a problem, give the correlation ID: it finds the request in the logs, as [Logging](logging.md) describes.
