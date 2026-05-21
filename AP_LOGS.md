# AP logs Usage Examples

The API is available at:
`https://datagems-dev.scayle.es/dmm/api/v1/ap`

You can interact with it using curl commands, going into the `tests` folder:

```bash
cd tests
```

> Prerequisite: all steps below require a valid access token. Complete [0) Access Token Setup](#auth-test-bearer-token-required) before running any command.

## Table of Contents

- [1) Store an AP](#1-store-an-AP)
- [2) Search an AP](#2-search-an-AP)


## 1) Store an AP

The endpoint allow to store an AP in the database Grafeo.

```bash
curl -X 'POST' \
  'https://datagems-dev.scayle.es/dmm/api/v1/ap/store' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@query/query_db.json" | python -m json.tool
```
Example data file: [tests/query/query_db.json](tests/query/query_db.json) (command path when running from `tests`: `query/query_db.json`).

The API returns when the AP is successfully loaded in Grafeo:
```json
{
    {
    "message": "AP successfully stored in Grafeo"
    }
}
```

The API returns when the AP id already exists in the database.
```json
{
    {
    "detail": "AP with id a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf already exists in Grafeo."
}
}
```

## 2) Search an AP

You can retrieve AP that are stored in Grafeo. You can filter by the AP id, the User id, and select the properties. All parameters are optional.

### GET one or more datasets after filtering

Parameters:
- nodeIds: UUID(s) to fetch.
- properties: Which dataset properties to include (e.g. url, country, name, archivedAt, datePublished).
- apId: Filter by AP id.
- userID: Filter by User id.
- property: Only these properties will be showed.

```bash
curl -X 'GET' \
  'https://datagems-dev.scayle.es/dmm/api/v1/ap/search?userId=38b5aafb-184d-4b1e-9e9e-5541afca2c96' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

The API returns:
```json
[
    {
        "ap": {
            "nodes": [
                {
                    "labels": [
                        "Analytical_Pattern"
                    ],
                    "id": "a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf",
                    "properties": {
                        "description": "Analytical Pattern to query a dataset",
                        "name": "Query Dataset AP",
                        "process": "query",
                        "startTime": "2026-03-17T10:43:17.829Z"
                    }
                },
                {
                    "labels": [
                        "SQL_Operator",
                        "Query_Operator"
                    ],
                    ...
                },
                ...
            ],
            "edges": [
                {
                    "from": "437e473a-bc17-46ce-8b36-a8b48cb2ef75",
                    "to": "8756aa7a-2436-48c7-afb0-0f99401392ad",
                    "labels": [
                        "output"
                    ]
                },
                ...
            ]
        }
    },
    {
        "ap": {
            "nodes": [
                {
                    "labels": [
                        "Analytical_Pattern"
                    ],
                    "id": "4ecb7e5b-eb82-4ae6-8354-5a2943702fcd",
                    ...
                },
                ...
            ],
            "edges": [
                {
                    "from": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                    "to": "dca293c0-e20c-47de-be58-acad8b8c423c",
                    "labels": [
                        "request"
                    ]
                },
                ...
            ]
        }
    }
]

```
