# AP logs Usage Examples

The API is available at:
`https://datagems-dev.scayle.es/dmm/api/v1/ap`

You can interact with it using curl commands, going into the `tests` folder:

```bash
cd tests
```

> Prerequisite: all steps below require a valid access token. Complete [0) Access Token Setup](#auth-test-bearer-token-required) before running any command.

## Table of Contents


- [1) Get an AP](#1-get-an-AP)
- [2) Search an AP](#2-search-an-AP)
- [3) Store an AP](#3-store-an-AP)
- [4) Delete an AP](#4-delete-an-AP)
## 1) Get an AP

You can retrieve an AP that are stored in Grafeo by its id. 

```bash
curl -X 'GET' \
  'https://datagems-dev.scayle.es/dmm/api/v1/aplog/get/fe00ff20-07fe-4cb9-ac99-16a161d95280' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

The API returns:
```json
{
    "code": 200,
    "message": "success",
    "content": {
        "ap": {
            "nodes": [
                {
                    "labels": [
                        "Analytical_Pattern"
                    ],
                    "id": "a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf",
                    "properties": {
                        ...
                    }
                },
                {
                    "labels": [
                        "Operator",
                        "SQL_Operator",
                        "Query_Operator"
                    ],
                    "id": "437e473a-bc17-46ce-8b36-a8b48cb2ef75",
                    "properties": {
                        ...
                    }
                },
                ...
            ],
            "edges": [
                {
                    "from": "a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf",
                    "to": "437e473a-bc17-46ce-8b36-a8b48cb2ef75",
                    "labels": [
                        "consist_of"
                    ]
                },
                {
                    "from": "474c2c12-4185-42a0-9e79-38af377bdcad",
                    "to": "a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf",
                    "labels": [
                        "is_accomplished"
                    ]
                },
                ...
            ]
        }
    }
}

```

## 2) Search an AP

You can retrieve Analytical Pattern (AP) logs stored in Grafeo.  
All parameters are optional.  
The endpoint supports **two retrieval modes** depending on the filters used.

### Shallow AP Log (default)

Returned when **no deep filters** are provided, or when only these parameters are used:

- userID: Filter by User id.
- startDate/endDate: Filter by the AP date
- limit: The number of returned AP logs, default is 20

In this mode, each AP log contains only: 
- **User** node, **Task** node, and **Analytical_Pattern** node
- Connected nodes: 
    - `User -request-> Task`  
    - `Task -is_accomplished-> Analytical_Pattern`

```bash
curl -X 'GET' \
  'https://datagems-dev.scayle.es/dmm/api/v1/aplog/search' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

The API returns:
```json
{
    "code": 200,
    "message": "APlogs retrieved successfully with the input parameters.",
    "aplogs": [
        {
            "ap": {
                "nodes": [
                    {
                        "labels": [
                            "Analytical_Pattern"
                        ],
                        "id": "fe00ff20-07fe-4cb9-ac99-16a161d95280",
                        "properties": {
                            ...
                        }
                    },
                    {
                        "labels": [
                            "User"
                        ],
                        "id": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                        "properties": {
                            ...
                        }
                    },
                    {
                        "labels": [
                            "Task"
                        ],
                        "id": "bd2bb468-9999-4c09-b9af-fc836c2f6c73",
                        "properties": {
                            ...
                        }
                    }
                ],
                "edges": [
                    {
                        "from": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                        "to": "bd2bb468-9999-4c09-b9af-fc836c2f6c73",
                        "labels": [
                            "request"
                        ]
                    },
                    {
                        "from": "bd2bb468-9999-4c09-b9af-fc836c2f6c73",
                        "to": "fe00ff20-07fe-4cb9-ac99-16a161d95280",
                        "labels": [
                            "is_accomplished"
                        ]
                    }
                ]
            }
        }
    ],
    "count": 1,
    "total": 1
}
```

### Full AP Graph (deep filters)

Returned when *any* of the following parameters are provided:
- `operator`
- `fileObjectId`
- `datasetId`

In this mode, each full AP log will be returned.

```bash
curl -X 'GET' \
  'https://datagems-dev.scayle.es/dmm/api/v1/aplog/search?operator=Query_Disambiguation_Operator' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

The API returns:
```json
{
    "code": 200,
    "message": "APlogs retrieved successfully with the input parameters.",
    "aplogs": [
        {
            "ap": {
                "nodes": [
                    {
                        "labels": [
                            "Analytical_Pattern"
                        ],
                        "id": "fe00ff20-07fe-4cb9-ac99-16a161d95280",
                        "properties": {
                            ...
                        }
                    },
                    {
                        "labels": [
                            "Operator",
                            "Query_Disambiguation_Operator"
                        ],
                        "id": "505580d9-4147-49a3-b0df-e103593388be",
                        "properties": {
                            ...
                        }
                    },
                    ...
                ],
                "edges": [
                    {
                        "from": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                        "to": "bd2bb468-9999-4c09-b9af-fc836c2f6c73",
                        "labels": [
                            "request"
                        ]
                    },
                    {
                        "from": "bd2bb468-9999-4c09-b9af-fc836c2f6c73",
                        "to": "fe00ff20-07fe-4cb9-ac99-16a161d95280",
                        "labels": [
                            "is_accomplished"
                        ]
                    },
                    {
                        "from": "fe00ff20-07fe-4cb9-ac99-16a161d95280",
                        "to": "505580d9-4147-49a3-b0df-e103593388be",
                        "labels": [
                            "consist_of"
                        ]
                    },
                    ...
                ]
            }
        }
    ],
    "count": 1,
    "total": 1
}
```

## 3) Store an AP

The endpoint allow to store an AP in the database Grafeo. This endpoint is only used for testing, the AP logs are stored after they are executed. 

```bash
curl -X 'POST' \
  'https://datagems-dev.scayle.es/dmm/api/v1/aplog/store' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@query-disambiguation/query-disambiguation-response.json" | python -m json.tool
```
Example data file: [query-disambiguation/query-disambiguation-response.json](tests/query-disambiguation/query-disambiguation-response.json) (command path when running from `tests`: `query/query_db.json`).

When the AP is successfully loaded in Grafeo, the API returns :
```json
{
    {
    "message": "AP successfully stored in Grafeo"
    }
}
```

When the AP already exist in Grafeo, the API returns :
```json
{
    {
    "detail": "AP with id fe00ff20-07fe-4cb9-ac99-16a161d95280 already exists in Grafeo."
}
}
```

## 4) Delete an AP

The endpoint allow to delete an AP in the database Grafeo.

```bash
curl -X 'DELETE' \
  'https://datagems-dev.scayle.es/dmm/api/v1/aplog/delete/fe00ff20-07fe-4cb9-ac99-16a161d95280' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" | python -m json.tool
```

The API returns: 
```json
{
    "code": 200,
    "message": "AP log with id 'fe00ff20-07fe-4cb9-ac99-16a161d95280' deleted successfully."
}
```