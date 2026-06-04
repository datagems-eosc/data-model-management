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

## 1) Get an AP

You can retrieve an AP that are stored in Grafeo by its id. 

```bash
curl -X 'GET' \
  'https://datagems-dev.scayle.es/dmm/api/v1/aplog/get/a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf' \
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

You can retrieve AP that are stored in Grafeo. You can filter by the User id, startDate, endDate, operator type, dataset id and file object id. All parameters are optional.

### GET the AP logs 

By default, the 20 more recent AP (order by date) are returned. The returned AP logs will contain the User, Task and Analytical Pattern

Parameters:
- userID: Filter by User id.
- startDate/endDate: Filter by the AP date
- limit: The number of returned AP logs, default is 20


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
                        "_id": 29,
                        "_labels": [
                            "Analytical_Pattern"
                        ],
                        "created_at": "2026-06-04T10:31:38.767Z",
                        "description": "Analytical Pattern about disambiguation of a heatwave in Europe recently?",
                        "endTime": "2026-03-17T10:44:17.829Z",
                        "id": "fe00ff20-07fe-4cb9-ac99-16a161d95280",
                        "name": "Disambiguation heatwave europe recently",
                        "startTime": "2026-03-17T10:43:17.829Z",
                        "updated_at": "2026-06-04T10:31:38.767Z"
                    },
                    {
                        "_id": 27,
                        "_labels": [
                            "User"
                        ],
                        "created_at": "2026-06-04T10:31:38.767Z",
                        "id": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                        "updated_at": "2026-06-04T10:31:38.767Z"
                    },
                    {
                        "_id": 28,
                        "_labels": [
                            "Task"
                        ],
                        "created_at": "2026-06-04T10:31:38.767Z",
                        "description": "Task to disambiguate was there a heatwave in Europe recently?",
                        "id": "bd2bb468-9999-4c09-b9af-fc836c2f6c73",
                        "name": "Disambiguation heatwave europe recently Task",
                        "updated_at": "2026-06-04T10:31:38.767Z"
                    }
                ],
                "edges": [
                    {
                        "from": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                        "to": "bd2bb468-9999-4c09-b9af-fc836c2f6c73",
                        "type": "request"
                    },
                    {
                        "from": "bd2bb468-9999-4c09-b9af-fc836c2f6c73",
                        "to": "fe00ff20-07fe-4cb9-ac99-16a161d95280",
                        "type": "is_accomplished"
                    }
                ]
            }
        }
    ],
    "count": 1,
    "total": 1
}

```


## 3) Store an AP

The endpoint allow to store an AP in the database Grafeo.

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

## Delete an AP

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