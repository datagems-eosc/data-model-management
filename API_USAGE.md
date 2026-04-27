# API Usage Examples

The API is available at:
`https://datagems-dev.scayle.es/dmm/api/v1`

You can interact with it using curl commands, going into the `tests` folder:

```bash
cd tests
```

> Prerequisite: all steps below require a valid access token. Complete [0) Access Token Setup](#auth-test-bearer-token-required) before running any command.

## Table of Contents

- [1) Upload a Dataset to s3](#1-upload-a-dataset-to-s3)
- [2) Register a Dataset](#2-register-a-dataset)
- [3) Load a Dataset](#3-load-a-dataset)
- [4) Update a Dataset](#4-update-a-dataset)
- [5) Get one or all Datasets](#5-get-one-or-all-datasets)
- [6) Filter the Datasets](#6-filter-the-datasets)
- [7) Query a Dataset](#7-query-a-dataset)
- [8) Converter](#8-converter)
- [9) Cross-dataset Discovery Search](#9-cross-dataset-discovery-search)
- [10) In-dataset Discovery text2sql](#10-in-dataset-discovery-text2sql)
- [Auth Test (Bearer token required)](#auth-test-bearer-token-required)

## 1) Upload a Dataset to s3

Before registering a dataset, the file should already be present on S3. This method uploads the actual data file using the data-workflow endpoint:

### POST a file to data-workflow
```bash
curl -X POST "https://datagems-dev.scayle.es/dmm/api/v1/data-workflow" \
 -F "file=@data/zoo/zoo-2024.csv" \
 -F "file_name=zoo.csv" \
 -F "dataset_id=c893daaf-680f-4947-88e5-03fd61900795" | python -m json.tool
```
Example data file: [tests/data/zoo/zoo-2024.csv](tests/data/zoo/zoo-2024.csv) (command path when running from `tests`: `data/zoo/zoo-2024.csv`).

The API returns:
```json
{
    "code": 201,
    "message": "Dataset zoo.csv uploaded successfully with ID c893daaf-680f-4947-88e5-03fd61900795 at /s3/scratchpad/c893daaf-680f-4947-88e5-03fd61900795",
    "dataset": {
        "id": "c893daaf-680f-4947-88e5-03fd61900795",
        "name": "zoo.csv",
        "archivedAt": "/s3/scratchpad/c893daaf-680f-4947-88e5-03fd61900795"
    }
}
```

The file is initially uploaded to the scratchpad location.

> NOTE: This method is intended only for internal testing. It may be removed in the future.


## 2) Register a Dataset

To register a new dataset in the system:

### POST a dataset registration AP
```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  --data @register/zoo.json \
  https://datagems-dev.scayle.es/dmm/api/v1/dataset/register \
  | python -m json.tool
```
Example payload: [tests/register/zoo.json](tests/register/zoo.json) (command path when running from `tests`: `register/zoo.json`).


This registers a new dataset using the JSON payload. The API returns:
```json
{
    "code": 201,
    "message": "Dataset with ID c893daaf-680f-4947-88e5-03fd61900795 registered successfully in Neo4j",
    "ap": {
        "nodes": [
            {
                "id": "4ecb7e5b-eb82-4ae6-8354-5a2943702fcd",
                "labels": [
                    "Analytical_Pattern"
                ],
                "properties": {
                    "description": "Analytical Pattern to register a dataset",
                    ...
                }
            },
            {
                "id": "69ce4693-e71e-4616-9320-037c90a88858",
                "labels": [
                    "DataModelManagement_Operator"
                ],
                "properties": {
                    "command": "create",
                    ...
                }
            },
            {
                "id": "c893daaf-680f-4947-88e5-03fd61900795",
                "labels": [
                    "sc:Dataset"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                "labels": [
                    "User"
                ]
            },
            {
                "id": "dca293c0-e20c-47de-be58-acad8b8c423c",
                "labels": [
                    "Task"
                ],
                "properties": {
                    "description": "Task to register a dataset",
                    ...
                }
            }
        ],
        "edges": [
            {
                "from": "4ecb7e5b-eb82-4ae6-8354-5a2943702fcd",
                "to": "69ce4693-e71e-4616-9320-037c90a88858",
                "labels": [
                    "consist_of"
                ]
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "69ce4693-e71e-4616-9320-037c90a88858",
                "labels": [
                    "input"
                ]
            },
            {
                "from": "dca293c0-e20c-47de-be58-acad8b8c423c",
                "to": "4ecb7e5b-eb82-4ae6-8354-5a2943702fcd",
                "labels": [
                    "is_accomplished"
                ]
            },
            {
                "from": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                "to": "dca293c0-e20c-47de-be58-acad8b8c423c",
                "labels": [
                    "request"
                ]
            }
        ]
    }
}```

The response includes the dataset ID and the analytical pattern graph structure showing the registration process.

The register workflow may decide to change the ID of the uploaded dataset. If that is the case the returned AP will have a different value for `id` in the `sc:Dataset` node.
The `archivedAt` attribute will still point to the current folder in the S3 scratchpad.


## 3) Load a Dataset

To move a dataset from the scratchpad to the permanent storage location:

### PUT a dataset load request
```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  --data @load/zoo.json \
  https://datagems-dev.scayle.es/dmm/api/v1/dataset/load \
  | python3 -m json.tool
```
Example payload: [tests/load/zoo.json](tests/load/zoo.json) (command path when running from `tests`: `load/zoo.json`).

Optional query parameter:
- `force` (bool, default `false`): set to `true` when source and target paths are already the same.
```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  --data @load/zoo.json \
  https://datagems-dev.scayle.es/dmm/api/v1/dataset/load?force=true \
  | python3 -m json.tool
```

This moves the dataset from `s3://scratchpad/` to `s3://dataset/`. The API returns:
```json
{
    "code": 200,
    "message": "Dataset moved from s3://scratchpad/c893daaf-680f-4947-88e5-03fd61900795 to s3://dataset/c893daaf-680f-4947-88e5-03fd61900795",
    "ap": {
        "nodes": [
            {
                "id": "c401292a-4c7a-4e7c-9856-682f626ee1ef",
                "labels": [
                    "Analytical_Pattern"
                ],
                "properties": {
                    "description": "Analytical Pattern to load a dataset",
                    ...
                }
            },
            {
                "id": "1c1373cc-2abb-4f1e-b1e7-43befbb6130b",
                "labels": [
                    "DataModelManagement_Operator"
                ],
                "properties": {
                    "command": "update",
                    ...
                }
            },
            {
                "id": "c893daaf-680f-4947-88e5-03fd61900795",
                "labels": [
                    "sc:Dataset"
                ],
                "properties": {
                    "status": "staged",
                    "archivedAt": "s3://dataset/c893daaf-680f-4947-88e5-03fd61900795"
                }
            },
            {
                "id": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                "labels": [
                    "User"
                ]
            },
            {
                "id": "a75097be-cd64-4942-bd2d-b2b6b399f7cd",
                "labels": [
                    "Task"
                ],
                "properties": {
                    "description": "Task to change storage location of a dataset",
                    ...
                }
            }
        ],
        "edges": [
            {
                "from": "c401292a-4c7a-4e7c-9856-682f626ee1ef",
                "to": "1c1373cc-2abb-4f1e-b1e7-43befbb6130b",
                "labels": [
                    "consist_of"
                ]
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "1c1373cc-2abb-4f1e-b1e7-43befbb6130b",
                "labels": [
                    "input"
                ]
            },
            {
                "from": "a75097be-cd64-4942-bd2d-b2b6b399f7cd",
                "to": "c401292a-4c7a-4e7c-9856-682f626ee1ef",
                "labels": [
                    "is_accomplished"
                ]
            },
            {
                "from": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                "to": "a75097be-cd64-4942-bd2d-b2b6b399f7cd",
                "labels": [
                    "request"
                ]
            }
        ]
    }
}
```

>  Note: the endpoint will return an AP with the same `DataModelManagement_Operator` as received in input with an updated `archivedAt` attribute of the dataset node pointing to the new location.

## 4) Update a Dataset

To update an existing dataset with additional metadata or file information:

### PUT a dataset update
```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  --data @update/dataset_profile/zoo_light.json \
  https://datagems-dev.scayle.es/dmm/api/v1/dataset/update \
  | python3 -m json.tool
```
Example payload: [tests/update/dataset_profile/zoo_light.json](tests/update/dataset_profile/zoo_light.json) (command path when running from `tests`: `update/dataset_profile/zoo_light.json`).


This updates the dataset with the light profile. The API returns:
```json
{
    "code": 200,
    "message": "Dataset update completed: 2 node(s) created, 2 edge(s) added",
    "ap": {
        "nodes": [
            {
                "id": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
                "labels": [
                    "Analytical_Pattern"
                ],
                "properties": {
                    "description": "Analytical Pattern to update a dataset",
                    ...
                }
            },
            {
                "id": "24a62ae9-41a9-472d-9a8a-438f35937980",
                "labels": [
                    "DataModelManagement_Operator"
                ],
                "properties": {
                    "command": "update",
                    ...
                }
            },
            {
                "id": "c893daaf-680f-4947-88e5-03fd61900795",
                "labels": [
                    "sc:Dataset"
                ]
            },
            {
                "id": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                "labels": [
                    "Data",
                    "CSV",
                    "cr:FileObject"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "Data",
                    "CSV",
                    "cr:FileObject"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                "labels": [
                    "User"
                ]
            },
            {
                "id": "efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
                "labels": [
                    "Task"
                ],
                "properties": {
                    "description": "Task to update a dataset",
                    ...
                }
            }
        ],
        "edges": [
            {
                "from": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
                "to": "24a62ae9-41a9-472d-9a8a-438f35937980",
                "labels": [
                    "consist_of"
                ]
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "24a62ae9-41a9-472d-9a8a-438f35937980",
                "labels": [
                    "input"
                ]
            },
            {
                "from": "efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
                "to": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
                "labels": [
                    "is_accomplished"
                ]
            },
            {
                "from": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                "to": "efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
                "labels": [
                    "request"
                ]
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                "labels": [
                    "distribution"
                ]
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "distribution"
                ]
            }
        ]
    },
    "metadata": {
        "summary": {
            "nodes_created": 2,
            "nodes_updated": 0,
            "edges_added": 2,
            "record_set_detected": false,
            "datasets_processed": [
                "c893daaf-680f-4947-88e5-03fd61900795"
            ]
        }
    }
}
```

```bash
curl -X PUT \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  --data @update/dataset_profile/zoo_heavy.json \
  https://datagems-dev.scayle.es/dmm/api/v1/dataset/update \
  | python3 -m json.tool
```
Example payload: [tests/update/dataset_profile/zoo_heavy.json](tests/update/dataset_profile/zoo_heavy.json) (command path when running from `tests`: `update/dataset_profile/zoo_heavy.json`).

This updates the dataset with the heavy profile. The API returns:
```json
{
    "code": 200,
    "message": "Dataset update completed: 3 node(s) created, 4 edge(s) added, dataset status set to 'ready'",
    "ap": {
        "nodes": [
            {
                "id": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
                "labels": [
                    "Analytical_Pattern"
                ],
                "properties": {
                    "description": "Analytical Pattern to update a dataset",
                    ...
                }
            },
            {
                "id": "24a62ae9-41a9-472d-9a8a-438f35937980",
                "labels": [
                    "DataModelManagement_Operator"
                ],
                "properties": {
                    "command": "update",
                    ...
                }
            },
            {
                "id": "c893daaf-680f-4947-88e5-03fd61900795",
                "labels": [
                    "sc:Dataset"
                ]
            },
            {
                "id": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                "labels": [
                    "Data",
                    "CSV",
                    "cr:FileObject"
                ]
            },
            {
                "id": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "Data",
                    "CSV",
                    "cr:FileObject"
                ]
            },
            {
                "id": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                "labels": [
                    "cr:RecordSet"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "labels": [
                    "cr:Field",
                    "Column"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                "labels": [
                    "Statistics",
                    "dg:ColumnStatistics"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                "labels": [
                    "User"
                ]
            },
            {
                "id": "efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
                "labels": [
                    "Task"
                ],
                "properties": {
                    "description": "Task to update a dataset",
                    ...
                }
            }
        ],
        "edges": [
            {
                "from": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
                "to": "24a62ae9-41a9-472d-9a8a-438f35937980",
                "labels": [
                    "consist_of"
                ]
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "24a62ae9-41a9-472d-9a8a-438f35937980",
                "labels": [
                    "input"
                ]
            },
            {
                "from": "efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
                "to": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
                "labels": [
                    "is_accomplished"
                ]
            },
            {
                "from": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                "to": "efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
                "labels": [
                    "request"
                ]
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                "labels": [
                    "distribution"
                ]
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "distribution"
                ]
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                "labels": [
                    "recordSet"
                ]
            },
            {
                "from": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                "to": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "labels": [
                    "field"
                ]
            },
            {
                "from": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "to": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "source/fileObject"
                ]
            },
            {
                "from": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "to": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                "labels": [
                    "statistics"
                ]
            }
        ]
    },
    "metadata": {
        "summary": {
            "nodes_created": 3,
            "nodes_updated": 0,
            "edges_added": 4,
            "record_set_detected": true,
            "datasets_processed": [
                "c893daaf-680f-4947-88e5-03fd61900795"
            ]
        }
    }
}
```


## 5) Get one or all Datasets

Retrieve one or all ready datasets.

### GET one or all datasets
```bash
curl -X 'GET' \
  'https://datagems-dev.scayle.es/dmm/api/v1/dataset/search' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -m json.tool

curl -X 'GET' \
  'https://datagems-dev.scayle.es/dmm/api/v1/dataset/get/c893daaf-680f-4947-88e5-03fd61900795' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -m json.tool
```

Optional query parameter:
- `format` (str, default `None`): if `croissant`, the output will be given in Croissant format.
```bash
curl -X 'GET' \
  'https://datagems-dev.scayle.es/dmm/api/v1/dataset/get/c893daaf-680f-4947-88e5-03fd61900795?format=croissant' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -m json.tool
```


The API returns:
```json
{
    "code": 200,
    "message": "Datasets retrieved successfully",
    "datasets": [
        {
            "nodes": [
                {
                    "id": "056ff7ea-ac5a-4496-abc5-ad254ddf58fa",
                    "labels": [
                        "sc:Dataset"
                    ],
                    "properties": {
                        ...
                    }
                },
                {
                    "id": "38d53b0e-c88f-4509-aeea-f9cfa189eab2",
                    "labels": [
                        "Data",
                        "cr:FileObject",
                        "CSV"
                    ],
                    "properties": {
                        ...
                    }
                }
            ],
            "edges": [
                {
                    "from": "056ff7ea-ac5a-4496-abc5-ad254ddf58fa",
                    "to": "38d53b0e-c88f-4509-aeea-f9cfa189eab2",
                    "labels": [
                        "distribution"
                    ],
                    "properties": {}
                }
            ]
        },
        {
            "nodes": [
                {
                    "id": "c893daaf-680f-4947-88e5-03fd61900795",
                    "labels": [
                        "sc:Dataset"
                    ],
                    "properties": {
                        ...
                    }
                },
                {
                    "id": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "labels": [
                        "cr:RecordSet"
                    ],
                    "properties": {
                        ...
                    }
                },
                {
                    "id": "f5234567-890a-bcde-f012-3456789abcde",
                    "labels": [
                        "Data",
                        "cr:FileObject",
                        "CSV"
                    ],
                    "properties": {
                        ...
                    }
                },
                {
                    "id": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                    "labels": [
                        "Data",
                        "cr:FileObject",
                        "CSV"
                    ],
                    "properties": {
                        ...
                    }
                },
                {
                    "id": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                    "labels": [
                        "Column",
                        "cr:Field"
                    ],
                    "properties": {
                        ...
                    }
                },
                {
                    "id": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                    "labels": [
                        "Statistics",
                        "dg:ColumnStatistics"
                    ],
                    "properties": {
                       ...
                    }
                }
            ],
            "edges": [
                {
                    "from": "c893daaf-680f-4947-88e5-03fd61900795",
                    "to": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "labels": [
                        "recordSet"
                    ],
                    "properties": {}
                },
                {
                    "from": "c893daaf-680f-4947-88e5-03fd61900795",
                    "to": "f5234567-890a-bcde-f012-3456789abcde",
                    "labels": [
                        "distribution"
                    ],
                    "properties": {}
                },
                {
                    "from": "c893daaf-680f-4947-88e5-03fd61900795",
                    "to": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                    "labels": [
                        "distribution"
                    ],
                    "properties": {}
                },
                {
                    "from": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "to": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                    "labels": [
                        "field"
                    ],
                    "properties": {}
                },
                {
                    "from": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                    "to": "f5234567-890a-bcde-f012-3456789abcde",
                    "labels": [
                        "source/fileObject"
                    ],
                    "properties": {}
                },
                {
                    "from": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                    "to": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                    "labels": [
                        "statistics"
                    ],
                    "properties": {}
                }
            ]
        },
        ...
    ],
    "offset": 0,
    "count": 5,
    "total": 5
}

{
    "code": 200,
    "message": "Dataset with ID c893daaf-680f-4947-88e5-03fd61900795 retrieved successfully from Neo4j",
    "dataset": {
        "nodes": [
            {
                "id": "c893daaf-680f-4947-88e5-03fd61900795",
                "labels": [
                    "sc:Dataset"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                "labels": [
                    "Data",
                    "cr:FileObject",
                    "CSV"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "Data",
                    "cr:FileObject",
                    "CSV"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "labels": [
                    "Column",
                    "cr:Field"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                "labels": [
                    "cr:RecordSet"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                "labels": [
                    "Statistics",
                    "dg:ColumnStatistics"
                ],
                "properties": {
                   ...
                }
            }
        ],
        "edges": [
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                "labels": [
                    "distribution"
                ],
                "properties": {}
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "distribution"
                ],
                "properties": {}
            },
            {
                "from": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "to": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "source/fileObject"
                ],
                "properties": {}
            },
            {
                "from": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                "to": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "labels": [
                    "field"
                ],
                "properties": {}
            },
            {
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                "labels": [
                    "recordSet"
                ],
                "properties": {}
            },
            {
                "from": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "to": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                "labels": [
                    "statistics"
                ],
                "properties": {}
            }
        ]
    }
}
```


## 6) Filter the Datasets

You can filter the dataset, selecting the properties and sorting the results. All parameters are optional.

### GET one or more datasets after filtering

Parameters:
- nodeIds: UUID(s) to fetch.
- properties: Which dataset properties to include (e.g. url, country, name, archivedAt, datePublished).
- types: Filter by DatasetType (CSV, TextSet, ImageSet, ...).
- orderBy: Field(s) to sort by (archivedAt, datePublished, name, ...).
- direction (int): 1 = ascending (default), -1 = descending.
- publishedDateFrom (date YYYY-MM-DD): Minimum datePublished.
- publishedDateTo (date YYYY-MM-DD): Maximum datePublished.
- status (str, optional): Filter datasets based on their status.

```bash
curl -X 'GET' \
  'https://datagems-dev.scayle.es/dmm/api/v1/dataset/search?properties=archivedAt' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -m json.tool
```

The API returns:
```json
{
    "code": 200,
    "message": "Datasets retrieved successfully",
    "datasets": [
        {
            "nodes": [
                {
                    "id": "056ff7ea-ac5a-4496-abc5-ad254ddf58fa",
                    "labels": [
                        "sc:Dataset"
                    ],
                    "properties": {
                        "archivedAt": "s3://dataset/056ff7ea-ac5a-4496-abc5-ad254ddf58fa"
                    }
                }
            ],
            "edges": null
        },
        ...
    ],
    "offset": 0,
    "count": 5,
    "total": 5
}
```

## 7) Query a Dataset

The polyglot/query endpoint allows you to execute SQL queries across different data sources (supported: PostgreSQL tables, CSV files). 

In the following example, the SQL query could reference a CSV file and a PostgreSQL table. It also support querying two CSV files (even if they are not from the same dataset), or two PostgreSQL tables. 
```sql
SELECT t1.a, t2.b FROM {{arg1}} as t1 JOIN {{arg2}} as t2 ON t1.c = t2.d
```
The FileObjects are given as inputs of the SQL_Operator in the AP, and we use placeholders like `{{arg1}}`, `{{arg2}}` as properties of the input edge to reference the fileObjects (CSV file, table) you want to query.

The output is filled in the AP by adding a path to the generated CSV file in S3, represented as a `FileObject` node (e.g., `s3://data-model-management/results/{result_id}/output.csv`)

### POST a query
```bash
curl -X POST -H "Content-Type: application/json" \
--data @query/query_db.json https://datagems-dev.scayle.es/dmm/api/v1/polyglot/query \
-H "Authorization: Bearer $TOKEN" \
| python -m json.tool
```
Example payload: [tests/query/query_db.json](tests/query/query_db.json) (command path when running from `tests`: `query/query_db.json`).


This query retrieves data from two datasets and creates a new dataset from the output. The API returns:

```json
{
    "code": 200,
    "message": "Query executed successfully, results stored at /s3/data-model-management/results/888f7ce1-6405-4c73-9b3a-fb11b8eb40a1",
    "ap": {
        "nodes": [
            {
                "id": "a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf",
                "labels": [
                    "Analytical_Pattern"
                ],
                "properties": {
                    ...
                }
            },
            {
                "id": "437e473a-bc17-46ce-8b36-a8b48cb2ef75",
                "labels": [
                    "SQL_Operator",
                    "Query_Operator"
                ],
                "properties": {
                    "query": "SELECT t1.time, t1.latitude, t1.longitude, t1.tmin, t2.tmean FROM {{arg1}} as t1 JOIN {{arg2}} as t2 ON t1.time = t2.time and t1.longitude = t2.longitude and t1.latitude = t2.latitude WHERE t1.time >= '1991-01-01' AND t1.time < '1991-02-01' LIMIT 100;",
                    ...
                }
            },
            {
                "id": "7c4d20a0-33e8-471e-8a3f-a7a54aa09f68",
                "labels": [
                    "sc:Dataset"
                ]
            },
            {
                "id": "888f7ce1-6405-4c73-9b3a-fb11b8eb40a1",
                "labels": [
                    "sc:Dataset"
                ],
                "properties": {
                    "sc:archivedAt": "s3://data-model-management/results/888f7ce1-6405-4c73-9b3a-fb11b8eb40a1"
                }
            },
            {
                "id": "b1eef6e4-a3a4-4721-ba86-5f99ed52e2c4",
                "labels": [
                    "dg:DatabaseConnection"
                ]
            },
            {
                "id": "11890253-bc36-4a23-8e3a-d81b83177f84",
                "labels": [
                    "cr:FileObject"
                ]
            },
            {
                "id": "9bd99e2b-97b4-4fb3-858a-6229df4df73f",
                "labels": [
                    "cr:FileObject"
                ]
            },
            {
                "id": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                "labels": [
                    "User"
                ]
            },
            {
                "id": "474c2c12-4185-42a0-9e79-38af377bdcad",
                "labels": [
                    "Task"
                ],
                "properties": {
                    "description": "Task to query a dataset",
                    "name": "Dataset Querying Task"
                }
            },
            {
                "id": "f90042fe-95e2-4774-88ed-7ece926a9888",
                "labels": [
                    "cr:FileObject",
                    "CSV"
                ],
                "properties": {
                    "@type": "cr:FileObject",
                    "contentUrl": "s3://data-model-management/results/888f7ce1-6405-4c73-9b3a-fb11b8eb40a1/output.csv",
                    ...
                }
            }
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
                "from": "11890253-bc36-4a23-8e3a-d81b83177f84",
                "to": "437e473a-bc17-46ce-8b36-a8b48cb2ef75",
                "labels": [
                    "input"
                ],
                "properties": {
                    "argname": "arg1"
                }
            },
            {
                "from": "11890253-bc36-4a23-8e3a-d81b83177f84",
                "to": "b1eef6e4-a3a4-4721-ba86-5f99ed52e2c4",
                "labels": [
                    "contained_in"
                ]
            },
            {
                "from": "7c4d20a0-33e8-471e-8a3f-a7a54aa09f68",
                "to": "b1eef6e4-a3a4-4721-ba86-5f99ed52e2c4",
                "labels": [
                    "distribution"
                ]
            },
            {
                "from": "9bd99e2b-97b4-4fb3-858a-6229df4df73f",
                "to": "b1eef6e4-a3a4-4721-ba86-5f99ed52e2c4",
                "labels": [
                    "contained_in"
                ]
            },
            {
                "from": "9bd99e2b-97b4-4fb3-858a-6229df4df73f",
                "to": "437e473a-bc17-46ce-8b36-a8b48cb2ef75",
                "labels": [
                    "input"
                ],
                "properties": {
                    "argname": "arg2"
                }
            },
            {
                "from": "437e473a-bc17-46ce-8b36-a8b48cb2ef75",
                "to": "888f7ce1-6405-4c73-9b3a-fb11b8eb40a1",
                "labels": [
                    "output"
                ]
            },
            {
                "from": "474c2c12-4185-42a0-9e79-38af377bdcad",
                "to": "a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf",
                "labels": [
                    "is_accomplished"
                ]
            },
            {
                "from": "38b5aafb-184d-4b1e-9e9e-5541afca2c96",
                "to": "474c2c12-4185-42a0-9e79-38af377bdcad",
                "labels": [
                    "request"
                ]
            },
            {
                "from": "7c4d20a0-33e8-471e-8a3f-a7a54aa09f68",
                "to": "11890253-bc36-4a23-8e3a-d81b83177f84",
                "labels": [
                    "distribution"
                ]
            },
            {
                "from": "7c4d20a0-33e8-471e-8a3f-a7a54aa09f68",
                "to": "9bd99e2b-97b4-4fb3-858a-6229df4df73f",
                "labels": [
                    "distribution"
                ]
            },
            {
                "from": "888f7ce1-6405-4c73-9b3a-fb11b8eb40a1",
                "to": "f90042fe-95e2-4774-88ed-7ece926a9888",
                "labels": [
                    "distribution"
                ]
            }
        ]
    }
}
```

This creates a new dataset with the query results and links it to the input datasets and files.
The output dataset is stored in a temporary location in S3 (`s3://data-model-management/results/`) and can be retrieved with the GET endpoints.


## 8) Converter

This converter supports conversion from MoMa (PG-JSON) to Croissant (JSON-LD).

```bash
curl -X POST "https://datagems-dev.scayle.es/dmm/api/v1/convert?from=moma&to=croissant" \
-F "file=@data/zoo/zoo_2024_pg.json" \
| python3 -m json.tool
```
Example input file: [tests/data/zoo/zoo_2024_pg.json](tests/data/zoo/zoo_2024_pg.json) (command path when running from `tests`: `data/zoo/zoo_2024_pg.json`).

This converts the input `zoo_2024_pg.json` file (MoMa format) to Croissant format. The API returns:
```json
{
    "message": "MoMa profile converted to Croissant format successfully",
    "croissant": {
        "@context": {
            "@language": "en",
            "@vocab": "https://schema.org/",
            ...
        },
        "@type": "Dataset",
        "@id": "8930240b-a0e8-46e7-ace8-aab2b42fcc01",
        "distribution": [
            {
                "@id": "2560eadd-5f6a-4a59-a85c-f89a4996fac8",
                "@type": "cr:FileObject",
                ...
            }
        ],
        "recordSet": [
            {
                "@id": "b66da86c-8f51-44ef-a6e6-13bdb61d0978",
                "@type": "cr:RecordSet",
                ...,
                "field": [
                    {
                        "@id": "9fdc4471-a7c3-423b-bfc4-24e3869d11d6",
                        "@type": "cr:Field",
                        ...,
                        "source": {
                            "extract": {
                                "column": "\u00c3\u00af\u00c2\u00bb\u00c2\u00bfKategorie"
                            },
                            "fileObject": {
                                "@id": "2560eadd-5f6a-4a59-a85c-f89a4996fac8"
                            }
                        },
                        "statistics": [
                            {
                                "@id": "583474af-1be5-4f51-85d7-096f8a196cab",
                                "@type": "dg:ColumnStatistics",
                                ...
                            }
                        ]
                    },
                    {
                        "@id": "5914f6ae-2163-4969-8f5f-9896832c798b",
                        "@type": "cr:Field",
                        ...,
                        "source": {
                            "extract": {
                                "column": "Art"
                            },
                            "fileObject": {
                                "@id": "2560eadd-5f6a-4a59-a85c-f89a4996fac8"
                            }
                        },
                        "statistics": [
                            {
                                "@id": "88fa6df0-160b-49d7-adfb-70ac36fa265e",
                                "@type": "dg:ColumnStatistics",
                                ...
                            }
                        ]
                    },
                    {
                        "@id": "70a2c485-38a9-4d85-a1aa-e1b4debced61",
                        "@type": "cr:Field",
                        ...
                        "source": {
                            "extract": {
                                "column": "wissenschaftlicher Name"
                            },
                            "fileObject": {
                                "@id": "2560eadd-5f6a-4a59-a85c-f89a4996fac8"
                            }
                        },
                        "statistics": [
                            {
                                "@id": "b2fe40d4-5306-4e47-a710-9586b35c7263",
                                "@type": "dg:ColumnStatistics",
                                ...
                            }
                        ]
                    },
                    {
                        "@id": "5bf587dd-4dd4-4d8f-ad05-b26d57fb3d39",
                        "@type": "cr:Field",
                        ...,
                        "source": {
                            "extract": {
                                "column": "Anzahl"
                            },
                            "fileObject": {
                                "@id": "2560eadd-5f6a-4a59-a85c-f89a4996fac8"
                            }
                        },
                        "statistics": [
                            {
                                "@id": "52d15255-707d-4d04-b779-c92ba53de860",
                                "@type": "dg:ColumnStatistics",
                                ...
                            }
                        ]
                    },
                    {
                        "@id": "d9f55d08-4253-443d-8ccf-ad55dd93179b",
                        "@type": "cr:Field",
                        ...,
                        "source": {
                            "extract": {
                                "column": "Kommune"
                            },
                            "fileObject": {
                                "@id": "2560eadd-5f6a-4a59-a85c-f89a4996fac8"
                            }
                        },
                        "statistics": [
                            {
                                "@id": "3ab879a5-dd54-4af6-9851-beefaab4afba",
                                "@type": "dg:ColumnStatistics",
                                ...
                            }
                        ]
                    }
                ]
            }
        ],
        "name": "ZOO 2024 Dataset",
        ...
    }
}
```
## 9) Cross-dataset Discovery Search


The `POST /cross-dataset-discovery/search` endpoint requires a valid bearer token.

```bash
curl -X POST --location "https://datagems-dev.scayle.es/dmm/api/v1/cross-dataset-discovery/search" \
-H "Authorization: Bearer $TOKEN" \
-F "file=@cross-dataset/cdd-search-ap-request.json" | python3 -m json.tool
```
Example payload file: [tests/cross-dataset/cdd-search-ap-request.json](tests/cross-dataset/cdd-search-ap-request.json) (command path when running from `tests`: `cross-dataset/cdd-search-ap-request.json`).

This stores the AP in MoMa and forwards the JSON file to the `cross-dataset-discovery/search` endpoint. The API returns:
```json
{
    "code": 200,
    "message": "Cross-Dataset Discovery completed successfully",
    "content": {
        "ap": {
            "nodes": [
                {
                    "id": "3f8a2b1c-4d5e-4a6b-9c7d-8e9f0a1b2c3d",
                    "labels": [
                        "User"
                    ]
                },
                {
                    "id": "7a1b2c3d-4e5f-4a6b-9c8d-7e6f5a4b3c2d",
                    "labels": [
                        "Analytical_Pattern"
                    ],
                    "properties": {
                        "description": "Analytical Pattern to retrieve a list of datasets",
                        "name": "Retrieve Datasets AP",
                        "process": "cross_dataset_discovery",
                        "publishedDate": "2025-06-30",
                        "startTime": "10:00:00"
                    }
                },
                {
                    "id": "9b2c3d4e-5f6a-4b7c-8d9e-0f1a2b3c4d5e",
                    "labels": [
                        "Query_Operator",
                        "CDD_Operator"
                    ],
                    "properties": {
                        "description": "Dataset search executed via Cross-Dataset Discovery",
                        "k": 2,
                        "name": "Cross-Dataset Discovery Operator",
                        "publishedDate": "2025-06-30",
                        "query": "Find datasets about the weather in Athens",
                        "startTime": "10:00:00"
                    }
                },
                {
                    "id": "2d4e5f6a-7b8c-4d9e-0f1a-2b3c4d5e6f7a",
                    "labels": [
                        "Task"
                    ],
                    "properties": {
                        "description": "Task to find a dataset",
                        "name": "Dataset Retrieval Task"
                    }
                },
                {
                    "id": "1f6fba0c-9aea-4345-b5a3-457c924f9e0c",
                    "labels": [
                        "sc:Dataset"
                    ]
                },
                {
                    "id": "article_Datenkompression",
                    "labels": [
                        "cr:FileObject"
                    ]
                },
                {
                    "id": "article_Putsch",
                    "labels": [
                        "cr:FileObject"
                    ]
                }
            ],
            "edges": [
                {
                    "from": "7a1b2c3d-4e5f-4a6b-9c8d-7e6f5a4b3c2d",
                    "labels": [
                        "consist_of"
                    ],
                    "to": "9b2c3d4e-5f6a-4b7c-8d9e-0f1a2b3c4d5e"
                },
                {
                    "from": "2d4e5f6a-7b8c-4d9e-0f1a-2b3c4d5e6f7a",
                    "labels": [
                        "is_accomplished"
                    ],
                    "to": "7a1b2c3d-4e5f-4a6b-9c8d-7e6f5a4b3c2d"
                },
                {
                    "from": "3f8a2b1c-4d5e-4a6b-9c7d-8e9f0a1b2c3d",
                    "labels": [
                        "request"
                    ],
                    "to": "2d4e5f6a-7b8c-4d9e-0f1a-2b3c4d5e6f7a"
                },
                {
                    "from": "9b2c3d4e-5f6a-4b7c-8d9e-0f1a2b3c4d5e",
                    "labels": [
                        "output"
                    ],
                    "to": "article_Datenkompression"
                },
                {
                    "from": "1f6fba0c-9aea-4345-b5a3-457c924f9e0c",
                    "labels": [
                        "distribution"
                    ],
                    "to": "article_Datenkompression"
                },
                {
                    "from": "9b2c3d4e-5f6a-4b7c-8d9e-0f1a2b3c4d5e",
                    "labels": [
                        "output"
                    ],
                    "to": "article_Putsch"
                },
                {
                    "from": "1f6fba0c-9aea-4345-b5a3-457c924f9e0c",
                    "labels": [
                        "distribution"
                    ],
                    "to": "article_Putsch"
                }
            ]
        },
        "metadata": {
            "query_time": 149.59,
            "results": [
                {
                    ...
                },
                {
                    ...
                }
            ]
        }
    }
}
```

## 10) In-dataset Discovery text2sql


The `POST /in-dataset-discovery/text2sql` endpoint requires a valid bearer token.

```bash
curl -X POST --location "https://datagems-dev.scayle.es/dmm/api/v1/in-dataset-discovery/text2sql" \
-H "Authorization: Bearer $TOKEN" \
-F "file=@in-dataset/request.json" | python3 -m json.tool
```
Example payload file: [tests/in-dataset/request.json](tests/cross-dataset/cdd-search-ap-request.json) (command path when running from `tests`: `in-dataset/request.json`).

This stores the AP in MoMa and forwards the JSON file to the `in-dataset-discovery/search` endpoint. Then, the SQL_Operators is executed. The API returns:
```json

```




## Auth Test (Bearer token required)

The `POST /authtest` endpoint requires a valid bearer token.

Current authorized-party (`azp`) accepted by the API: `swagger-client`.

### 0) Get an access token (Keycloak dev realm)
```bash
TOKEN=$(curl --silent --location 'https://datagems-dev.scayle.es/oauth/realms/dev/protocol/openid-connect/token' \
--header 'Content-Type: application/x-www-form-urlencoded' \
--data-urlencode 'grant_type=password' \
--data-urlencode 'client_id=swagger-client' \
--data-urlencode 'username=<CLIENT_ID>' \
--data-urlencode 'password=<CLIENT_SECRET>' \
--data-urlencode 'scope=<SCOPE>' \
| jq -r '.access_token')
```

### 0.1) Call auth test endpoint
```bash
curl --location 'https://datagems-dev.scayle.es/dmm/api/v1/authtest' \
    --header "Authorization: Bearer $TOKEN" \
    --header 'Content-Type: application/json' \
    --data '{"query":"healthcheck","k":1}' | python -m json.tool
```
