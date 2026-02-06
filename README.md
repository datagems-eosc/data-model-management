# dmm-api

Data Model & Management Platform API (WP5)

The official documentation is available at: https://datagems-eosc.github.io/data-model-management/latest/

## 1. Getting started with your project

### Set Up Your Development Environment

#### Linux/macOS

If you do not have `uv` installed, you can install it with

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
After executing the command above, you will need to restart your shell.

`uv` is a python package similar to `poetry`.

Then, install the environment and the pre-commit hooks with

```bash
make install
```

This will also generate your `uv.lock` file





### Windows

If you do not have `uv` installed, you can install it with

```bash
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
Or following the instructions at docs.astral.sh/uv/getting-started/installation/#installation-methods.

After executing the command above, you will need to restart your shell.

`uv` is a python package similar to `poetry`.

Then, install the environment and the pre-commit hooks with

```bash
uv sync
uv run pre-commit install
```

This will also generate your `uv.lock` file

---


## API Usage Examples
The API is available at:
https://datagems-dev.scayle.es/dmm/api/v1

You can interact with it using curl commands, going into the `dmm_api` folder:

```bash
cd dmm_api
```


### 1) Upload a Dataset to s3

Before registering a dataset, the files should be already present on s3. This method uploads the actual data file using the data-workflow endpoint:

#### POST a file to data-workflow
```bash
curl -X POST "https://datagems-dev.scayle.es/dmm/api/v1/data-workflow" \
 -F "file=@data/zoo/zoo-2024.csv" \
 -F "file_name=zoo.csv" \
 -F "dataset_id=c893daaf-680f-4947-88e5-03fd61900795" | python -m json.tool
```
The API returns:
```json
{
    "code": 201,
    "message": "Dataset zoo.csv uploaded successfully with ID c893daaf-680f-4947-88e5-03fd61900795 at /s3/scratchpad/c893daaf-680f-4947-88e5-03fd61900795",
    "dataset": {
        "id": "c893daaf-680f-4947-88e5-03fd61900795",
        "name": "zoo.csv",
        "sc:archivedAt": "/s3/scratchpad/c893daaf-680f-4947-88e5-03fd61900795"
    }
}
```

The file is initially uploaded to the scratchpad location.

> NOTE: This method is only for internal testing use. It may be removed in the future.


### 2) Register a Dataset

To register a new dataset in the system:

#### POST a dataset registration AP
```bash
curl -X POST -H "Content-Type: application/json" --data @../tests/register/oasa.json https://datagems-dev.scayle.es/dmm/api/v1/dataset/register | python -m json.tool
```

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
                    "Description": "Analytical Pattern to register a dataset",
                    ...
                }
            },
            {
                "id": "69ce4693-e71e-4616-9320-037c90a88858",
                "labels": [
                    "DataModelManagement_Operator"
                ],
                "properties": {
                    "Description": "An operator to register a dataset into DataGEMS",
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
                    "Description": "Task to register a dataset",
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
}
```

The response includes the dataset ID and the analytical pattern graph structure showing the registration process.

The register worflow may decide to change the ID of the uploaded dataset. If that is the case the returned AP will have a different value for `id` in the `sc:Dataset` node.
The `archivedAt` attribute will still point to the current folder in the S3 scratchpad.


### 3) Load a Dataset

To move a dataset from the scratchpad to the permanent storage location:

#### PUT a dataset load request
```bash
curl -X PUT -H "Content-Type: application/json" --data @../tests/load/oasa.json https://datagems-dev.scayle.es/dmm/api/v1/dataset/load | python -m json.tool
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
                    "Description": "Analytical Pattern to load a dataset",
                    ...
                }
            },
            {
                "id": "1c1373cc-2abb-4f1e-b1e7-43befbb6130b",
                "labels": [
                    "DataModelManagement_Operator"
                ],
                "properties": {
                    "Description": "An operator to load a dataset into s3/dataset",
                    ...
                }
            },
            {
                "id": "c893daaf-680f-4947-88e5-03fd61900795",
                "labels": [
                    "sc:Dataset"
                ],
                "properties": {
                    "dg:status": "staged",
                    "sc:archivedAt": "s3://dataset/c893daaf-680f-4947-88e5-03fd61900795"
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
                    "Description": "Task to change storage location of a dataset",
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


### 4) Update a Dataset

To update an existing dataset with additional metadata or file information:

#### PUT a dataset update
```bash
curl -X PUT -H "Content-Type: application/json" --data @../tests/update/dataset_profile/zoo_light.json https://datagems-dev.scayle.es/dmm/api/v1/dataset/update | python -m json.tool
```

This updates the dataset with the light profile. The API returns:
```json
{
    "code": 200,
    "message": "Dataset update completed: 2 node(s) created, 2 edge(s) created",
    "ap": {
        "nodes": [
            {
                "id": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
                "labels": [
                    "Analytical_Pattern"
                ],
                "properties": {
                    "Description": "Analytical Pattern to update a dataset",
                    ...
                }
            },
            {
                "id": "24a62ae9-41a9-472d-9a8a-438f35937980",
                "labels": [
                    "DataModelManagement_Operator"
                ],
                "properties": {
                    "Description": "An operator to update a dataset into DataGEMS",
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
                    "CSV",
                    "cr:FileObject"
                ],
                "properties": {
                    "contentSize": "2407043 B",
                    ...
                }
            },
            {
                "id": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "CSV",
                    "cr:FileObject"
                ],
                "properties": {
                    "contentSize": "1500000 B",
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
                    "Description": "Task to update a dataset",
                    "Name": "Dataset Updating Task"
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
                    "is_achieved"
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
    }
}

```bash
curl -X PUT -H "Content-Type: application/json" --data @../tests/update/dataset_profile/zoo_heavy.json https://datagems-dev.scayle.es/dmm/api/v1/dataset/update | python -m json.tool
```
This updates the dataset with the heavy profile. The API returns:
```json
{
    "code": 200,
    "message": "Dataset update completed: 3 node(s) created, 4 edge(s) created",
    "ap": {
        "nodes": [
            {
                "id": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
                "labels": [
                    "Analytical_Pattern"
                ],
                "properties": {
                    "Description": "Analytical Pattern to update a dataset",
                    ...
                }
            },
            {
                "id": "24a62ae9-41a9-472d-9a8a-438f35937980",
                "labels": [
                    "DataModelManagement_Operator"
                ],
                "properties": {
                    "Description": "An operator to update a dataset into DataGEMS",
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
                    "CSV",
                    "cr:FileObject"
                ],
                "properties": {
                    "contentSize": "2407043 B",
                    ...
                }
            },
            {
                "id": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "CSV",
                    "cr:FileObject"
                ],
                "properties": {
                    "contentSize": "1500000 B",
                    ...
                }
            },
            {
                "id": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                "labels": [
                    "cr:RecordSet"
                ],
                "properties": {
                    "cr:examples": "{\"\\u00ef\\u00bb\\u00bfKategorie\": [\"Amphibien\", ...]}",
                    ...
                }
            },
            {
                "id": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "labels": [
                    "cr:Field"
                ],
                "properties": {
                    "dataType": "sc:Text",
                    ...
                }
            },
            {
                "id": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                "labels": [
                    "dg:ColumnStatistics"
                ],
                "properties": {
                    "dg:histogram": null,
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
                    "Description": "Task to update a dataset",
                    ...
                }
            }
        ],
        "edges": [
            {
                "from": "a8bbe300-c7f2-429c-83fd-ecafda705c90",
                "to": "24a62ae9-41a9-472d-9a8a-438f35937980",
                "labels": [
                    "consistOf"
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
                    "isAchieved"
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
                "to": "c893daaf-680f-4947-88e5-03fd61900795",
                "labels": [
                    "source/fileObject"
                ]
            },
            {
                "from": "6bc80891-81bf-4890-9e47-44f6ee72a6c1",
                "to": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "labels": [
                    "statistics"
                ]
            }
        ]
    }
}
```


### 5) Get one or all Datasets

Retrieve one or all ready datasets.

#### GET one or all datasets
```bash
curl -X GET -H "Content-Type: application/json" "https://datagems-dev.scayle.es/dmm/api/v1/dataset/search" | python -m json.tool

curl -X GET -H "Content-Type: application/json" "https://datagems-dev.scayle.es/dmm/api/v1/dataset/get/c893daaf-680f-4947-88e5-03fd61900795" | python -m json.tool
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
                        "country": "PT",
                        ...
                    }
                },
                {
                    "id": "38d53b0e-c88f-4509-aeea-f9cfa189eab2",
                    "labels": [
                        "CSV",
                        "cr:FileObject"
                    ],
                    "properties": {
                        "contentUrl": "s3://dataset/056ff7ea-ac5a-4496-abc5-ad254ddf58fa/weather_data_fr.csv",
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
                        "country": "CH",
                        ...
                    }
                },
                {
                    "id": "f5234567-890a-bcde-f012-3456789abcde",
                    "labels": [
                        "CSV",
                        "cr:FileObject"
                    ],
                    "properties": {
                        "contentUrl": "s3://dataset/c893daaf-680f-4947-88e5-03fd61900795/zoo-metadata.csv",
                        ...
                    }
                },
                {
                    "id": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                    "labels": [
                        "CSV",
                        "cr:FileObject"
                    ],
                    "properties": {
                        "contentUrl": "s3://dataset/c893daaf-680f-4947-88e5-03fd61900795/zoo-2024.csv",
                        ...
                    }
                },
                {
                    "id": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                    "labels": [
                        "cr:Field"
                    ],
                    "properties": {
                        "dataType": "sc:Text",
                        ...
                    }
                },
                {
                    "id": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "labels": [
                        "cr:RecordSet"
                    ],
                    "properties": {
                        "cr:examples": "{\"\\u00ef\\u00bb\\u00bfKategorie\": [\"Amphibien\", ...]}",
                        ...
                    }
                }
            ],
            "edges": [
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
                    "from": "c893daaf-680f-4947-88e5-03fd61900795",
                    "to": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                    "labels": [
                        "recordSet"
                    ],
                    "properties": {}
                }
            ]
        }
    ]
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
                    "country": "CH",
                    ...
                }
            },
            {
                "id": "f5234567-890a-bcde-f012-3456789abcde",
                "labels": [
                    "CSV",
                    "cr:FileObject"
                ],
                "properties": {
                    "contentUrl": "s3://dataset/c893daaf-680f-4947-88e5-03fd61900795/zoo-metadata.csv",
                    ...
                }
            },
            {
                "id": "883b5c9b-408a-4dd8-8619-e34e664b9920",
                "labels": [
                    "CSV",
                    "cr:FileObject"
                ],
                "properties": {
                    "contentUrl": "s3://dataset/c893daaf-680f-4947-88e5-03fd61900795/zoo-2024.csv",
                    ...
                }
            },
            {
                "id": "c5f7c053-4aa5-4c3f-9d7f-9a2d58a3b6e1",
                "labels": [
                    "cr:Field"
                ],
                "properties": {
                    "dataType": "sc:Text",
                    ...
                }
            },
            {
                "id": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                "labels": [
                    "cr:RecordSet"
                ],
                "properties": {
                    "cr:examples": "{\"\\u00ef\\u00bb\\u00bfKategorie\": [\"Amphibien\", ...]}",
                    ...
                }
            }
        ],
        "edges": [
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
                "from": "c893daaf-680f-4947-88e5-03fd61900795",
                "to": "eb87b0f3-fb8a-4a24-8234-3da28b7398a0",
                "labels": [
                    "recordSet"
                ],
                "properties": {}
            }
        ]
    }
}
```


### 6) Filter the Datasets

You can filter the dataset, selecting the properties and sorting the results. All parameters are optional.

#### GET one or more datasets after filtering

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
curl -X GET -H "Content-Type: application/json" "https://datagems-dev.scayle.es/dmm/api/v1/dataset/search?properties=sc:archivedAt" | python -m json.tool
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
                        "sc:archivedAt": "s3://dataset/056ff7ea-ac5a-4496-abc5-ad254ddf58fa"
                    }
                }
            ],
            "edges": []
        },
        {
            "nodes": [
                {
                    "id": "c893daaf-680f-4947-88e5-03fd61900795",
                    "labels": [
                        "sc:Dataset"
                    ],
                    "properties": {
                        "sc:archivedAt": "s3://dataset/c893daaf-680f-4947-88e5-03fd61900795"
                    }
                }
            ],
            "edges": []
        }
    ]
}
```

### 7) Query a Dataset

To query one or more datasets:

#### POST a query
```bash
curl -X POST -H "Content-Type: application/json" --data @../tests/query/query_before.json https://datagems-dev.scayle.es/dmm/api/v1/polyglot/query | python -m json.tool
```

This query two dataset properties and creates a new dataset from the output. The API returns:
```json
{
   "code":200,
   "message":"Query executed successfully, results stored at /s3/data-model-management/results/2c830b06-a1da-48ca-a982-15062002797c",
   "ap":{
      "nodes":[
         {
            "id:"a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf",
            "labels":[
               "Analytical_Pattern"
            ],
            "properties":{
               "Description":"Analytical Pattern to query a dataset",
               ...
            }
         },
         {
            "id":"437e473a-bc17-46ce-8b36-a8b48cb2ef75",
            "labels":[
               "SQL_Operator"
            ],
            "properties":{
               "Description":"Query executed via DuckDB on two datasets",
               "Name":"DuckDB Query Operator",
               "Parameters":{
                  "arg1":"aca33450-d1b4-4721-af00-7f1a73d5e34f",
                  "arg2":"c5a06fbd-887f-4562-8121-55dfe5a658c7",
                  "command":"query",
                  "queryType":"SELECT"
               },
               "PublishedDate":"2025-06-30",
               "Query":"SELECT crete.date AS date, crete.energy_mwh AS crete_mwh, kyonos.energy_mwh AS kyonos_mwh FROM {{arg1}} AS kyonos JOIN {{arg2}} AS crete ON crete.date = kyonos.date WHERE crete.energy_mwh > 10000 ORDER BY crete.date",
               ...
            }
         },
         {
            "id":"928a3f45-7eec-474a-ab07-90736feb7ace",
            "labels":[
               "sc:Dataset"
            ],
            "properties":{
               "type":"sc:Dataset",
               "archivedAt":"s3://dataset/928a3f45-7eec-474a-ab07-90736feb7ace",
               ...
            }
         },
         {
            "id":"36f74548-0f4f-47c7-bfdf-6502e9fc0768",
            "labels":[
               "sc:Dataset"
            ],
            "properties":{
               "type":"sc:Dataset",
               "archivedAt":"s3://dataset/36f74548-0f4f-47c7-bfdf-6502e9fc0768",
               ...
            }
         },
         {
            "id":"2c830b06-a1da-48ca-a982-15062002797c",
            "labels":[
               "sc:Dataset"
            ],
            "properties":{
               "type":"sc:Dataset",
               "archivedAt":"s3://data-model-management/results/2c830b06-a1da-48ca-a982-15062002797c",
               "description":"Temporary dataset created after a query",
               ...
            }
         },
         {
            "id":"aca33450-d1b4-4721-af00-7f1a73d5e34f",
            "labels":[
               "cr:FileObject",
               "CSV"
            ],
            "properties":{
               "type":"cr:FileObject",
               ...
         },
         {
            "id":"c5a06fbd-887f-4562-8121-55dfe5a658c7",
            "labels":[
               "cr:FileObject",
               "CSV"
            ],
            "properties":{
               "type":"cr:FileObject",
               ...
            }
         },
         {
            "id":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "labels":[
               "User"
            ],
            "properties":{
               "City":"Verona",
               ...
            }
         },
         {
            "id":"474c2c12-4185-42a0-9e79-38af377bdcad",
            "labels":[
               "Task"
            ],
            "properties":{
               "Description":"Task to query a dataset",
               ...
            }
         },
         {
            "id":"9a25e4fe-4ab7-467b-ac58-577a70f12c67",
            "labels":[
               "cr:FileObject",
               "CSV"
            ],
            "properties":{
               "type":"cr:FileObject",
               ...
            }
         }
      ],
      "edges":[
         {
            "from":"a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf",
            "to":"437e473a-bc17-46ce-8b36-a8b48cb2ef75",
            "labels":[
               "consist_of"
            ]
         },
         {
            "from":"437e473a-bc17-46ce-8b36-a8b48cb2ef75",
            "to":"928a3f45-7eec-474a-ab07-90736feb7ace",
            "labels":[
               "input"
            ]
         },
         {
            "from":"437e473a-bc17-46ce-8b36-a8b48cb2ef75",
            "to":"36f74548-0f4f-47c7-bfdf-6502e9fc0768",
            "labels":[
               "input"
            ]
         },
         {
            "from":"437e473a-bc17-46ce-8b36-a8b48cb2ef75",
            "to":"2c830b06-a1da-48ca-a982-15062002797c",
            "labels":[
               "output"
            ]
         },
         {
            "from":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "to":"437e473a-bc17-46ce-8b36-a8b48cb2ef75",
            "labels":[
               "intervene"
            ]
         },
         {
            "from":"474c2c12-4185-42a0-9e79-38af377bdcad",
            "to":"a51f3e82-ca74-4ef6-8d1e-2bb08f4df6cf",
            "labels":[
               "is_achieved"
            ]
         },
         {
            "from":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "to":"474c2c12-4185-42a0-9e79-38af377bdcad",
            "labels":[
               "request"
            ]
         },
         {
            "from":"928a3f45-7eec-474a-ab07-90736feb7ace",
            "to":"aca33450-d1b4-4721-af00-7f1a73d5e34f",
            "labels":[
               "distribution"
            ]
         },
         {
            "from":"36f74548-0f4f-47c7-bfdf-6502e9fc0768",
            "to":"c5a06fbd-887f-4562-8121-55dfe5a658c7",
            "labels":[
               "distribution"
            ]
         },
         {
            "from":"2c830b06-a1da-48ca-a982-15062002797c",
            "to":"9a25e4fe-4ab7-467b-ac58-577a70f12c67",
            "labels":[
               "distribution"
            ]
         }
      ]
   }
}

```

---

## Running the API Locally
You can run the API either directly via Python or using Docker.

### Terminal

To start the API server, open your terminal and navigate to the dmm_api directory.

Run the api.py script:

```bash
cd dmm_api
python api.py
```

---

### Docker

Alternatively, run the API using Docker.

Use the provided Dockerfile to build the image:
```bash
docker build -t fastapi-image .
```
Start a container from the image and mount the results directory:
```bash
docker run -d -p 5000:5000 -v /path/to/your/local/results:/app/dmm_api/data/results --name fastapi-container fastapi-image
```
Replace `/path/to/your/local/results` with the actual path to your local results directory, e.g., `desktop/repositories/data-model-management/dmm_api/data/results`.

---

## License

See the [LICENSE](LICENSE) file for license rights and limitations (MIT).
