# dmm-api

Data Model & Management Platform API (WP5)
v0.01

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
  -F "file=@data/oasa/oasa_daily_ridership_1.csv" \
  -F "file_name=oasa.csv" \
  -F "dataset_id=00000000-9c56-4360-aace-631888242947"
```
The API returns:
```json
{
  "code": 201,
  "message": "Dataset oasa.csv uploaded successfully with ID 475e6aa3-9c56-4360-aace-631888242947 at /s3/scratchpad 475e6aa3-9c56-4360-aace-631888242947",
  "dataset": {
    "id": "475e6aa3-9c56-4360-aace-631888242947",
    "name": "oasa.csv",
    "archivedAt": "/s3/scratchpad/475e6aa3-9c56-4360-aace-631888242947"
  }
}
```

The file is initially uploaded to the scratchpad location.

> NOTE: This method is only for internal testing use. It may be removed in the future.


### 2) Register a Dataset

To register a new dataset in the system:

#### POST a dataset registration AP
```bash
curl -X POST -H "Content-Type: application/json" --data @../tests/register/oasa.json https://datagems-dev.scayle.es/dmm/api/v1/dataset/register
```

This registers a new dataset using the JSON payload. The API returns:
```json
{
   "code":201,
   "message":"Dataset with ID 475e6aa3-9c56-4360-aace-631888242947 registered successfully in Neo4j",
   "ap":{
      "nodes":[
         {
            "@id":"4ecb7e5b-eb82-4ae6-8354-5a2943702fcd",
            "labels":[
               "Analytical_Pattern"
            ],
            "properties":{
               "Description":"Analytical Pattern to register a dataset",
               ...
            }
         },
         {
            "@id":"69ce4693-e71e-4616-9320-037c90a88858",
            "labels":[
               "DataModelManagement_Operator"
            ],
            "properties":{
               "Description":"An operator to register a dataset into DataGEMS",
               ...
            }
         },
         {
            "@id":"475e6aa3-9c56-4360-aace-631888242947",
            "labels":[
               "sc:Dataset"
            ],
            "properties":{
               "@type": "sc:Dataset",
               "archivedAt": "s3://scratchpad/475e6aa3-9c56-4360-aace-631888242947"
            }
         },
         {
            "@id":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "labels":[
               "User"
            ],
            "properties":{
               "City":"Verona",
               ...
            }
         },
         {
            "@id":"dca293c0-e20c-47de-be58-acad8b8c423c",
            "labels":[
               "Task"
            ],
            "properties":{
               "Description":"Task to register a dataset",
               ...
            }
         }
      ],
      "edges":[
         {
            "from":"4ecb7e5b-eb82-4ae6-8354-5a2943702fcd",
            "to":"69ce4693-e71e-4616-9320-037c90a88858",
            "labels":[
               "consist_of"
            ]
         },
         {
            "from":"69ce4693-e71e-4616-9320-037c90a88858",
            "to":"475e6aa3-9c56-4360-aace-631888242947",
            "labels":[
               "input"
            ]
         },
         {
            "from":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "to":"69ce4693-e71e-4616-9320-037c90a88858",
            "labels":[
               "intervene"
            ]
         },
         {
            "from":"dca293c0-e20c-47de-be58-acad8b8c423c",
            "to":"4ecb7e5b-eb82-4ae6-8354-5a2943702fcd",
            "labels":[
               "is_accomplished"
            ]
         },
         {
            "from":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "to":"dca293c0-e20c-47de-be58-acad8b8c423c",
            "labels":[
               "request"
            ]
         }
      ]
   }
}
```

The response includes the dataset ID and the analytical pattern graph structure showing the registration process.

The register worflow may decide to change the ID of the uploaded dataset. If that is the case the returned AP will have a different value for `@id` in the `sc:Dataset` node.
The `archivedAt` attribute will still point to the current folder in the S3 scratchpad.


### 3) Load a Dataset

To move a dataset from the scratchpad to the permanent storage location:

#### PUT a dataset load request
```bash
curl -X PUT -H "Content-Type: application/json" --data @../tests/load/oasa.json https://datagems-dev.scayle.es/dmm/api/v1/dataset/load
```

This moves the dataset from `s3://scratchpad/` to `s3://dataset/`. The API returns:
```json
{
   "code":200,
   "message":"Dataset moved from s3://scratchpad/475e6aa3-9c56-4360-aace-631888242947 to s3://dataset/475e6aa3-9c56-4360-aace-631888242947",
   "ap":{
      "nodes":[
         {
            "@id":"c401292a-4c7a-4e7c-9856-682f626ee1ef",
            "labels":[
               "Analytical_Pattern"
            ],
            "properties":{
               "Description":"Analytical Pattern to load a dataset",
               ...
            }
         },
         {
            "@id":"1c1373cc-2abb-4f1e-b1e7-43befbb6130b",
            "labels":[
               "DataModelManagement_Operator"
            ],
            "properties":{
               "Description":"An operator to load a dataset into s3/dataset",
               ...
            }
         },
         {
            "@id":"475e6aa3-9c56-4360-aace-631888242947",
            "labels":[
               "sc:Dataset"
            ],
            "properties":{
               "@type":"sc:Dataset",
               ...
            }
         },
         {
            "@id":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "labels":[
               "User"
            ],
            "properties":{
               "City":"Verona",
               ...
            }
         },
         {
            "@id":"a75097be-cd64-4942-bd2d-b2b6b399f7cd",
            "labels":[
               "Task"
            ],
            "properties":{
               "Description":"Task to change storage location of a dataset",
               "Name":"Dataset Loading Task"
            }
         }
      ],
      "edges":[
         {
            "from":"c401292a-4c7a-4e7c-9856-682f626ee1ef",
            "to":"1c1373cc-2abb-4f1e-b1e7-43befbb6130b",
            "labels":[
               "consist_of"
            ]
         },
         {
            "from":"1c1373cc-2abb-4f1e-b1e7-43befbb6130b",
            "to":"475e6aa3-9c56-4360-aace-631888242947",
            "labels":[
               "input"
            ]
         },
         {
            "from":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "to":"1c1373cc-2abb-4f1e-b1e7-43befbb6130b",
            "labels":[
               "intervene"
            ]
         },
         {
            "from":"a75097be-cd64-4942-bd2d-b2b6b399f7cd",
            "to":"c401292a-4c7a-4e7c-9856-682f626ee1ef",
            "labels":[
               "is_accomplished"
            ]
         },
         {
            "from":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "to":"a75097be-cd64-4942-bd2d-b2b6b399f7cd",
            "labels":[
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
curl -X PUT -H "Content-Type: application/json" --data @../tests/update/dataset_profile/oasa_light.json https://datagems-dev.scayle.es/dmm/api/v1/dataset/update
```

This updates the dataset properties and creates file object distributions. The API returns:
```json
{
   "code":200,
   "message":"Dataset with ID 475e6aa3-9c56-4360-aace-631888242947 updated successfully in Neo4j",
   "ap":{
      "nodes":[
         {
            "@id":"a8bbe300-c7f2-429c-83fd-ecafda705c90",
            "labels":[
               "Analytical_Pattern"
            ],
            "properties":{
               "Description":"Analytical Pattern to update a dataset",
               ...
            }
         },
         {
            "@id":"24a62ae9-41a9-472d-9a8a-438f35937980",
            "labels":[
               "DataModelManagement_Operator"
            ],
            "properties":{
               "Description":"An operator to update a dataset into DataGEMS",
               ...
            }
         },
         {
            "@id":"475e6aa3-9c56-4360-aace-631888242947",
            "labels":[
               "sc:Dataset"
            ],
            "properties":{
               "@type":"sc:Dataset",
               ...
            }
         },
         {
            "@id":"ecb28ef4-9b68-4133-8d8b-12cf9f2917cf",
            "labels":[
               "FileObject"
            ],
            "properties":{
               "@type":"cr:FileObject",
               ...
            }
         },
         {
            "@id":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "labels":[
               "User"
            ],
            "properties":{
               "City":"Verona",
               ...
            }
         },
         {
            "@id":"efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
            "labels":[
               "Task"
            ],
            "properties":{
               "Description":"Task to update a dataset",
               ...
            }
         }
      ],
      "edges":[
         {
            "from":"a8bbe300-c7f2-429c-83fd-ecafda705c90",
            "to":"24a62ae9-41a9-472d-9a8a-438f35937980",
            "labels":[
               "consist_of"
            ]
         },
         {
            "from":"24a62ae9-41a9-472d-9a8a-438f35937980",
            "to":"475e6aa3-9c56-4360-aace-631888242947",
            "labels":[
               "input"
            ]
         },
         {
            "from":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "to":"24a62ae9-41a9-472d-9a8a-438f35937980",
            "labels":[
               "intervene"
            ]
         },
         {
            "from":"efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
            "to":"a8bbe300-c7f2-429c-83fd-ecafda705c90",
            "labels":[
               "is_achieved"
            ]
         },
         {
            "from":"38b5aafb-184d-4b1e-9e9e-5541afca2c96",
            "to":"efb6e907-52ba-47c0-b1ca-fdbffd8616d6",
            "labels":[
               "request"
            ]
         },
         {
            "from":"475e6aa3-9c56-4360-aace-631888242947",
            "to":"ecb28ef4-9b68-4133-8d8b-12cf9f2917cf",
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
