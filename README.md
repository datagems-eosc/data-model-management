# dmm-api

[![Commit activity](https://img.shields.io/github/commit-activity/m/datagems-eosc/dmm-api)](https://img.shields.io/github/commit-activity/m/datagems-eosc/dmm-api)
[![License](https://img.shields.io/github/license/datagems-eosc/dmm-api)](https://img.shields.io/github/license/datagems-eosc/dmm-api)

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



## 2. Running the API
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


## API Usage Examples


Once the API is running, it should be accessible at:
http://127.0.0.1:5001/api/v1

You can interact with it using curl commands:

### 1) Check if there is a dataset

To start, you can check if there are any datasets already.
#### GET all datasets
```bash
curl -X GET -H "Content-Type: application/json" http://127.0.0.1:5000/api/v1/dataset
```

This returns a list of all registered datasets. If none have been uploaded yet, it will return:
```bash
{"status":{"code":200,"message":"Datasets retrieved successfully."},"datasets":[]}
```

### 2) Upload a dataset

#### POST a dataset
```bash
curl -X POST -H "Content-Type: application/json" --data @../tests/dataset/oasa.json http://127.0.0.1:5000/api/v1/dataset/register
```
This registers a new dataset using the JSON payload from oasa.json. The output looks like:
```bash
{"status":{"code":201,"message":"Dataset with UUID f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763 uploaded successfully."},"dataset":{"@context":{"@language":"en","@vocab":"https://schema.org/","citeAs":"cr:citeAs","column":"cr:column","conformsTo":"dct:conformsTo", ...},"@id":"f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763","@type":"sc:Dataset","citeAs":"","conformsTo":"","country":"PT","datePublished":"24-05-2025","description":"Subway data","distribution":[{"@id":"e590461a-a632-4ddb-abc0-bc341165e26c","@type":"cr:FileObject","contentSize":"2407043 B","contentUrl":"","description":"","encodingFormat":"text/csv","name":"csv_1.csv","sha256":"6df8c700f8c47533c567b7b3108f8f6ddf807474260bcb576f626b72107fa3ad"}],"fieldOfScience":["CIVIL ENGINEERING"],"headline":"Subway data.","inLanguage":["el"],"keywords":["dev","keyword"],"license":"???","name":"Dev Data","recordSet":[],"url":"","version":""}}
```

### 3) Check that the upload worked

#### GET all datasets
```bash
curl -X GET -H "Content-Type: application/json" http://127.0.0.1:5000/api/v1/dataset
```
The output looks like:
```bash
{"status":{"code":200,"message":"Datasets retrieved successfully."},"datasets":[{"@context":{"@language":"en","@vocab":"https://schema.org/","citeAs":"cr:citeAs","column":"cr:column","conformsTo":"dct:conformsTo", ...},"@id":"f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763","@type":"sc:Dataset","citeAs":"","conformsTo":"","country":"PT","datePublished":"24-05-2025","description":"Subway data","distribution":[{"@id":"e590461a-a632-4ddb-abc0-bc341165e26c","@type":"cr:FileObject","contentSize":"2407043 B","contentUrl":"","description":"","encodingFormat":"text/csv","name":"csv_1.csv","sha256":"6df8c700f8c47533c567b7b3108f8f6ddf807474260bcb576f626b72107fa3ad"}],"fieldOfScience":["CIVIL ENGINEERING"],"headline":"Subway data.","inLanguage":["el"],"keywords":["dev","keyword"],"license":"???","name":"Dev Data","recordSet":[],"url":"","version":""}]}
```

#### GET a specific dataset
```bash
curl -X GET -H "Content-Type: application/json" http://127.0.0.1:5000/api/v1/dataset/f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763
```
Replace `f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763` with the any other <dataset_id>.

The output is be the same as the one above, since for now we only have that dataset.

### 4) Update the dataset with the profile

#### PUT a profile
```bash
curl -X PUT -H "Content-Type: application/json" --data @../tests/dataset_profile/oasa.json http://127.0.0.1:5000/api/v1/dataset/update
```
This attaches a dataset profile to an existing dataset. The output looks like:
```bash
{"status":{"code":201,"message":"Dataset with UUID f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763 updated successfully."},"dataset":{"@context":{"@language":"en","@vocab":"https://schema.org/","citeAs":"cr:citeAs","column":"cr:column","conformsTo":"dct:conformsTo", ...},"@id":"f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763","@type":"sc:Dataset","citeAs":"","conformsTo":"","country":"PT","datePublished":"24-05-2025","description":"Subway data","distribution":[{"@id":"e590461a-a632-4ddb-abc0-bc341165e26c","@type":"cr:FileObject","contentSize":"2407043 B","contentUrl":"","description":"","encodingFormat":"text/csv","name":"csv_1.csv","sha256":"6df8c700f8c47533c567b7b3108f8f6ddf807474260bcb576f626b72107fa3ad"}],"fieldOfScience":["CIVIL ENGINEERING"],"headline":"Subway data.","inLanguage":["el"],"keywords":["dev","keyword"],"license":"???","name":"Dev Data","recordSet":[{"@id":"4f7acf6f-dfa5-4a5a-9b3d-c234af96fa37","@type":"cr:RecordSet","description":"","field":[{"@id":"d51441bd-19bd-4e8b-8ca8-08bb76796038","@type":"cr:Field","dataType":"sc:Integer","description":"","name":"csv_1/dv_agency","sample":[2,2,2],"source":{"extract":{"column":"dv_agency"},"fileObject":{"@id":"e590461a-a632-4ddb-abc0-bc341165e26c"}}}, ...],"name":"csv_1"}],"url":"","version":""}}
```

### 5) Check that the update worked

#### GET a specific dataset
```bash
curl -X GET -H "Content-Type: application/json" http://127.0.0.1:5000/api/v1/dataset/f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763
```
The output looks like:
```bash
{"status":{"code":200,"message":"Dataset with UUID f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763 retrieved successfully."},"dataset":{"@context":{"@language":"en","@vocab":"https://schema.org/","citeAs":"cr:citeAs","column":"cr:column","conformsTo":"dct:conformsTo", ...},"@id":"f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763","@type":"sc:Dataset","citeAs":"","conformsTo":"","country":"PT","datePublished":"24-05-2025","description":"Subway data","distribution":[{"@id":"e590461a-a632-4ddb-abc0-bc341165e26c","@type":"cr:FileObject","contentSize":"2407043 B","contentUrl":"","description":"","encodingFormat":"text/csv","name":"csv_1.csv","sha256":"6df8c700f8c47533c567b7b3108f8f6ddf807474260bcb576f626b72107fa3ad"}],"fieldOfScience":["CIVIL ENGINEERING"],"headline":"Subway data.","inLanguage":["el"],"keywords":["dev","keyword"],"license":"???","name":"Dev Data","recordSet":[{"@id":"4f7acf6f-dfa5-4a5a-9b3d-c234af96fa37","@type":"cr:RecordSet","description":"","field":[{"@id":"d51441bd-19bd-4e8b-8ca8-08bb76796038","@type":"cr:Field","dataType":"sc:Integer","description":"","name":"csv_1/dv_agency","sample":[2,2,2],"source":{"extract":{"column":"dv_agency"},"fileObject":{"@id":"e590461a-a632-4ddb-abc0-bc341165e26c"}}}, ...],"name":"csv_1"}],"url":"","version":""}}
```

### 6) Query Analytical Pattern

#### POST an Analytical Pattern
```bash
curl -X POST -H "Content-Type: application/json" --data @../tests/dataset_query/analytical_pattern.json http://127.0.0.1:5000/api/v1/dataset/query
```
This sends a query Analytical Pattern to the API. The ouput looks like:
```bash
TODO
```

#### POST a new dataset
```bash
TODO
```
The result of the Analytical Pattern will be stored as a new dataset.

### 7) Check that the results of the query are saved as a new dataset

#### GET all datasets
```bash
curl http://127.0.0.1:5000/api/v1/dataset
```
You should now see the original dataset and a new dataset representing the query result. The ouput looks like:
```bash
TODO
```
---

## License

See the [LICENSE](LICENSE) file for license rights and limitations (MIT).
