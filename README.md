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

To start the API server, navigate to the dmm_api directory and run the api.py script:

```bash
cd dmm_api
python api.py
```


## API Usage Examples

Once the API is running, you can interact with it using curl commands:

### 1) Check if there is a dataset

To start, you can check if there are any datasets already.
#### GET all datasets
```bash
curl http://127.0.0.1:5000/api/v1/dataset
```
This returns a list of all registered datasets. If none have been uploaded yet, it will return an empty array ([]).

### 2) Upload a dataset

#### POST a dataset
```bash
curl -X POST -H "Content-Type: application/json" --data @../tests/dataset/oasa.json http://127.0.0.1:5000/api/v1/dataset/register
```
This registers a new dataset using the JSON payload from oasa.json.

### 3) Check that the upload worked

#### GET all datasets
```bash
curl http://127.0.0.1:5000/api/v1/dataset
```

#### GET a specific dataset
```bash
curl http://127.0.0.1:5000/api/v1/dataset/f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763
```
or
```bash
curl -X GET -H "Content-Type: application/json" http://127.0.0.1:5000/api/v1/dataset/f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763
```
Replace "f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763" with the any other <dataset_id>.

### 4) Update the dataset with the profile

#### PUT a profile
```bash
curl -X PUT -H "Content-Type: application/json" --data @../tests/dataset_profile/oasa.json http://127.0.0.1:5000/api/v1/dataset/update
```
This attaches a dataset profile to an existing dataset.

### 5) Check that the update worked

#### GET a specific dataset
```bash
curl http://127.0.0.1:5000/api/v1/dataset/f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763
```
or
```bash
curl -X GET -H "Content-Type: application/json" http://127.0.0.1:5000/api/v1/dataset/f73815ed453ef32dfe0b19c22a6d410d5b16e3ac88e76dc6d375045a28823763
```

### 6) Query Analytical Pattern

#### POST an Analytical Pattern
```bash
curl -X POST -H "Content-Type: application/json" --data @../tests/dataset_query/analytical_pattern.json http://127.0.0.1:5000/api/v1/dataset/query
```
This sends a query Analytical Pattern to the API. The result will be stored as a new dataset.

### 7) Check that the results of the query are saved as a new dataset

#### GET all datasets
```bash
curl http://127.0.0.1:5000/api/v1/dataset
```
You should now see the original dataset and a new dataset representing the query result.

---

## Docker

To be implemented

---
The uv-python cookiecutter was originally created in [https://github.com/fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
