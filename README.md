# dmm-api

[![Commit activity](https://img.shields.io/github/commit-activity/m/datagems-eosc/dmm-api)](https://img.shields.io/github/commit-activity/m/datagems-eosc/dmm-api)
[![License](https://img.shields.io/github/license/datagems-eosc/dmm-api)](https://img.shields.io/github/license/datagems-eosc/dmm-api)

Data Model & Management Platform API (WP5)


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

To run the Flask API, navigate to the dmm_api folder and execute the api.py file in your terminal:

```bash
cd dmm_api
python api.py
```

## API Usage Examples

Once the API is running, you can interact with it using the following commands:

### 1) Dataset Operations

#### POST a dataset
```bash
curl -X POST -H "Content-Type: application/json" --data @../tests/dataset/oasa.json http://127.0.0.1:5000/api/v1/dataset/register
```

#### GET all datasets
```bash
curl http://127.0.0.1:5000/api/v1/dataset
```

#### GET a specific dataset
```bash
curl http://127.0.0.1:5000/api/v1/dataset/ds_1
```
or
```bash
curl -X GET -H "Content-Type: application/json" http://127.0.0.1:5000/api/v1/dataset/ds_1
```

### 2) Dataset Profile Operations

#### PUT a profile
```bash
curl -X PUT -H "Content-Type: application/json" --data @../tests/dataset_profile/oasa.json http://127.0.0.1:5000/api/v1/dataset/update
```

### 3) Query Analytical Pattern

#### POST an Analytical Pattern
```bash
curl -X POST -H "Content-Type: application/json" --data @../tests/dataset_query/analytical_pattern.json http://127.0.0.1:5000/api/v1/dataset/query
```

---

## Docker

To be implemented

---
The uv-python cookiecutter was originally created in [https://github.com/fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
