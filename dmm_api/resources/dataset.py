import os
import duckdb
from flask_restful import Resource
from flask import request
import pandas as pd

datasets = {}
profiles = {}
query_results = {}


class DatasetResource(Resource):
    def post(self):
        """Receive and store a dataset"""
        data = request.get_json()
        if not data:
            return {"message": "No data received"}, 400

        dataset_id = f"ds_{len(datasets) + 1}"
        datasets[dataset_id] = data

        return {"message": "Dataset stored successfully", "dataset_id": dataset_id}, 200

    def get(self, dataset_id=None):
        """Return dataset with a specific ID or all datasets"""
        if dataset_id:
            if dataset_id not in datasets:
                return {"message": "Dataset not found"}, 404

            return {"dataset_id": dataset_id, "data": datasets[dataset_id]}, 200

        else:
            """Return all the datasets"""
            return {
                "count": len(datasets),
                "dataset_ids": list(datasets.keys()),
                # To view the JSON dataset too, not only the ID
                # "datasets": datasets
            }, 200


class DatasetRegister(Resource):
    def post(self):
        """Sending dataset to external API"""
        if not datasets:
            return {"message": "No datasets available to register"}, 400
        try:
            dataset_id = next(iter(datasets.items()))

            response = {
                "status": "success",
                "api_response": {
                    "registered_id": f"ext_{dataset_id}",
                    "service": "dataset-registry",
                },
            }

            return {
                "message": "Dataset sent to external API",
                "dataset_id": dataset_id,
                "external_response": response,
            }, 200

        except Exception as e:
            return {"message": "Failed to register dataset", "error": str(e)}, 500


class DatasetProfile(Resource):
    def post(self):
        """Receive and store a profile for a dataset"""
        data = request.get_json()
        if not data:
            return {"message": "No profile data received"}, 400

        profile_id = f"pf_{len(profiles) + 1}"
        profiles[profile_id] = data

        return {"message": "Profile stored successfully", "profile_id": profile_id}, 200

    def get(self, profile_id=None):
        """Return a profile with a specific ID or all profiles"""
        if profile_id:
            if profile_id not in profiles:
                return {"message": "Profile not found"}, 404

            return {"profile_id": profile_id, "data": profiles[profile_id]}, 200

        else:
            """Return all the dataset profiles"""
            return {
                "count": len(profiles),
                "profile_ids": list(profiles.keys()),
                # To view the JSON profile too, not only the ID
                # "datasets": profiles
            }, 200


class DatasetQuery(Resource):
    def post(self):
        """Execute a SQL query on a dataset based on an Analytical Pattern."""
        data = request.get_json()
        if not data:
            return {"message": "No query data received"}, 400

        try:
            # Extract the Dataset ID + SQL query
            for node in data["nodes"]:
                if node["labels"] == ["Dataset"]:
                    dataset_id = node["id"]
                elif node["labels"] == ["Software"]:
                    query = node["properties"]["query"][0]

            # Later, this will be Neo4j
            csv_path = f"dmm_api/data/{dataset_id}.csv"
            if not os.path.exists(csv_path):
                return {"message": f"Dataset not found: {csv_path}"}, 404

            # Query with DuckDB
            conn = duckdb.connect()
            conn.execute(
                f"CREATE OR REPLACE TABLE dataset AS SELECT * FROM read_csv_auto('{csv_path}')"
            )
            result = conn.execute(query).fetchall()

            # Convert to a CSV
            data = pd.DataFrame(result).to_csv(index=False)

            query_results[dataset_id] = {
                "dataset_id": dataset_id,
                "query": query,
                "result": data,
            }

            return {
                "message": "Query executed successfully",
                "dataset_id": dataset_id,
                "query": query,
                "result": data,
            }, 200

        except StopIteration:
            return {
                "message": "Analytical Pattern is missing required nodes (Dataset/Software)"
            }, 400
        except Exception as e:
            return {"message": "Failed to execute query", "error": str(e)}, 500

    def get(self, dataset_id=None):
        """Get query results"""
        if dataset_id:
            if dataset_id not in query_results:
                return {
                    "message": f"No query results available for dataset {dataset_id}",
                    "available_datasets": list(query_results.keys()),
                }, 404

            return {
                "dataset_id": dataset_id,
                "query": query_results[dataset_id]["query"],
                "result": query_results[dataset_id]["result"],
            }
        else:
            return {
                "available_dataset_ids": list(query_results.keys()),
                "message": "Access specific dataset at /dataset/query/<dataset_id>",
            }


# POST (send) a dataset
# curl -X POST -H "Content-Type: application/json" --data @../tests/dataset/metadata-britannica.json http://127.0.0.1:5000/api/v1/dataset

# GET all datasets
# curl http://127.0.0.1:5000/api/v1/dataset

# GET a specific dataset
# curl http://127.0.0.1:5000/api/v1/dataset/ds_1
# or
# curl -X GET -H "Content-Type: application/json" http://127.0.0.1:5000/api/v1/dataset/ds_1


# POST a profile
# curl -X POST -H "Content-Type: application/json" --data @../tests/dataset/metadata-britannica.json http://127.0.0.1:5000/api/v1/dataset/profile

# GET all dataset profiles
# curl http://127.0.0.1:5000/api/v1/dataset/profile


# POST an Analytical Pattern
# curl -X POST -H "Content-Type: application/json" --data @../tests/dataset_query/analytical_pattern.json http://127.0.0.1:5000/api/v1/dataset/query

# GET query results for a specific dataset
# curl -X GET http://127.0.0.1:5000/api/v1/dataset/query/ds_1
