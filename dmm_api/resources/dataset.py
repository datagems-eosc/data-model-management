from flask_restful import Resource
from flask import request

from .query_executor import execute_query_csv
from .data_resolver import resolve_dataset

datasets = {}
query_results = {}


# This class return the dataset (either before or after the profiling) through a GET request
class DatasetResource(Resource):
    def get(self, dataset_id=None):
        """Return dataset with a specific ID or all datasets"""
        if dataset_id:
            if dataset_id not in datasets:
                return {"message": "Dataset not found"}, 404

            return {"data": datasets[dataset_id]}, 200

        else:
            """Return all the datasets"""
            return {
                "dataset_ids": list(datasets.keys()),
                # To view the JSON dataset too, not only the ID
                "datasets": datasets,
            }, 200


# This class receives a dataset (before profiling) through a POST request
class DatasetRegister(Resource):
    def post(self):
        """Receive and store a dataset"""
        try:
            data = request.get_json()
            if not data:
                return {"message": "No data received"}, 400
            if "@id" not in data:
                return {"message": "Missing required field '@id'"}, 400

            dataset_id = data["@id"]
            if dataset_id in datasets:
                return {"message": "Dataset with this ID already exists"}, 409
            datasets[dataset_id] = data

            return {
                "message": "Dataset stored successfully",
                "dataset_id": dataset_id,
                "data": data,
            }, 200
        except Exception as e:
            return {"message": "Failed to store dataset", "error": str(e)}, 500


# This class updates the dataset of DatasetRegister with the new dataset after profiling through a PUT request
class DatasetUpdate(Resource):
    def put(self):
        """Update an existing dataset with new data after profiling"""
        try:
            data = request.get_json()
            if not data:
                return {"message": "No data received"}, 400
            if "@id" not in data:
                return {"message": "Missing required field '@id'"}, 400

            dataset_id = data["@id"]
            if dataset_id in datasets:
                datasets[dataset_id] = data
            else:
                return {"message": "Dataset with this ID does not exist"}, 404

            return {
                "message": "Dataset updated successfully",
                "dataset_id": dataset_id,
                "data": data,
            }, 200
        except Exception as e:
            return {"message": "Failed to update dataset", "error": str(e)}, 500


# This class executes a SQL query on a dataset through a POST request
class DatasetQuery(Resource):
    def post(self):
        """Execute a SQL query on a dataset based on an Analytical Pattern."""
        data = request.get_json()
        if not data:
            return {"message": "No query data received"}, 400

        try:
            # Extract information from the JSON data
            dataset_id = None
            dataset_name = None
            database_name = None
            query = None
            software = None

            for node in data["nodes"]:
                if node["labels"] == ["CSVDB"]:
                    database_name = node["properties"]["Name"]
                if node["labels"] == ["CSV"]:
                    dataset_id = node["id"]
                    dataset_name = node["properties"]["Name"]
                elif node["labels"] == ["SQL_Operator"]:
                    query = node["properties"]["Query"]
                    software = node["properties"]["Software"]["name"]
            if not dataset_id:
                return {"message": "Dataset node not found."}, 400
            if not query:
                return {"message": "Query not found."}, 400
            if not software:
                return {
                    "message": "Database software must be specified in the Operator node."
                }, 400

            path = resolve_dataset(dataset_id, database_name)

            # execute_query gets called based on the dataset type
            query_result = execute_query_csv(dataset_name, query, software, path)

            query_results[dataset_id] = {
                "dataset_id": dataset_id,
                "query": query_result["query"],
                "result": query_result["result"],
                "json_metadata_path": query_result["json_metadata_path"],
            }

            return {
                "message": "The query results have been saved in CSV format. The CSV path is available in the JSON metadata file.",
                "dataset_id": dataset_id,
                "query": query_result["query"],
                "json_metadata_path": query_result["json_metadata_path"],
            }, 200

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
