from flask_restful import Resource
from flask import request

from .query_executor import execute_query_csv
from .data_resolver import resolve_dataset

datasets = {}
profiles = {}
query_results = {}


class DatasetResource(Resource):
    def post(self):
        """Receive and store a dataset"""
        data = request.get_json()
        if not data:
            return {"message": "No data received"}, 400

        # To be changed the way the id is generated
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
                "datasets": datasets,
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
                "datasets": profiles,
            }, 200


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
