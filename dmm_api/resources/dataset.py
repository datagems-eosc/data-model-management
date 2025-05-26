from flask_restful import Resource
from flask import request

datasets = {}
profiles = {}


class DatasetResource(Resource):
    def post(self):
        """Receive and store a dataset"""
        data = request.get_json()
        if not data:
            return {"message": "No data received"}, 400

        dataset_id = f"ds_{len(datasets) + 1}"
        datasets[dataset_id] = data

        return {"message": "Dataset stored successfully", "dataset_id": dataset_id}, 200

    def get(self):
        """Return all the datasets"""
        return {
            "count": len(datasets),
            "profile_ids": list(datasets.keys()),
            # To view the JSON dataset too, not only the ID
            # "datasets": profiles
        }, 200


class DatasetRegister(Resource):
    def post(self):
        """Sending dataset to external API"""
        try:
            if not datasets:
                return {"message": "No datasets available to register"}, 400

            dataset_id, dataset_data = next(iter(datasets.items()))

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

        profile_id = f"profile_{len(profiles) + 1}"
        profiles[profile_id] = data

        return {"message": "Profile stored successfully", "profile_id": profile_id}, 200

    def get(self):
        """Return all the dataset profiles"""
        return {
            "count": len(profiles),
            "profile_ids": list(profiles.keys()),
            # To view the JSON profile too, not only the ID
            # "datasets": profiles
        }, 200


# POST (send) a dataset
# curl -X POST -H "Content-Type: application/json" --data @dmm_api/metadata-britannica.json http://127.0.0.1:5000/api/v1/dataset

# GET all datasets
# curl http://127.0.0.1:5000/api/v1/dataset

# POST (send) a profile
# curl -X POST -H "Content-Type: application/json" --data @dmm_api/metadata-britannica.json http://127.0.0.1:5000/api/v1/dataset/profile

# GET all dataset profiles
# curl http://127.0.0.1:5000/api/v1/dataset/profile
