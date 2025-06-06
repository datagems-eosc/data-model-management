from flask import Flask, jsonify, redirect, url_for
from flask_restful import Api
from resources.dataset import (
    DatasetResource,
    DatasetRegister,
    DatasetProfile,
    DatasetQuery,
)

app = Flask(__name__)
api = Api(app, prefix="/api/v1")

# Endpoints
api.add_resource(DatasetResource, "/dataset", "/dataset/<string:dataset_id>")
api.add_resource(DatasetRegister, "/dataset/register")
api.add_resource(
    DatasetProfile, "/dataset/profile", "/dataset/profile/<string:profile_id>"
)
api.add_resource(DatasetQuery, "/dataset/query", "/dataset/query/<string:dataset_id>")


@app.route("/")
def home():
    """Root endpoint to redirect to API home"""
    return redirect(url_for("api_home"))


@app.route("/api/v1")
def api_home():
    """API root endpoint showing available endpoints"""
    return jsonify(
        {
            "message": "API V1 is running",
            "endpoints": {
                "dataset": {
                    "description": "Dataset operations",
                    "url": "/api/v1/dataset",
                },
                "dataset_register": {
                    "description": "Dataset registering operations",
                    "url": "/api/v1/dataset/register",
                },
                "dataset_profile": {
                    "description": "Dataset profile operations",
                    "url": "/api/v1/dataset/profile",
                },
                "dataset_query": {
                    "description": "Query the dataset",
                    "url": "/api/v1/dataset/query",
                },
            },
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
