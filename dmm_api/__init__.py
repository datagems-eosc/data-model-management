from flask import Flask, jsonify, redirect, url_for
from flask_restful import Api
from resources.dataset import DatasetResource, DatasetRegister, DatasetProfile

app = Flask(__name__)
api = Api(app, prefix="/api/v1")

# Endpoints
api.add_resource(DatasetResource, "/dataset")
api.add_resource(DatasetRegister, "/dataset/register")
api.add_resource(DatasetProfile, "/dataset/profile")


@app.route("/")
def home():
    """Root endpoint that redirects to API home"""
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
                    "description": "Register dataset with external service",
                    "url": "/api/v1/dataset/register",
                },
                "dataset_profile": {
                    "description": "Dataset profile operations",
                    "url": "/api/v1/dataset/profile",
                },
            },
        }
    )


if __name__ == "__main__":
    app.run(debug=True)

# routes.py
# from resources.dataset import (
#     DatasetResource,
#     DatasetRegister,
#     DatasetProfile
# )

# def initialize_routes(app, api):
#     api.add_resource(DatasetResource, '/api/v1/dataset')
#     api.add_resource(DatasetRegister, '/api/v1/dataset/register')
#     api.add_resource(DatasetProfile, '/api/v1/dataset/profile')
