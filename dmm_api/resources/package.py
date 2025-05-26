from flask_restful import Resource
from flask import request, jsonify


class Package(Resource):
    def post(self):
        data = request.get_json(force=True)
        return jsonify(data)
