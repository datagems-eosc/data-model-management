from flask import Flask, request, jsonify
from flask_restful import Api, Resource

app = Flask(__name__)
api = Api(app)

json_received_flag = False
stored_json = None


class JsonReceiver(Resource):
    def post(self):
        global json_received_flag, stored_json
        data = request.get_json(force=True)
        if data:
            print("Json received")
            json_received_flag = True
            stored_json = data
            return {"message": "JSON received"}, 200
        else:
            return {"message": "No JSON received"}, 400


@app.route("/")
def home():
    if json_received_flag and stored_json:
        return jsonify(stored_json)
    else:
        return "No JSON received yet"


api.add_resource(JsonReceiver, "/json")

if __name__ == "__main__":
    app.run(debug=True)

# Windows
# curl.exe -X POST -H "Content-Type: application/json" --data @dmm_api\metadata-britannica.json http://127.0.0.1:5000/json
# Linux
# curl -X POST -H "Content-Type: application/json" --data @dmm_api/metadata-britannica.json http://127.0.0.1:5000/jsonfrom
