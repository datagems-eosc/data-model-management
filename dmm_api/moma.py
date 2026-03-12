import json
import requests


def call_retrieveDataset():
    # url = "http://localhost:8000/getDatasets"
    url = "https://datagems-dev.scayle.es/moma/getDatasets"

    params = {
        "nodeIds": [],
        # "properties": ["url", "country"],  # optional
        # "types": ["RelationalDatabase"],  # optional
        # "orderBy": ["country"],  # optional
        # "publishedDateFrom": None,  # optional, as string YYYY-MM-DD
        # "publishedDateTo": None,  # optional
        # "direction": 1,  # optional
        # "status": "ready",  # optional
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise error for bad status codes
        result = response.json()
        print("Response from service:", result)
        print(json.dumps(result, indent=2))
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
