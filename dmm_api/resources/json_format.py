import json
import os
import uuid
import numpy as np
import pandas as pd


def create_json(csv_path):
    """
    Convert a CSV file to JSON format and create a dataset metadata file.
    """
    try:
        df = pd.read_csv(csv_path)
        data_records = df.to_dict(orient="records")
        data_records = df.replace({np.nan: None}).to_dict(orient="records")

        new_dataset_uuid = str(uuid.uuid4())

        json_dataset = {
            "@id": new_dataset_uuid,
            "@type": "sc:Dataset",
            "name": "Query Result",
            "data": data_records,
        }

        json_path = os.path.join(os.path.dirname(csv_path), "results.json")
        with open(json_path, "w") as f:
            json.dump(json_dataset, f, indent=4)

        return json_dataset

    except Exception as e:
        raise Exception(f"Failed to create JSON dataset: {str(e)}")
