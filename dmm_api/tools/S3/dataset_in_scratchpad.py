import json
import os
from pathlib import Path
from typing import Dict, Any


def upload_dataset_to_scratchpad(dataset: Dict[str, Any], dataset_id: str) -> None:
    SCRATCHPAD_DIR = os.getenv("SCRATCHPAD_DIR", "/s3/scratchpad")
    try:
        scratchpad_folder = Path(SCRATCHPAD_DIR) / dataset_id
        scratchpad_folder.mkdir(parents=True, exist_ok=True)

        # Write the dataset as a JSON
        dataset_file = scratchpad_folder / "dataset.json"
        with open(dataset_file, "w") as f:
            json.dump(dataset, f, indent=4)

    except Exception as e:
        raise RuntimeError(f"Failed to upload dataset to scratchpad: {str(e)}")
