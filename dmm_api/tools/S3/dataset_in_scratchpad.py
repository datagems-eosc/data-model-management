import json
import os
from pathlib import Path
from typing import Dict, Any


def upload_dataset_to_scratchpad(dataset: Dict[str, Any], dataset_id: str) -> None:
    SCRATCHPAD_DIR = os.getenv("SCRATCHPAD_DIR")
    try:
        target_path = Path(SCRATCHPAD_DIR) / dataset_id
        target_path.parent.mkdir(parents=True, exist_ok=True)
        # Create a dummy file to simulate dataset upload
        with open(target_path, "w") as f:
            json.dump(dataset, f, indent=4)

    except Exception as e:
        raise RuntimeError(f"Failed to upload dataset to scratchpad: {str(e)}")
