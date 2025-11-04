"""
This module provides utility functions for handling datasets in the scratchpad.

Functions:
    upload_dataset_to_scratchpad(dataset: bytes, file_name: str, dataset_id: str) -> str:
        Uploads a dataset file to the scratchpad directory.

    save_croissant_to_scratchpad(dataset: Dict[str, Any], dataset_id: str) -> str:
        Saves a croissant JSON object to the scratchpad directory.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any


def upload_dataset_to_scratchpad(
    file_content: bytes, file_name: str, dataset_id: str
) -> str:
    """
    Uploads a dataset file to the scratchpad directory.

    Args:
        file_content (bytes): The dataset content in bytes.
        file_name (str): The name of the file to save in the scratchpad.
        dataset_id (str): The unique identifier for the dataset.

    Returns:
        str: The path to the scratchpad folder where the dataset is saved.

    Raises:
        RuntimeError: If the upload process fails.
    """
    SCRATCHPAD_DIR = os.getenv("SCRATCHPAD_DIR", "/s3/scratchpad")
    try:
        scratchpad_folder = Path(SCRATCHPAD_DIR) / dataset_id
        scratchpad_folder.mkdir(parents=True, exist_ok=True)

        # Write the dataset as a JSON
        dataset_file = scratchpad_folder / file_name

        # NOTE: If file name exists we overwrite the file silently
        with open(dataset_file, "w") as f:
            # Save bytes to file
            f.write(file_content.decode("utf-8"))

        return str(scratchpad_folder)
    except Exception as e:
        raise RuntimeError(f"Failed to upload dataset to scratchpad: {str(e)}")


def save_croissant_to_scratchpad(dataset: Dict[str, Any], dataset_id: str) -> str:
    """
    Saves a croissant JSON object to the scratchpad directory.

    Args:
        dataset (Dict[str, Any]): The croissant JSON object to save.
        dataset_id (str): The unique identifier for the dataset.

    Returns:
        str: The path to the saved JSON file.

    Raises:
        RuntimeError: If the save process fails.
    """
    SCRATCHPAD_DIR = os.getenv("SCRATCHPAD_DIR", "/s3/scratchpad")
    try:
        scratchpad_folder = Path(SCRATCHPAD_DIR) / dataset_id
        scratchpad_folder.mkdir(parents=True, exist_ok=True)

        # Write the dataset as a JSON
        dataset_file = scratchpad_folder / f".dataset-{dataset_id}.json"
        with open(dataset_file, "w") as f:
            json.dump(dataset, f, indent=4)

        return str(dataset_file)
    except Exception as e:
        raise RuntimeError(f"Failed to upload dataset to scratchpad: {str(e)}")
