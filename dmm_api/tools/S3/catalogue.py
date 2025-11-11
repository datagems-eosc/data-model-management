import os
from pathlib import Path


def upload_dataset_to_catalogue(json_content: str, dataset_id: str) -> None:
    """
    Save a JSON-LD dataset string into the local catalogue directory.

    Args:
        json_content (str): The dataset content as a JSON string.
        dataset_id (str): Unique identifier for the dataset.

    Raises:
        TypeError: If json_content is not a string.
        RuntimeError: If writing to the catalogue fails.
    """
    if not isinstance(json_content, str):
        raise TypeError(
            f"Expected JSON string for 'json_content', got {type(json_content).__name__}"
        )

    CATALOGUE_DIR = os.environ.get("CATALOGUE_DIR", "/s3/data-model-management")
    CATALOGUE_FOLDER = os.environ.get("CATALOGUE_FOLDER", "catalogue")
    catalogue_path = os.path.join(CATALOGUE_DIR, CATALOGUE_FOLDER.strip("/"))

    try:
        catalogue_folder = Path(catalogue_path) / dataset_id
        catalogue_folder.mkdir(parents=True, exist_ok=True)

        dataset_file = catalogue_folder / "dataset.json"

        with open(dataset_file, "w", encoding="utf-8") as f:
            f.write(json_content)

    except Exception as e:
        raise RuntimeError(f"Failed to upload dataset to catalogue: {str(e)}")
