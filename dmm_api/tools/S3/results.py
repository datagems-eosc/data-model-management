import os
from pathlib import Path
from uuid import uuid4


def upload_csv_to_results(
    file_content: bytes,
    # , user_id: str
) -> str:
    RESULTS_DIR = os.environ.get("RESULTS_DIR", "/s3/data-model-management")
    RESULTS_FOLDER = os.environ.get("RESULTS_FOLDER", "results")
    results_path = os.path.join(RESULTS_DIR, RESULTS_FOLDER.strip("/"))
    try:
        dataset_id = str(uuid4())

        results_folder = Path(results_path) / dataset_id
        results_folder.mkdir(parents=True, exist_ok=True)

        # Write the dataset file
        dataset_file = results_folder / "output.csv"
        # NOTE: If file name exists we overwrite the file silently
        with open(dataset_file, "wb") as f:
            # Save bytes to file
            f.write(file_content)

        return str(results_folder)

    except Exception as e:
        raise RuntimeError(f"Failed to upload dataset to results: {str(e)}")
