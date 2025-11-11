import os
from pathlib import Path


RESULTS_DIR = os.environ.get("RESULTS_DIR", "/s3/data-model-management")
RESULTS_FOLDER = os.environ.get("RESULTS_FOLDER", "results")
results_path = os.path.join(RESULTS_DIR, RESULTS_FOLDER.strip("/"))


def upload_csv_to_results(file_content: bytes, dataset_id: str) -> tuple[str, str]:
    try:
        results_folder = Path(results_path) / dataset_id
        results_folder.mkdir(parents=True, exist_ok=True)

        # Write the dataset file
        dataset_file = results_folder / "output.csv"
        # NOTE: If file name exists we overwrite the file silently
        with open(dataset_file, "wb") as f:
            # Save bytes to file
            f.write(file_content)

        return str(results_folder), dataset_id

    except Exception as e:
        raise RuntimeError(f"Failed to upload dataset to results: {str(e)}")


def upload_ap_to_results(ap_content: str, dataset_id: str) -> None:
    try:
        results_folder = Path(results_path) / dataset_id
        results_folder.mkdir(parents=True, exist_ok=True)

        # Write the AP file
        ap_file = results_folder / ".query_ap.json"
        with open(ap_file, "w", encoding="utf-8") as f:
            f.write(ap_content)

    except Exception as e:
        raise RuntimeError(f"Failed to upload AP to results: {str(e)}")
