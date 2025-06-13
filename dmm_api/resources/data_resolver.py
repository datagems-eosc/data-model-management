import os


def resolve_dataset(dataset_id):
    """Resolve the dataset path based on the dataset ID."""
    # Later, this will be Neo4j
    csv_path = f"dmm_api/data/{dataset_id}.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Dataset not found: {csv_path}")
    return csv_path
