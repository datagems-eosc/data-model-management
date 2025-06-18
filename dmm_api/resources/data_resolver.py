import os


def resolve_dataset(dataset_id, database_name):
    """Resolve the dataset path based on the dataset ID."""
    # Later, this will be Neo4j
    path = f"dmm_api/data/{database_name}/{dataset_id}.csv"
    if not os.path.exists(path):
        raise FileNotFoundError(f"Dataset not found: {path}")
    return path
