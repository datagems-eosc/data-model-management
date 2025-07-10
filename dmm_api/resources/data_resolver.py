import os


def resolve_dataset(database_name, csv_name):
    """Resolve the dataset path based on the dataset ID."""
    # Later, this will be Neo4j
    path = f"dmm_api/data/{database_name}/{csv_name}"
    if not os.path.exists(path):
        raise FileNotFoundError(f"Dataset not found: {path}")
    return path
