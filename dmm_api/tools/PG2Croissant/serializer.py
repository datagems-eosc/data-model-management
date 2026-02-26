import json

def to_jsonld(croissant_dict: dict) -> str:
    """
    Convert a Croissant dataset dictionary to a JSON-LD string.

    Args:
        croissant_dict (dict): The Croissant dataset represented as a dictionary.

    Returns:
        str: The JSON-LD representation of the Croissant dataset.
    """
    return json.dumps(croissant_dict, indent=2)