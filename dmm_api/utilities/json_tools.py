"""
JSON tools: JSON-LD validation and JSON-LD -> PG-JSON conversion.

Contract
- Inputs: Python dict produced by json.loads/json.load (nested mapping/list/primitive types)
- No file I/O in this module. Callers should load JSON themselves.
- Outputs: Python dicts (validated JSON-LD or PG-JSON structure)

Error modes
- Validation raises JsonLDValidationError for semantic/shape issues
- Conversion raises ValueError for invalid input types or missing required info
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Dict, List, Set, Union


class JsonLDValidationError(Exception):
    """Raised when JSON-LD validation fails."""


def _validate_uuid(value: str, field_name: str) -> None:
    """Validate that a string is a valid UUID format."""
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError):
        raise JsonLDValidationError(
            f"'{field_name}' must be a valid UUID, got '{value}'"
        )


def validate_jsonld(data: Dict[str, Any], strict: bool = False) -> Dict[str, Any]:
    """
    Validate a JSON-LD document provided as a nested dictionary.

    Requirements:
    - Must be a dict
    - Must contain '@context' (string | dict | list)
    - If strict=True, must also contain '@type'
    - If '@type' present: str | list[str]
    - If '@id' present: str
    - If Croissant detected in @context and strict=True, check common fields

    Returns the input dict if valid (no mutation performed).
    """
    if not isinstance(data, dict):
        raise JsonLDValidationError(
            f"JSON-LD document must be a dictionary, got {type(data)}"
        )

    if "@context" not in data:
        raise JsonLDValidationError("JSON-LD document must contain '@context' field")

    context = data["@context"]
    if not isinstance(context, (dict, str, list)):
        raise JsonLDValidationError(
            f"'@context' must be a string, object, or array, got {type(context)}"
        )

    if strict and "@type" not in data:
        raise JsonLDValidationError(
            "JSON-LD document must contain '@type' field (strict mode)"
        )

    if "@type" in data:
        type_value = data["@type"]
        if not isinstance(type_value, (str, list)):
            raise JsonLDValidationError(
                f"'@type' must be a string or array, got {type(type_value)}"
            )

    if "@id" in data:
        id_value = data["@id"]
        if not isinstance(id_value, str):
            raise JsonLDValidationError(f"'@id' must be a string, got {type(id_value)}")
        # Validate that @id is a valid UUID
        _validate_uuid(id_value, "@id")

    # Croissant detection and additional checks
    has_croissant = False
    if isinstance(context, dict):
        for v in context.values():
            if isinstance(v, str) and "croissant" in v.lower():
                has_croissant = True
                break

    if has_croissant:
        _validate_croissant_structure(data, strict)

    return data


def _validate_croissant_structure(data: Dict[str, Any], strict: bool = False) -> None:
    # Ensure dataset-ish type in strict mode
    if "@type" in data:
        t = data["@type"]
        types = [t] if isinstance(t, str) else t
        if strict and not any("Dataset" in str(tt) for tt in types):
            raise JsonLDValidationError(
                "Croissant JSON-LD must have @type containing 'Dataset' (strict)"
            )

    # Common fields in strict mode
    if strict:
        for field in ("name", "description", "distribution"):
            if field not in data:
                raise JsonLDValidationError(
                    f"Croissant dataset should contain '{field}' (strict mode)"
                )

    # Validate distribution
    if "distribution" in data:
        dist = data["distribution"]
        if not isinstance(dist, list):
            raise JsonLDValidationError("'distribution' must be an array")
        for idx, item in enumerate(dist):
            if not isinstance(item, dict):
                raise JsonLDValidationError(
                    f"distribution[{idx}] must be an object, got {type(item)}"
                )
            # Validate @id in distribution items
            if "@id" in item:
                _validate_uuid(item["@id"], f"distribution[{idx}].@id")

    # Validate recordSet
    if "recordSet" in data:
        rs = data["recordSet"]
        if not isinstance(rs, list):
            raise JsonLDValidationError("'recordSet' must be an array")
        for idx, item in enumerate(rs):
            if not isinstance(item, dict):
                raise JsonLDValidationError(
                    f"recordSet[{idx}] must be an object, got {type(item)}"
                )
            # Validate @id in recordSet items
            if "@id" in item:
                _validate_uuid(item["@id"], f"recordSet[{idx}].@id")


def convert_jsonld_to_pgjson(
    data: Dict[str, Any], *, include_context: bool = False, generate_ids: bool = True
) -> Dict[str, Any]:
    """
    Convert a JSON-LD dict into a Property Graph JSON dict.

    PG-JSON schema (simplified):
    {
      "graph": {"nodes": [...], "edges": [...]},
      "metadata": {"source_format": "JSON-LD", "node_count": n, "edge_count": m, "root_node": id}
    }
    """
    if not isinstance(data, dict):
        raise ValueError(f"Input must be a dictionary, got {type(data)}")

    converter = _JsonLDToPGJSONConverter(
        include_context=include_context, generate_ids=generate_ids
    )
    return converter.convert(data)


class _JsonLDToPGJSONConverter:
    def __init__(self, include_context: bool, generate_ids: bool) -> None:
        self.include_context = include_context
        self.generate_ids = generate_ids
        self.nodes: List[Dict[str, Any]] = []
        self.edges: List[Dict[str, Any]] = []
        self.node_ids: Set[str] = set()

    def convert(self, jsonld_data: Dict[str, Any]) -> Dict[str, Any]:
        self.nodes.clear()
        self.edges.clear()
        self.node_ids.clear()

        context = jsonld_data.get("@context", {})
        root_id = self._process_object(
            jsonld_data, context, parent_id=None, relationship=None
        )

        pg_json = {
            "graph": {
                "nodes": self.nodes,
                "edges": self.edges,
            },
            "metadata": {
                "source_format": "JSON-LD",
                "node_count": len(self.nodes),
                "edge_count": len(self.edges),
                "root_node": root_id,
            },
        }
        if self.include_context and context:
            pg_json["metadata"]["context"] = context
        return pg_json

    def _process_object(
        self,
        obj: Dict[str, Any],
        context: Union[Dict[str, Any], str, List[Any]],
        parent_id: str | None,
        relationship: str | None,
    ) -> str:
        node_id = obj.get("@id")
        if not node_id:
            if self.generate_ids:
                node_id = self._generate_id(obj)
            else:
                raise ValueError(f"Object missing @id: {obj}")

        # Reuse existing node if we've seen it
        if node_id in self.node_ids:
            if parent_id and relationship:
                self._create_edge(parent_id, node_id, relationship)
            return node_id

        self.node_ids.add(node_id)

        node_type = obj.get("@type", "Node")
        if isinstance(node_type, list):
            node_type = node_type[0] if node_type else "Node"

        properties: Dict[str, Any] = {}
        nested: List[tuple[str, Any]] = []

        for key, value in obj.items():
            if key.startswith("@"):
                # skip JSON-LD keywords in properties
                continue

            if isinstance(value, dict) and any(k.startswith("@") for k in value.keys()):
                nested.append((key, value))
            elif isinstance(value, list):
                array_objects = []
                simple_values = []
                for item in value:
                    if isinstance(item, dict) and any(
                        k.startswith("@") for k in item.keys()
                    ):
                        array_objects.append(item)
                    else:
                        simple_values.append(item)
                if array_objects:
                    nested.append((key, array_objects))
                if simple_values:
                    properties[key] = simple_values
            else:
                properties[key] = value

        node = {
            "id": node_id,
            "labels": [self._clean_type(str(node_type))],
            "properties": properties,
        }
        self.nodes.append(node)

        if parent_id and relationship:
            self._create_edge(parent_id, node_id, relationship)

        for prop_name, value in nested:
            if isinstance(value, list):
                for item in value:
                    self._process_object(
                        item, context, parent_id=node_id, relationship=prop_name
                    )
            else:
                self._process_object(
                    value, context, parent_id=node_id, relationship=prop_name
                )

        return node_id

    def _create_edge(self, from_id: str, to_id: str, relationship: str) -> None:
        edge = {
            "id": f"{from_id}_{relationship}_{to_id}",
            "type": self._clean_type(relationship),
            "source": from_id,
            "target": to_id,
            "properties": {},
        }
        self.edges.append(edge)

    def _generate_id(self, obj: Dict[str, Any]) -> str:
        """
        Generate a deterministic UUID for an object without @id.

        Uses UUID5 (SHA-1 based) with a namespace and the object's content
        to ensure consistent IDs for identical objects.
        """
        # Use JSON representation as the unique identifier
        content = json.dumps(obj, sort_keys=True)
        # Generate UUID5 from content (deterministic)
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, content))

    def _clean_type(self, t: str) -> str:
        if ":" in t:
            return t.split(":")[-1]
        if "/" in t:
            return t.split("/")[-1]
        return t
