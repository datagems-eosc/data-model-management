# DMM API Utilities (json_tools)

This package exposes a single module, `json_tools`, with two pure functions that operate on in-memory Python dictionaries (no file I/O):

1. `validate_jsonld(data: dict, strict: bool = False) -> dict`
  - Validates JSON-LD documents for correctness (presence and types of `@context`, `@type`, etc.)
  - Supports optional strict validation and basic Croissant-specific checks

2. `convert_jsonld_to_pgjson(data: dict, *, include_context: bool = False, generate_ids: bool = True) -> dict`
  - Converts JSON-LD documents to Property Graph JSON format (nodes/edges + metadata)
  - Accepts only dictionaries; callers should load JSON themselves

## Usage

These utilities are part of the `dmm_api` package. Import them like so:

```python
from dmm_api.utilities import validate_jsonld, JsonLDValidationError, convert_jsonld_to_pgjson
```

## JSON-LD Validator

### Basic Usage

```python
from dmm_api.utilities import validate_jsonld, JsonLDValidationError

# Validate from a dictionary
data = {
    "@context": "https://schema.org/",
    "@type": "Dataset",
    "@id": "my-dataset",
    "name": "My Dataset"
}

try:
    validated = validate_jsonld(data)
    print("Valid JSON-LD!")
except JsonLDValidationError as e:
    print(f"Validation error: {e}")

# Load the JSON yourself if it comes from a file
import json
with open("path/to/dataset.json", "r", encoding="utf-8") as f:
  validated = validate_jsonld(json.load(f))
```

### Validation Modes

The validator supports two modes:

- **Non-strict (default)**: Checks for required `@context` field and validates types
- **Strict**: Additionally requires `@type` field and enforces Croissant-specific rules

```python
# Strict validation
validate_jsonld(data, strict=True)
```

### What is Validated

The validator checks:

- ✓ Presence of required `@context` field
- ✓ Valid types for `@context` (string, object, or array)
- ✓ Valid types for `@type` (string or array)
- ✓ Valid type for `@id` (string)
- ✓ Croissant format compliance (when detected)
- ✓ Distribution and recordSet structure

## JSON-LD to PG-JSON Converter

### Basic Usage

```python
from dmm_api.utilities import convert_jsonld_to_pgjson
# Convert from dictionary
jsonld_data = {
    "@context": "https://schema.org/",
    "@type": "Dataset",
    "@id": "dataset-1",
    "name": "My Dataset",
    "distribution": {
        "@type": "FileObject",
        "@id": "file-1",
        "name": "data.csv"
    }
}

pg_json = convert_jsonld_to_pgjson(jsonld_data)

# Access the graph
print(f"Nodes: {len(pg_json['graph']['nodes'])}")
print(f"Edges: {len(pg_json['graph']['edges'])}")

# Save to file (caller responsibility)
import json
with open("output.pgjson", "w", encoding="utf-8") as f:
  json.dump(pg_json, f, ensure_ascii=False, indent=2)
```

### Conversion Options

```python
pg_json = convert_jsonld_to_pgjson(data, include_context=True)  # include @context in metadata

# Don't auto-generate IDs (will raise error if @id is missing)
pg_json = convert_jsonld_to_pgjson(data, generate_ids=False)
```

### PG-JSON Format

The converter produces a Property Graph JSON structure with:

```json
{
  "graph": {
    "nodes": [
      {
        "id": "dataset-1",
        "labels": ["Dataset"],
        "properties": {
          "name": "My Dataset"
        }
      },
      {
        "id": "file-1",
        "labels": ["FileObject"],
        "properties": {
          "name": "data.csv"
        }
      }
    ],
    "edges": [
      {
        "id": "dataset-1_distribution_file-1",
        "type": "distribution",
        "source": "dataset-1",
        "target": "file-1",
        "properties": {}
      }
    ]
  },
  "metadata": {
    "source_format": "JSON-LD",
    "node_count": 2,
    "edge_count": 1,
    "root_node": "dataset-1"
  }
}
```

### Conversion Rules

- **Nodes**: Created from objects with `@type` or `@id`
- **Edges**: Created from relationships between objects (nested objects or references)
- **Properties**: Simple key-value pairs from the JSON-LD object
- **Labels**: Derived from `@type` field (namespace prefixes removed)
- **IDs**: Use `@id` if present, otherwise auto-generate from content

## Examples

Examples can be found in the tests under `tests/test_utilities.py`.

## Use Cases

### 1. API Input Validation

```python
from fastapi import HTTPException
from dmm_api.utilities import validate_jsonld, JsonLDValidationError

@app.post("/api/v1/dataset/register")
async def register_dataset(data: dict):
    try:
        validated_data = validate_jsonld(data, strict=True)
        # Process validated data...
    except JsonLDValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
```

### 2. Graph Database Import

```python
from dmm_api.utilities import convert_jsonld_to_pgjson

# Convert dataset to graph format for Neo4j, etc.
pg_json = convert_jsonld_to_pgjson("dataset.json")

# Extract nodes and edges for import
nodes = pg_json["graph"]["nodes"]
edges = pg_json["graph"]["edges"]

# Import to graph database...
```

### 3. Dataset Analysis

```python
# Analyze dataset structure
pg_json = convert_jsonld_to_pgjson("dataset.json")

print(f"Dataset has {pg_json['metadata']['node_count']} entities")
print(f"with {pg_json['metadata']['edge_count']} relationships")

# Find all FileObjects
files = [n for n in pg_json["graph"]["nodes"]
         if "FileObject" in n["labels"]]
print(f"Contains {len(files)} data files")
```

## Testing

Run the test suite (examples embedded):

```bash
pytest tests/test_utilities.py -v
```

## Error Handling

### JsonLDValidationError

Raised when JSON-LD validation fails:

```python
from dmm_api.utilities import JsonLDValidationError

try:
    validate_jsonld(invalid_data)
except JsonLDValidationError as e:
    print(f"Validation failed: {e}")
    # Handle validation error...
```

### FileNotFoundError

Raised when a file path doesn't exist:

```python
try:
    validate_jsonld("nonexistent.json")
except FileNotFoundError as e:
    print(f"File not found: {e}")
```

### ValueError

Raised for invalid input types or missing required IDs:

```python
try:
    convert_jsonld_to_pgjson(data, generate_ids=False)
except ValueError as e:
    print(f"Missing @id: {e}")
```

## API Reference

### `validate_jsonld(data, strict=False)`

Validate a JSON-LD document.

**Parameters:**
- `data` (str | Path | dict): JSON-LD data or file path
- `strict` (bool): Enable strict validation mode

**Returns:** `dict` - Validated JSON-LD document

**Raises:** `JsonLDValidationError`, `FileNotFoundError`, `json.JSONDecodeError`

### `convert_jsonld_to_pgjson(data, include_context=False, generate_ids=True)`

Convert JSON-LD to PG-JSON format.

**Parameters:**
- `data` (str | Path | dict): JSON-LD data or file path
- `include_context` (bool): Include @context in metadata
- `generate_ids` (bool): Auto-generate missing node IDs

**Returns:** `dict` - PG-JSON document

**Raises:** `ValueError`, `FileNotFoundError`, `json.JSONDecodeError`

### `save_pgjson(pg_json, output_path)`

Save PG-JSON to a file.

**Parameters:**
- `pg_json` (dict): PG-JSON data
- `output_path` (str | Path): Output file path

## Contributing

When adding new utilities:

1. Add the utility module in `dmm_api/utilities/`
2. Export main functions in `__init__.py`
3. Add comprehensive tests in `tests/test_utilities.py`
4. Update this README with usage examples
5. Run pre-commit hooks and tests before committing

## License

Same as the main dmm-api project.
