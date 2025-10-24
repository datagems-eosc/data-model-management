"""
Tests for JSON-LD utilities.

This module contains tests for the JSON-LD validator and converter utilities.
"""

import json
import pytest
from pathlib import Path
from dmm_api.utilities import (
    validate_jsonld,
    JsonLDValidationError,
    convert_jsonld_to_pgjson,
)


class TestJsonLDValidator:
    """Tests for JSON-LD validation functionality."""

    def test_validate_simple_jsonld(self):
        """Test validation of a simple valid JSON-LD document."""
        data = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": "f4d02209-19a8-4eec-a389-826258e11461",
            "name": "Test Dataset",
        }
        result = validate_jsonld(data)
        assert result == data

    def test_validate_missing_context(self):
        """Test that validation fails when @context is missing."""
        data = {"@type": "Dataset", "name": "Test"}
        with pytest.raises(JsonLDValidationError, match="@context"):
            validate_jsonld(data)

    def test_validate_missing_type_strict(self):
        """Test that strict validation fails when @type is missing."""
        data = {"@context": "https://schema.org/", "name": "Test"}
        with pytest.raises(JsonLDValidationError, match="@type"):
            validate_jsonld(data, strict=True)

    def test_validate_missing_type_non_strict(self):
        """Test that non-strict validation passes without @type."""
        data = {"@context": "https://schema.org/", "name": "Test"}
        result = validate_jsonld(data, strict=False)
        assert result == data

    def test_validate_invalid_context_type(self):
        """Test validation fails with invalid @context type."""
        data = {"@context": 123, "@type": "Dataset"}
        with pytest.raises(
            JsonLDValidationError, match="JSON-LD to RDF conversion error"
        ):
            validate_jsonld(data)

    def test_validate_invalid_id_type(self):
        """Test validation fails with invalid @id type."""
        data = {
            "@context": {
                "@vocab": "https://schema.org/",
                "cr": "http://mlcommons.org/croissant/",
            },
            "@type": "Dataset",
            "@id": 123,
            "name": "Test",
        }
        with pytest.raises(JsonLDValidationError, match="@id' must be a string"):
            validate_jsonld(data)

    def test_validate_invalid_uuid_type(self):
        """Test validation fails with invalid @id type."""
        data = {
            "@context": {
                "@vocab": "https://schema.org/",
                "cr": "http://mlcommons.org/croissant/",
            },
            "@type": "Dataset",
            "@id": "not-a-uuid",
            "name": "Test",
        }
        with pytest.raises(JsonLDValidationError, match="@id' must be a valid UUID v4"):
            validate_jsonld(data)

    def test_validate_invalid_type_type(self):
        """Test validation fails with invalid @type type."""
        data = {"@context": "https://schema.org/", "@type": 123}
        with pytest.raises(JsonLDValidationError, match="@type"):
            validate_jsonld(data)

    def test_validate_array_type(self):
        """Test validation passes with array @type."""
        data = {
            "@context": "https://schema.org/",
            "@type": ["Dataset", "Thing"],
            "name": "Test",
        }
        result = validate_jsonld(data, strict=False)
        assert result == data

    def test_validate_no_triples(self):
        """Test validation fails with invalid @id type."""
        data = {
            "@context": "https://schema.org/",
            "@id": "2f0fab38-fb7c-4fb3-8d37-30b79b691aff",
        }
        with pytest.raises(
            JsonLDValidationError, match="JSON-LD document produced no RDF triples"
        ):
            validate_jsonld(data)

    def test_validate_complex_context(self):
        """Test validation with complex @context."""
        data = {
            "@context": {
                "@vocab": "https://schema.org/",
                "cr": "http://mlcommons.org/croissant/",
            },
            "@type": "Dataset",
            "name": "Test",
        }
        result = validate_jsonld(data, strict=False)
        assert result == data

    def test_validate_non_dict_input(self):
        """Test validation fails with non-dictionary input."""
        with pytest.raises(JsonLDValidationError, match="dictionary"):
            validate_jsonld([1, 2, 3])

    def test_validate_invalid_input_type(self):
        """Test validation fails with completely invalid input type."""
        with pytest.raises(JsonLDValidationError, match="dictionary"):
            validate_jsonld(12345)

    def test_validate_croissant_format(self):
        """Test validation of Croissant JSON-LD format."""
        data = {
            "@context": {
                "@vocab": "https://schema.org/",
                "cr": "http://mlcommons.org/croissant/",
            },
            "@type": "sc:Dataset",
            "@id": "2f0fab38-fb7c-4fb3-8d37-30b79b691aff",
            "name": "Test Dataset",
            "description": "A test dataset",
            "distribution": [
                {
                    "@type": "cr:FileObject",
                    "@id": "0ddf7adf-3ef4-4e1d-8177-416caf646267",
                    "name": "data.csv",
                }
            ],
        }
        result = validate_jsonld(data)
        assert result == data

    def test_validate_croissant_missing_fields_strict(self):
        """Test strict validation fails for incomplete Croissant format."""
        data = {
            "@context": {
                "@vocab": "https://schema.org/",
                "cr": "http://mlcommons.org/croissant/",
            },
            "@type": "Dataset",
            "name": "Test Dataset",
            # Missing 'description' and 'distribution'
        }
        with pytest.raises(JsonLDValidationError):
            validate_jsonld(data, strict=True)

    # Removed file I/O tests: json_tools operates on dictionaries only


class TestJsonLDToPGJSON:
    """Tests for JSON-LD to PG-JSON conversion."""

    def test_convert_simple_jsonld(self):
        """Test conversion of a simple JSON-LD document."""
        data = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": "3ac9f9e7-8b29-4c2a-9f4d-e5c6d7a8b9c0",
            "name": "Test Dataset",
            "description": "A test dataset",
        }
        result = convert_jsonld_to_pgjson(data)

        assert "graph" in result
        assert "nodes" in result["graph"]
        assert "edges" in result["graph"]
        assert len(result["graph"]["nodes"]) == 1

        node = result["graph"]["nodes"][0]
        assert node["id"] == "3ac9f9e7-8b29-4c2a-9f4d-e5c6d7a8b9c0"
        assert "Dataset" in node["labels"]
        assert node["properties"]["name"] == "Test Dataset"
        assert node["properties"]["description"] == "A test dataset"

    def test_convert_nested_objects(self):
        """Test conversion with nested objects."""
        data = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": "4bd0f0f8-9c3a-4d3b-8e5f-f6d7e8f9a0b1",
            "name": "Test Dataset",
            "distribution": {
                "@type": "FileObject",
                "@id": "5ce1f1f9-0d4b-4e4c-9f6g-g7e8f9g0b1c2",
                "name": "data.csv",
                "encodingFormat": "text/csv",
            },
        }
        result = convert_jsonld_to_pgjson(data)

        assert len(result["graph"]["nodes"]) == 2
        assert len(result["graph"]["edges"]) == 1

        # Check edge
        edge = result["graph"]["edges"][0]
        assert edge["source"] == "4bd0f0f8-9c3a-4d3b-8e5f-f6d7e8f9a0b1"
        assert edge["target"] == "5ce1f1f9-0d4b-4e4c-9f6g-g7e8f9g0b1c2"
        assert edge["type"] == "distribution"

    def test_convert_array_of_objects(self):
        """Test conversion with array of nested objects."""
        data = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": "dataset-1",
            "name": "Test Dataset",
            "distribution": [
                {"@type": "FileObject", "@id": "file-1", "name": "data1.csv"},
                {"@type": "FileObject", "@id": "file-2", "name": "data2.csv"},
            ],
        }
        result = convert_jsonld_to_pgjson(data)

        assert len(result["graph"]["nodes"]) == 3  # 1 dataset + 2 files
        assert len(result["graph"]["edges"]) == 2  # 2 distribution edges

    def test_convert_generate_ids(self):
        """Test ID generation for objects without @id."""
        data = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "name": "Test Dataset",
        }
        result = convert_jsonld_to_pgjson(data, generate_ids=True)

        assert len(result["graph"]["nodes"]) == 1
        node = result["graph"]["nodes"][0]
        assert node["id"] is not None
        # Check that generated ID is a valid UUID format
        import uuid

        try:
            uuid.UUID(node["id"])
            assert True  # Valid UUID
        except ValueError:
            assert False, f"Generated ID '{node['id']}' is not a valid UUID"

    def test_convert_no_generate_ids_error(self):
        """Test error when generate_ids=False and @id is missing."""
        data = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "name": "Test Dataset",
        }
        with pytest.raises(ValueError, match="missing @id"):
            convert_jsonld_to_pgjson(data, generate_ids=False)

    def test_convert_metadata(self):
        """Test that metadata is properly generated."""
        data = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": "dataset-1",
            "name": "Test Dataset",
        }
        result = convert_jsonld_to_pgjson(data)

        assert "metadata" in result
        assert result["metadata"]["source_format"] == "JSON-LD"
        assert result["metadata"]["node_count"] == 1
        assert result["metadata"]["edge_count"] == 0
        assert result["metadata"]["root_node"] == "dataset-1"

    def test_convert_include_context(self):
        """Test including context in metadata."""
        context = {"@vocab": "https://schema.org/"}
        data = {"@context": context, "@type": "Dataset", "@id": "dataset-1"}
        result = convert_jsonld_to_pgjson(data, include_context=True)

        assert "context" in result["metadata"]
        assert result["metadata"]["context"] == context

    def test_convert_simple_array_properties(self):
        """Test handling of simple array properties."""
        data = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": "dataset-1",
            "keywords": ["test", "data", "example"],
        }
        result = convert_jsonld_to_pgjson(data)

        node = result["graph"]["nodes"][0]
        assert node["properties"]["keywords"] == ["test", "data", "example"]

    def test_convert_namespace_cleaning(self):
        """Test that namespace prefixes are cleaned from types."""
        data = {
            "@context": "https://schema.org/",
            "@type": "sc:Dataset",
            "@id": "dataset-1",
        }
        result = convert_jsonld_to_pgjson(data)

        node = result["graph"]["nodes"][0]
        assert "Dataset" in node["labels"]

    def test_convert_duplicate_references(self):
        """Test handling of duplicate node references."""
        file_obj = {"@type": "FileObject", "@id": "file-1", "name": "data.csv"}
        data = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": "dataset-1",
            "distribution": [file_obj, file_obj],  # Same object referenced twice
        }
        result = convert_jsonld_to_pgjson(data)

        # Should only create one node for the file
        assert len(result["graph"]["nodes"]) == 2
        # But should create two edges
        assert len(result["graph"]["edges"]) == 2

    def test_convert_invalid_input_type(self):
        """Test conversion fails with invalid input type."""
        with pytest.raises(ValueError, match="dictionary"):
            convert_jsonld_to_pgjson(12345)

    # Removed file I/O tests: json_tools operates on dictionaries only


class TestIntegrationWithRealData:
    """Integration tests using real dataset files."""

    @pytest.fixture
    def oasa_dataset_path(self):
        """Path to the OASA test dataset."""
        return Path(__file__).parent / "dataset" / "oasa.json"

    def test_validate_real_dataset(self, oasa_dataset_path):
        """Test validation with real OASA dataset."""
        if oasa_dataset_path.exists():
            with oasa_dataset_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            result = validate_jsonld(data)
            assert "@context" in result
            assert "@type" in result
            assert "@id" in result

    def test_convert_real_dataset(self, oasa_dataset_path):
        """Test conversion with real OASA dataset."""
        if oasa_dataset_path.exists():
            with oasa_dataset_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            result = convert_jsonld_to_pgjson(data)
            assert "graph" in result
            assert len(result["graph"]["nodes"]) > 0
            # Dataset should have at least one distribution file
            assert "metadata" in result


class TestPGJSONToJsonLD:
    """Tests for PG-JSON to JSON-LD conversion."""

    def test_pgjson_to_jsonld_simple(self):
        """
        Test conversion of a simple PG-JSON graph to JSON-LD.
        TODO:
        - Create a PG-JSON dict with a single node and no edges.
        - Provide context in metadata.
        - Call convert_pgjson_to_jsonld and check output matches expected JSON-LD dict.
        - Assert @id, @type, and properties are correctly mapped.
        - Assert node id is a valid UUID v4.
        """
        from dmm_api.utilities import convert_pgjson_to_jsonld
        import uuid

        context = {"@vocab": "https://schema.org/"}
        node_id = "f4d02209-19a8-4eec-a389-826258e11461"  # valid UUID v4
        pgjson = {
            "graph": {
                "nodes": [
                    {
                        "id": node_id,
                        "labels": ["Dataset"],
                        "properties": {"name": "Test Dataset"},
                    }
                ],
                "edges": [],
            },
            "metadata": {
                "source_format": "JSON-LD",
                "node_count": 1,
                "edge_count": 0,
                "root_node": node_id,
                "context": context,
            },
        }
        expected_jsonld = {
            "@context": context,
            "@id": node_id,
            "@type": "Dataset",
            "name": "Test Dataset",
        }
        result = convert_pgjson_to_jsonld(pgjson)
        # Check that node_id is a valid UUID v4
        uuid_obj = uuid.UUID(result["@id"])
        assert uuid_obj.version == 4
        assert result == expected_jsonld

    def test_pgjson_to_jsonld_nested(self):
        """
        Test conversion of nested PG-JSON graph to JSON-LD.
        """
        from dmm_api.utilities import convert_pgjson_to_jsonld
        import uuid

        context = {"@vocab": "https://schema.org/"}
        dataset_id = "e2a1c2b3-4d5e-4f6a-8b7c-9d0e1f2a3b4c"  # valid UUID v4
        file_id = "a1b2c3d4-5e6f-7a8b-9c0d-1e2f3a4b5c6d"  # valid UUID v4
        pgjson = {
            "graph": {
                "nodes": [
                    {
                        "id": dataset_id,
                        "labels": ["Dataset"],
                        "properties": {"name": "Test Dataset"},
                    },
                    {
                        "id": file_id,
                        "labels": ["FileObject"],
                        "properties": {
                            "name": "data.csv",
                            "encodingFormat": "text/csv",
                        },
                    },
                ],
                "edges": [
                    {
                        "source": dataset_id,
                        "target": file_id,
                        "type": "distribution",
                    }
                ],
            },
            "metadata": {
                "source_format": "JSON-LD",
                "node_count": 2,
                "edge_count": 1,
                "root_node": dataset_id,
                "context": context,
            },
        }
        expected_jsonld = {
            "@context": context,
            "@id": dataset_id,
            "@type": "Dataset",
            "name": "Test Dataset",
            "distribution": {
                "@id": file_id,
                "@type": "FileObject",
                "name": "data.csv",
                "encodingFormat": "text/csv",
            },
        }
        result = convert_pgjson_to_jsonld(pgjson)
        # Check that IDs are valid UUID v4
        assert uuid.UUID(result["@id"]).version == 4
        assert "distribution" in result
        dist = result["distribution"]
        assert isinstance(dist, dict)
        assert dist["@id"] == file_id
        assert dist["@type"] == "FileObject"
        assert dist["name"] == "data.csv"
        assert dist["encodingFormat"] == "text/csv"
        # Check top-level fields
        assert result["@context"] == expected_jsonld["@context"]
        assert result["@id"] == expected_jsonld["@id"]
        assert result["@type"] == expected_jsonld["@type"]
        assert result["name"] == expected_jsonld["name"]

    def test_pgjson_to_jsonld_array_relationships(self):
        """
        Test conversion of PG-JSON with array relationships to JSON-LD.
        """
        from dmm_api.utilities import convert_pgjson_to_jsonld
        import uuid

        context = {"@vocab": "https://schema.org/"}
        dataset_id = "d93a4199-5678-4285-a5da-de40b39aba18"  # valid UUID v4
        file1_id = "54a03eb2-2f71-48e8-9d07-b1179f4bec2b"  # valid UUID v4
        file2_id = "cf63b5ba-4844-4b50-8ab3-10d40c321393"  # valid UUID v4

        pgjson = {
            "graph": {
                "nodes": [
                    {
                        "id": dataset_id,
                        "labels": ["Dataset"],
                        "properties": {"name": "Test Dataset"},
                    },
                    {
                        "id": file1_id,
                        "labels": ["FileObject"],
                        "properties": {
                            "name": "data1.csv",
                            "encodingFormat": "text/csv",
                        },
                    },
                    {
                        "id": file2_id,
                        "labels": ["FileObject"],
                        "properties": {
                            "name": "data2.csv",
                            "encodingFormat": "text/csv",
                        },
                    },
                ],
                "edges": [
                    {
                        "source": dataset_id,
                        "target": file1_id,
                        "type": "distribution",
                    },
                    {
                        "source": dataset_id,
                        "target": file2_id,
                        "type": "distribution",
                    },
                ],
            },
            "metadata": {
                "source_format": "JSON-LD",
                "node_count": 3,
                "edge_count": 2,
                "root_node": dataset_id,
                "context": context,
            },
        }

        expected_jsonld = {
            "@context": context,
            "@id": dataset_id,
            "@type": "Dataset",
            "name": "Test Dataset",
            "distribution": [
                {
                    "@id": file1_id,
                    "@type": "FileObject",
                    "name": "data1.csv",
                    "encodingFormat": "text/csv",
                },
                {
                    "@id": file2_id,
                    "@type": "FileObject",
                    "name": "data2.csv",
                    "encodingFormat": "text/csv",
                },
            ],
        }

        result = convert_pgjson_to_jsonld(pgjson)
        # Check that IDs are valid UUID v4
        assert uuid.UUID(result["@id"]).version == 4
        assert "distribution" in result
        dist = result["distribution"]
        assert isinstance(dist, list)
        assert len(dist) == 2
        ids = {d["@id"] for d in dist}
        assert file1_id in ids
        assert file2_id in ids
        # Check each file object
        for d in dist:
            assert d["@type"] == "FileObject"
            assert d["encodingFormat"] == "text/csv"
            assert d["name"] in {"data1.csv", "data2.csv"}
        # Check top-level fields
        assert result["@context"] == expected_jsonld["@context"]
        assert result["@id"] == expected_jsonld["@id"]
        assert result["@type"] == expected_jsonld["@type"]
        assert result["name"] == expected_jsonld["name"]

    def test_pgjson_to_jsonld_context_argument(self):
        """
        Test context provided as argument overrides metadata context.
        """
        from dmm_api.utilities import convert_pgjson_to_jsonld
        import uuid

        context_meta = {"@vocab": "https://schema.org/"}
        context_arg = {"@vocab": "https://example.org/"}
        node_id = "f4d02209-19a8-4eec-a389-826258e11461"  # valid UUID v4
        pgjson = {
            "graph": {
                "nodes": [
                    {
                        "id": node_id,
                        "labels": ["Dataset"],
                        "properties": {"name": "Test Dataset"},
                    }
                ],
                "edges": [],
            },
            "metadata": {
                "source_format": "JSON-LD",
                "node_count": 1,
                "edge_count": 0,
                "root_node": node_id,
                "context": context_meta,
            },
        }
        result = convert_pgjson_to_jsonld(pgjson, context=context_arg)
        uuid_obj = uuid.UUID(result["@id"])
        assert result["@id"] == node_id
        assert uuid_obj.version == 4
        assert result["@type"] == "Dataset"
        assert result["name"] == "Test Dataset"
        assert result["@context"] == context_arg

    def test_pgjson_to_jsonld_missing_context(self):
        """
        Test error when no context is provided in argument or metadata.
        """
        from dmm_api.utilities import convert_pgjson_to_jsonld

        node_id = "f4d02209-19a8-4eec-a389-826258e11461"  # valid UUID v4
        pgjson = {
            "graph": {
                "nodes": [
                    {
                        "id": node_id,
                        "labels": ["Dataset"],
                        "properties": {"name": "Test Dataset"},
                    }
                ],
                "edges": [],
            },
            "metadata": {
                "source_format": "JSON-LD",
                "node_count": 1,
                "edge_count": 0,
                "root_node": node_id,
                # No context
            },
        }
        import pytest

        with pytest.raises(ValueError, match="No context provided"):
            convert_pgjson_to_jsonld(pgjson)

    def test_pgjson_to_jsonld_infer_root_node(self):
        """
        Test PG-JSON to JSON-LD conversion when no root_node is provided in metadata,
        and only one node has no incoming edges (should infer root).
        """
        from dmm_api.utilities import convert_pgjson_to_jsonld

        context = {"@vocab": "https://schema.org/"}
        dataset_id = "e2a1c2b3-4d5e-4f6a-8b7c-9d0e1f2a3b4c"  # valid UUID v4
        file_id = "a1b2c3d4-5e6f-7a8b-9c0d-1e2f3a4b5c6d"  # valid UUID v4
        pgjson = {
            "graph": {
                "nodes": [
                    {
                        "id": dataset_id,
                        "labels": ["Dataset"],
                        "properties": {"name": "Test Dataset"},
                    },
                    {
                        "id": file_id,
                        "labels": ["FileObject"],
                        "properties": {
                            "name": "data.csv",
                            "encodingFormat": "text/csv",
                        },
                    },
                ],
                "edges": [
                    {
                        "source": dataset_id,
                        "target": file_id,
                        "type": "distribution",
                    }
                ],
            },
            "metadata": {
                "source_format": "JSON-LD",
                "node_count": 2,
                "edge_count": 1,
                # No root_node provided
                "context": context,
            },
        }
        result = convert_pgjson_to_jsonld(pgjson)
        # Should infer dataset_id as root
        assert result["@id"] == dataset_id
        assert result["@context"] == context
        assert result["@type"] == "Dataset"
        assert result["name"] == "Test Dataset"
        dist = result["distribution"]
        assert isinstance(dist, dict)
        assert dist["@id"] == file_id
        assert dist["@type"] == "FileObject"
        assert dist["name"] == "data.csv"
        assert dist["encodingFormat"] == "text/csv"

    def test_pgjson_to_jsonld_ambiguous_root(self):
        """
        Test error when root node is missing in metadata and multiple nodes have no incoming edges.
        """
        from dmm_api.utilities import convert_pgjson_to_jsonld
        import pytest

        context = {"@vocab": "https://schema.org/"}
        node1_id = "3aaf5bbd-deff-4a0b-a72e-0b36bff8c813"  # valid UUID v4
        node2_id = "2d4d2143-bb51-42a8-b06b-c47b15c55994"  # valid UUID v4
        file_id = "395ead2c-3ea5-41de-93fe-3ca1bcbba147"  # valid UUID v4
        pgjson = {
            "graph": {
                "nodes": [
                    {
                        "id": node1_id,
                        "labels": ["Dataset"],
                        "properties": {"name": "Dataset 1"},
                    },
                    {
                        "id": node2_id,
                        "labels": ["Dataset"],
                        "properties": {"name": "Dataset 2"},
                    },
                    {
                        "id": file_id,
                        "labels": ["FileObject"],
                        "properties": {
                            "name": "data.csv",
                            "encodingFormat": "text/csv",
                        },
                    },
                ],
                "edges": [
                    {
                        "source": node1_id,
                        "target": file_id,
                        "type": "distribution",
                    },
                ],
            },
            "metadata": {
                "source_format": "JSON-LD",
                "node_count": 3,
                "edge_count": 1,
                # No root_node provided
                "context": context,
            },
        }
        # Both node1_id and node2_id have no incoming edges
        with pytest.raises(ValueError, match="Ambiguous root node"):
            convert_pgjson_to_jsonld(pgjson)

    def test_pgjson_to_jsonld_round_trip(self):
        """
        Test round-trip conversion: JSON-LD -> PG-JSON -> JSON-LD.
        """
        from dmm_api.utilities import convert_jsonld_to_pgjson, convert_pgjson_to_jsonld
        import uuid

        context = {"@vocab": "https://schema.org/"}
        dataset_id = "e2a1c2b3-4d5e-4f6a-8b7c-9d0e1f2a3b4c"  # valid UUID v4
        file_id = "a1b2c3d4-5e6f-7a8b-9c0d-1e2f3a4b5c6d"  # valid UUID v4
        jsonld = {
            "@context": context,
            "@id": dataset_id,
            "@type": "Dataset",
            "name": "Test Dataset",
            "distribution": {
                "@id": file_id,
                "@type": "FileObject",
                "name": "data.csv",
                "encodingFormat": "text/csv",
            },
        }
        # Validate input JSON-LD
        uuid_obj = uuid.UUID(jsonld["@id"])
        assert uuid_obj.version == 4
        # Convert to PG-JSON
        pgjson = convert_jsonld_to_pgjson(jsonld, include_context=True)
        # Convert back to JSON-LD
        result = convert_pgjson_to_jsonld(pgjson)
        # Check round-trip equivalence (allowing for ordering differences)
        assert result["@id"] == jsonld["@id"]
        assert result["@type"] == jsonld["@type"]
        assert result["@context"] == jsonld["@context"]
        assert result["name"] == jsonld["name"]
        dist = result["distribution"]
        orig_dist = jsonld["distribution"]
        assert dist["@id"] == orig_dist["@id"]
        assert dist["@type"] == orig_dist["@type"]
        assert dist["name"] == orig_dist["name"]
        assert dist["encodingFormat"] == orig_dist["encodingFormat"]
