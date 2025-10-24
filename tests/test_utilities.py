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
        TODO:
        - Create a PG-JSON dict with a root node and one or more child nodes connected by edges.
        - Use relationships to represent nested objects (e.g., distribution).
        - Call convert_pgjson_to_jsonld and check nested structure in output JSON-LD.
        - Assert nested objects are reconstructed as dicts under the correct property.
        """
        pass

    def test_pgjson_to_jsonld_array_relationships(self):
        """
        Test conversion of PG-JSON with array relationships to JSON-LD.
        TODO:
        - Create a PG-JSON dict where the root node has multiple edges of the same type to different nodes.
        - Call convert_pgjson_to_jsonld and check that the property is an array of objects in JSON-LD.
        - Assert all related nodes are present in the array property.
        """
        pass

    def test_pgjson_to_jsonld_context_argument(self):
        """
        Test context provided as argument overrides metadata context.
        TODO:
        - Create a PG-JSON dict with context in metadata.
        - Call convert_pgjson_to_jsonld with a different context argument.
        - Assert that the output JSON-LD uses the argument context, not the metadata context.
        """
        pass

    def test_pgjson_to_jsonld_missing_context(self):
        """
        Test error when no context is provided in argument or metadata.
        TODO:
        - Create a PG-JSON dict with no context in metadata.
        - Call convert_pgjson_to_jsonld with no context argument.
        - Assert that ValueError is raised for missing context.
        """
        pass

    def test_pgjson_to_jsonld_missing_root(self):
        """
        Test error when root node is missing in metadata.
        TODO:
        - Create a PG-JSON dict with no root_node in metadata.
        - Call convert_pgjson_to_jsonld and assert an error is raised (ValueError or KeyError).
        """
        pass

    def test_pgjson_to_jsonld_round_trip(self):
        """
        Test round-trip conversion: JSON-LD -> PG-JSON -> JSON-LD.
        TODO:
        - Create a sample JSON-LD dict.
        - Convert to PG-JSON using convert_jsonld_to_pgjson.
        - Convert back to JSON-LD using convert_pgjson_to_jsonld.
        - Assert that the result matches the original JSON-LD (allowing for minor differences in ordering or type normalization).
        """
        pass
