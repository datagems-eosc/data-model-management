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
            "@id": "test-dataset",
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
        with pytest.raises(JsonLDValidationError, match="@context"):
            validate_jsonld(data)

    def test_validate_invalid_type_type(self):
        """Test validation fails with invalid @type type."""
        data = {"@context": "https://schema.org/", "@type": 123}
        with pytest.raises(JsonLDValidationError, match="@type"):
            validate_jsonld(data)

    def test_validate_invalid_id_type(self):
        """Test validation fails with invalid @id type."""
        data = {"@context": "https://schema.org/", "@id": 123}
        with pytest.raises(JsonLDValidationError, match="@id"):
            validate_jsonld(data)

    def test_validate_array_type(self):
        """Test validation passes with array @type."""
        data = {
            "@context": "https://schema.org/",
            "@type": ["Dataset", "Thing"],
            "name": "Test",
        }
        result = validate_jsonld(data)
        assert result == data

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
        result = validate_jsonld(data)
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
            "@id": "test-dataset",
            "name": "Test Dataset",
            "description": "A test dataset",
            "distribution": [
                {
                    "@type": "cr:FileObject",
                    "@id": "file-1",
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
            "@id": "dataset-1",
            "name": "Test Dataset",
            "description": "A test dataset",
        }
        result = convert_jsonld_to_pgjson(data)

        assert "graph" in result
        assert "nodes" in result["graph"]
        assert "edges" in result["graph"]
        assert len(result["graph"]["nodes"]) == 1

        node = result["graph"]["nodes"][0]
        assert node["id"] == "dataset-1"
        assert "Dataset" in node["labels"]
        assert node["properties"]["name"] == "Test Dataset"
        assert node["properties"]["description"] == "A test dataset"

    def test_convert_nested_objects(self):
        """Test conversion with nested objects."""
        data = {
            "@context": "https://schema.org/",
            "@type": "Dataset",
            "@id": "dataset-1",
            "name": "Test Dataset",
            "distribution": {
                "@type": "FileObject",
                "@id": "file-1",
                "name": "data.csv",
                "encodingFormat": "text/csv",
            },
        }
        result = convert_jsonld_to_pgjson(data)

        assert len(result["graph"]["nodes"]) == 2
        assert len(result["graph"]["edges"]) == 1

        # Check edge
        edge = result["graph"]["edges"][0]
        assert edge["source"] == "dataset-1"
        assert edge["target"] == "file-1"
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
        assert "Dataset_Test Dataset" in node["id"]

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
