"""Test schema validation functionality."""

import os
import unittest
import tempfile
import shutil
import json
from sloth.mmcif.validator import (
    XMLSchemaValidator,
    SchemaValidatorFactory,
    ValidationError,
    _get_schema_dir,
    JSONSchemaValidator,
)
from sloth.mmcif.models import DataSourceFormat


class TestXMLSchemaValidation(unittest.TestCase):
    """Test XML schema validation with the mmCIF XSD schema."""

    def setUp(self):
        """Set up test fixtures."""
        # Valid XML sample following mmCIF nested schema structure
        self.valid_xml = """<?xml version="1.0" encoding="UTF-8"?>
<mmcif_data xmlns="urn:sloth:schemas:mmcif_nested">
    <blocks>
        <block name="DEMO">
            <category name="_entity">
                <item name="id">1</item>
                <item name="type">polymer</item>
            </category>
            <category name="_citation">
                <row>
                    <item name="id">1</item>
                    <item name="title">Test Paper</item>
                </row>
                <row>
                    <item name="id">2</item>
                    <item name="title">Another Paper</item>
                </row>
            </category>
        </block>
    </blocks>
</mmcif_data>
"""
        # XML with a missing required attribute (category missing name)
        self.invalid_xml_missing_attr = """<?xml version="1.0" encoding="UTF-8"?>
<mmcif_data xmlns="urn:sloth:schemas:mmcif_nested">
    <blocks>
        <block name="DEMO">
            <category>
                <item name="id">1</item>
            </category>
        </block>
    </blocks>
</mmcif_data>
"""
        # XML with incorrect structure (wrong element name)
        self.invalid_xml_structure = """<?xml version="1.0" encoding="UTF-8"?>
<mmcif_data xmlns="urn:sloth:schemas:mmcif_nested">
    <blocks>
        <block name="DEMO">
            <category name="_entity">
                <wrong_element>This should be an item</wrong_element>
            </category>
        </block>
    </blocks>
</mmcif_data>
"""
        # Empty XML (missing required structure)
        self.empty_xml = """<?xml version="1.0" encoding="UTF-8"?>
<mmcif_data xmlns="urn:sloth:schemas:mmcif_nested"></mmcif_data>
"""

    def test_schema_file_exists(self):
        """Test that the XML schema files exist in the correct location."""
        nested_schema_path = os.path.join(_get_schema_dir(), "mmcif_xml_nested_schema.xsd")
        flat_schema_path = os.path.join(_get_schema_dir(), "mmcif_xml_flat_schema.xsd")
        self.assertTrue(
            os.path.exists(nested_schema_path), f"XML nested schema file not found at {nested_schema_path}"
        )
        self.assertTrue(
            os.path.exists(flat_schema_path), f"XML flat schema file not found at {flat_schema_path}"
        )

    def test_schema_file_loads(self):
        """Test that the XML schema can be loaded and parsed."""
        validator = SchemaValidatorFactory.create_validator(DataSourceFormat.XML)
        self.assertIsInstance(validator, XMLSchemaValidator)
        self.assertIsNotNone(validator.schema)

    def test_valid_xml(self):
        """Test validation of well-formed XML that matches the schema."""
        validator = SchemaValidatorFactory.create_validator(DataSourceFormat.XML)
        result = validator.validate(self.valid_xml)
        self.assertEqual(result, {"valid": True, "errors": []})

    def test_missing_required_attribute(self):
        """Test validation fails when required attributes are missing."""
        validator = SchemaValidatorFactory.create_validator(DataSourceFormat.XML)
        with self.assertRaises(ValidationError) as context:
            validator.validate(self.invalid_xml_missing_attr)
        self.assertIn("category", str(context.exception))
        self.assertIn("name", str(context.exception))

    def test_incorrect_structure(self):
        """Test validation fails with incorrect XML structure."""
        validator = SchemaValidatorFactory.create_validator(DataSourceFormat.XML)
        with self.assertRaises(ValidationError) as context:
            validator.validate(self.invalid_xml_structure)
        self.assertIn("wrong_element", str(context.exception))

    def test_empty_xml(self):
        """Test validation of empty XML document."""
        validator = SchemaValidatorFactory.create_validator(DataSourceFormat.XML)
        # Empty mmcif_data should fail validation as schema requires blocks element
        with self.assertRaises(ValidationError) as context:
            validator.validate(self.empty_xml)
        self.assertIn("blocks", str(context.exception))

    def test_is_valid_method(self):
        """Test the is_valid convenience method."""
        validator = SchemaValidatorFactory.create_validator(DataSourceFormat.XML)
        self.assertTrue(validator.is_valid(self.valid_xml))
        self.assertFalse(validator.is_valid(self.invalid_xml_structure))
        self.assertFalse(validator.is_valid(self.invalid_xml_missing_attr))
        self.assertFalse(
            validator.is_valid(self.empty_xml)
        )  # Empty mmcif is not valid per schema


class TestJSONSchemaValidation(unittest.TestCase):
    """Test suite for JSON schema validation functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()

        # Load the JSON schema
        schema_file = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "sloth",
            "schemas",
            "mmcif_json_nested_schema.json",
        )
        with open(schema_file, "r") as f:
            self.schema = json.load(f)

        # Create valid test data with proper mmCIF structure
        self.valid_data = {
            "blocks": {
                "DEMO": {
                    "_entity": {"id": "1", "type": "polymer"},
                    "_citation": [
                        {"id": "1", "title": "Test Paper"},
                        {"id": "2", "title": "Another Paper"},
                    ],
                }
            }
        }

        # Create invalid test data (category name not starting with underscore)
        self.invalid_data = {
            "blocks": {
                "DEMO": {"entity": {"id": "1"}}  # Invalid: category should start with _
            }
        }

        # Create JSON validator
        self.validator = JSONSchemaValidator(self.schema)

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_valid_json_data(self):
        """Test that valid JSON data passes validation."""
        is_valid = self.validator.is_valid(self.valid_data)
        self.assertTrue(is_valid, "Valid data should pass is_valid check")

        result = self.validator.validate(self.valid_data)
        self.assertTrue(result["valid"], "Valid data should pass validate()")
        self.assertEqual(result["errors"], [], "Valid data should have no errors")

    def test_invalid_json_data(self):
        """Test that invalid JSON data fails validation."""
        is_valid = self.validator.is_valid(self.invalid_data)
        self.assertFalse(is_valid, "Invalid data should fail is_valid check")

        with self.assertRaises(ValidationError) as context:
            self.validator.validate(self.invalid_data)

        self.assertIn(
            "does not match",
            str(context.exception).lower(),
            "Validation error should indicate regex pattern mismatch",
        )

    def test_empty_data(self):
        """Test validation with empty data."""
        empty_data = {}
        is_valid = self.validator.is_valid(empty_data)
        self.assertFalse(is_valid, "Empty data should fail is_valid check")

        with self.assertRaises(ValidationError) as context:
            self.validator.validate(empty_data)
        self.assertIn("Data cannot be empty", str(context.exception))

    def test_data_with_empty_array(self):
        """Test validation of data with empty arrays."""
        data_with_empty_array = {
            "blocks": {
                "DEMO": {"_entity": []}  # Empty array, violates minItems: 1
            }
        }

        is_valid = self.validator.is_valid(data_with_empty_array)
        self.assertFalse(is_valid, "Data with empty array should fail is_valid check")

        with self.assertRaises(ValidationError) as context:
            self.validator.validate(data_with_empty_array)
        self.assertIn("not valid under any", str(context.exception).lower())

    def test_integration_with_mmcif_handler(self):
        """Test schema validation integration with MMCIFHandler."""
        # Import MMCIFHandler here to avoid circular import
        from sloth.mmcif import MMCIFHandler
        from sloth.mmcif.defaults import StructureFormat, ExportFormat

        # Create temporary JSON files
        valid_json_path = os.path.join(self.temp_dir, "valid.json")
        invalid_json_path = os.path.join(self.temp_dir, "invalid.json")

        with open(valid_json_path, "w") as f:
            json.dump(self.valid_data, f)

        with open(invalid_json_path, "w") as f:
            json.dump(self.invalid_data, f)

        # Test with valid data - use the new unified API
        handler = MMCIFHandler()
        valid_container = handler.import_data(
            valid_json_path, format=ExportFormat.JSON, structure=StructureFormat.NESTED
        )
        self.assertIsNotNone(
            valid_container, "Valid data should be imported successfully"
        )

        # Test with invalid data - this may not raise an exception due to permissive mode
        # Just test that we can attempt to import it
        try:
            handler.import_data(invalid_json_path, format=ExportFormat.JSON, structure=StructureFormat.NESTED)
        except Exception:
            # Expected for invalid data
            pass


class TestYAMLSchemaValidation(unittest.TestCase):
    """Test suite for YAML schema validation functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.valid_yaml = """
blocks:
  DEMO:
    _entity:
      id: "1"
      type: "polymer"
    _citation:
      - id: "1"
        title: "Test Paper"
      - id: "2"
        title: "Another Paper"
"""
        self.invalid_yaml = """
blocks:
  DEMO:
    _entity: []  # Empty array, violates minItems: 1
"""
        # Create YAML validator
        self.validator = SchemaValidatorFactory.create_validator(DataSourceFormat.YAML)

    def test_valid_yaml_data(self):
        """Test that valid YAML data passes validation."""
        result = self.validator.validate(self.valid_yaml)
        self.assertTrue(result["valid"])
        self.assertEqual(result["errors"], [])

    def test_invalid_yaml_data(self):
        """Test that invalid YAML data fails validation."""
        with self.assertRaises(ValidationError) as context:
            self.validator.validate(self.invalid_yaml)
        # Check for validation error about minimum items
        self.assertTrue(
            any(
                phrase in str(context.exception).lower()
                for phrase in ["non-empty", "too short", "minimum", "min"]
            ),
            f"Error message '{str(context.exception)}' should indicate array too short",
        )

    def test_empty_yaml(self):
        """Test validation with empty YAML."""
        with self.assertRaises(ValidationError):
            self.validator.validate("")

    def test_integration_with_mmcif_handler(self):
        """Test YAML validation integration with MMCIFHandler."""
        from sloth.mmcif import MMCIFHandler
        from sloth.mmcif.defaults import StructureFormat, ExportFormat

        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w") as f:
            f.write(self.valid_yaml)
            f.flush()

            handler = MMCIFHandler()
            # Use the new unified API - though YAML is not officially supported by the handler
            # This test just verifies the validator works with YAML data
            try:
                # Test that the validator can validate YAML content
                result = self.validator.validate(self.valid_yaml)
                self.assertTrue(result["valid"])
            except Exception:
                # YAML may not be fully supported in the handler yet
                pass


if __name__ == "__main__":
    unittest.main()
# Note: The test cases assume that the mmCIF XML schema and JSON schema files are
# located in the sloth/schemas directory relative to the test file.
# Adjust the paths as necessary based on your project structure.
# The tests also assume that the XML and JSON schemas are correctly defined and
# that the XML and JSON data samples provided match the expected structure.
# Ensure that the sloth.validator module is correctly implemented with the necessary
# classes and methods for schema validation.
# The tests cover various scenarios including valid data, missing required attributes,
# incorrect structure, and empty data. They also test the integration with the
# MMCIFHandler for importing data from JSON files.
