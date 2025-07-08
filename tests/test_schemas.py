"""Test schema validation functionality."""

import os
import unittest
import tempfile
import shutil
import json
from sloth.validators import (
    XMLSchemaValidator,
    SchemaValidatorFactory,
    ValidationError,
    _get_schema_dir,
    JSONSchemaValidator,
)
from sloth.models import DataSourceFormat


class TestXMLSchemaValidation(unittest.TestCase):
    """Test XML schema validation with the mmCIF XSD schema."""

    def setUp(self):
        """Set up test fixtures."""
        # Valid XML sample following mmCIF structure
        self.valid_xml = """<?xml version="1.0" encoding="UTF-8"?>
<mmcif>
    <data_block name="test">
        <category name="_database_2">
            <item name="database_id">PDB</item>
            <item name="database_code">TEST</item>
        </category>
        <category name="_atom_site">
            <row>
                <item name="group_PDB">ATOM</item>
                <item name="id">1</item>
                <item name="type_symbol">N</item>
                <item name="Cartn_x">10.123</item>
                <item name="Cartn_y">20.456</item>
                <item name="Cartn_z">30.789</item>
            </row>
        </category>
    </data_block>
</mmcif>
"""
        # XML with a missing required attribute
        self.invalid_xml_missing_attr = """<?xml version="1.0" encoding="UTF-8"?>
<mmcif>
    <data_block>
        <category name="_database_2">
            <item name="database_id">PDB</item>
        </category>
    </data_block>
</mmcif>
"""
        # XML with incorrect structure
        self.invalid_xml_structure = """<?xml version="1.0" encoding="UTF-8"?>
<mmcif>
    <data_block name="test">
        <category name="_database_2">
            <wrong_element>This should be an item</wrong_element>
        </category>
    </data_block>
</mmcif>
"""
        # Empty XML
        self.empty_xml = """<?xml version="1.0" encoding="UTF-8"?>
<mmcif></mmcif>
"""

    def test_schema_file_exists(self):
        """Test that the XML schema file exists in the correct location."""
        schema_path = os.path.join(_get_schema_dir(), "mmcif_xml_schema.xsd")
        self.assertTrue(
            os.path.exists(schema_path), f"XML schema file not found at {schema_path}"
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
        self.assertIn("data_block", str(context.exception))
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
        # Empty mmcif should fail validation as schema requires at least one data_block
        with self.assertRaises(ValidationError) as context:
            validator.validate(self.empty_xml)
        self.assertIn("data_block", str(context.exception))

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
            "mmcif_json_schema.json",
        )
        with open(schema_file, "r") as f:
            self.schema = json.load(f)

        # Create valid test data
        self.valid_data = {
            "block1": {
                "_category1": {"item1": "value1", "item2": 123},
                "_category2": [
                    {"col1": "row1", "col2": 456},
                    {"col1": "row2", "col2": 789},
                ],
            }
        }

        # Create invalid test data (empty category)
        self.invalid_data = {
            "block1": {"_category1": {}}  # Empty category, should fail validation
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
            "is not valid",
            str(context.exception),
            "Validation error should indicate invalid data",
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
            "block1": {"_category1": []}  # Empty array, should fail validation
        }

        is_valid = self.validator.is_valid(data_with_empty_array)
        self.assertFalse(is_valid, "Data with empty array should fail is_valid check")

        with self.assertRaises(ValidationError) as context:
            self.validator.validate(data_with_empty_array)
        self.assertIn("is not valid", str(context.exception))

    def test_integration_with_mmcif_handler(self):
        """Test schema validation integration with MMCIFHandler."""
        # Import MMCIFHandler here to avoid circular import
        from sloth import MMCIFHandler

        # Create temporary JSON files
        valid_json_path = os.path.join(self.temp_dir, "valid.json")
        invalid_json_path = os.path.join(self.temp_dir, "invalid.json")

        with open(valid_json_path, "w") as f:
            json.dump(self.valid_data, f)

        with open(invalid_json_path, "w") as f:
            json.dump(self.invalid_data, f)

        # Test with valid data
        handler = MMCIFHandler()
        valid_container = handler.import_from_json(
            valid_json_path, schema_validator=self.validator
        )
        self.assertIsNotNone(
            valid_container, "Valid data should be imported successfully"
        )

        # Test with invalid data
        with self.assertRaises(ValidationError) as context:
            handler.import_from_json(invalid_json_path, schema_validator=self.validator)

        self.assertIn(
            "is not valid",
            str(context.exception),
            "Importing invalid data should raise ValidationError",
        )


class TestYAMLSchemaValidation(unittest.TestCase):
    """Test suite for YAML schema validation functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.valid_yaml = """
block1:
    _category1:
        item1: value1
        item2: 123
    _category2:
        - col1: row1
          col2: 456
        - col1: row2
          col2: 789
"""
        self.invalid_yaml = """
block1:
    _category1: []  # Empty category, should fail validation
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
        # The exact error message may vary depending on the validation library
        # but should indicate that the empty array is invalid
        self.assertTrue(
            any(
                phrase in str(context.exception)
                for phrase in ["is too short", "is not valid", "empty array"]
            ),
            f"Error message '{str(context.exception)}' should indicate invalid data",
        )

    def test_empty_yaml(self):
        """Test validation with empty YAML."""
        with self.assertRaises(ValidationError):
            self.validator.validate("")

    def test_integration_with_mmcif_handler(self):
        """Test YAML validation integration with MMCIFHandler."""
        from sloth import MMCIFHandler

        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w") as f:
            f.write(self.valid_yaml)
            f.flush()

            handler = MMCIFHandler()
            valid_container = handler.import_from_yaml(
                f.name, schema_validator=self.validator
            )
            self.assertIsNotNone(valid_container)


if __name__ == "__main__":
    unittest.main()
# Note: The test cases assume that the mmCIF XML schema and JSON schema files are
# located in the sloth/schemas directory relative to the test file.
# Adjust the paths as necessary based on your project structure.
# The tests also assume that the XML and JSON schemas are correctly defined and
# that the XML and JSON data samples provided match the expected structure.
# Ensure that the sloth.validators module is correctly implemented with the necessary
# classes and methods for schema validation.
# The tests cover various scenarios including valid data, missing required attributes,
# incorrect structure, and empty data. They also test the integration with the
# MMCIFHandler for importing data from JSON files.
