#!/usr/bin/env python3
"""
Test suite for parameter refactoring - ensuring permissive/validate distinction.

Tests the new parameter naming convention:
- permissive: controls schema validation
- validate: reserved for custom plugin validation
"""

import unittest
import tempfile
import json
import os
from sloth.mmcif import (
    MMCIFHandler,
    JSONImporter,
    XMLImporter,
    JSONExporter,
    XMLExporter,
    MMCIFDataContainer,
    DataBlock,
    Category,
)
from sloth.mmcif.defaults import ExportFormat, StructureFormat


class TestParameterRefactoring(unittest.TestCase):
    """Test the new parameter naming convention."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test data
        self.test_data = {
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
        
        # Create test JSON file
        self.json_file = os.path.join(self.temp_dir, "test.json")
        with open(self.json_file, "w") as f:
            json.dump(self.test_data, f)
            
        # Create test XML file
        self.xml_file = os.path.join(self.temp_dir, "test.xml")
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
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
        with open(self.xml_file, "w") as f:
            f.write(xml_content)

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_json_importer_permissive_parameter(self):
        """Test JSONImporter uses permissive parameter correctly."""
        # Test with permissive=True (should not validate schema)
        importer = JSONImporter(permissive=True)
        container = importer.import_data(self.json_file, nested=True, permissive=True)
        self.assertIsInstance(container, MMCIFDataContainer)
        
        # Test with permissive=False (should validate schema)
        importer = JSONImporter(permissive=False)
        container = importer.import_data(self.json_file, nested=True, permissive=False)
        self.assertIsInstance(container, MMCIFDataContainer)

    def test_xml_importer_permissive_parameter(self):
        """Test XMLImporter uses permissive parameter correctly."""
        # Test with permissive=True (should not validate schema)
        importer = XMLImporter(permissive=True)
        container = importer.import_data(self.xml_file, nested=True, permissive=True)
        self.assertIsInstance(container, MMCIFDataContainer)
        
        # Test with permissive=False (should validate schema)
        importer = XMLImporter(permissive=False)
        container = importer.import_data(self.xml_file, nested=True, permissive=False)
        self.assertIsInstance(container, MMCIFDataContainer)

    def test_json_exporter_permissive_parameter(self):
        """Test JSONExporter uses permissive parameter correctly."""
        # Create a simple mmCIF container
        block = DataBlock("TEST")
        category = Category("_entity", None)
        category._add_item_value("id", "1")
        category._add_item_value("type", "polymer")
        category._commit_all_batches()
        block._categories["_entity"] = category
        container = MMCIFDataContainer(data_blocks={"TEST": block})
        
        # Test with permissive=True
        exporter = JSONExporter(permissive=True)
        json_str = exporter.export_data(container, nested=True, permissive=True)
        self.assertIsInstance(json_str, str)
        
        # Test with permissive=False
        exporter = JSONExporter(permissive=False)
        json_str = exporter.export_data(container, nested=True, permissive=False)
        self.assertIsInstance(json_str, str)

    def test_xml_exporter_permissive_parameter(self):
        """Test XMLExporter uses permissive parameter correctly."""
        # Create a simple mmCIF container
        block = DataBlock("TEST")
        category = Category("_entity", None)
        category._add_item_value("id", "1")
        category._add_item_value("type", "polymer")
        category._commit_all_batches()
        block._categories["_entity"] = category
        container = MMCIFDataContainer(data_blocks={"TEST": block})
        
        # Test with permissive=True
        exporter = XMLExporter(permissive=True)
        xml_str = exporter.export_data(container, nested=True, permissive=True)
        self.assertIsInstance(xml_str, str)
        
        # Test with permissive=False
        exporter = XMLExporter(permissive=False)
        xml_str = exporter.export_data(container, nested=True, permissive=False)
        self.assertIsInstance(xml_str, str)

    def test_handler_unified_api_permissive(self):
        """Test MMCIFHandler unified API uses permissive parameter."""
        handler = MMCIFHandler()
        
        # Test import with permissive=True
        container = handler.import_data(
            self.json_file, 
            ExportFormat.JSON, 
            StructureFormat.NESTED, 
            permissive=True
        )
        self.assertIsInstance(container, MMCIFDataContainer)
        
        # Test import with permissive=False
        container = handler.import_data(
            self.json_file, 
            ExportFormat.JSON, 
            StructureFormat.NESTED, 
            permissive=False
        )
        self.assertIsInstance(container, MMCIFDataContainer)
        
        # Test export with permissive=True
        json_str = handler.export(
            container,
            ExportFormat.JSON,
            StructureFormat.NESTED,
            permissive=True
        )
        self.assertIsInstance(json_str, str)
        
        # Test export with permissive=False
        json_str = handler.export(
            container,
            ExportFormat.JSON,
            StructureFormat.NESTED,
            permissive=False
        )
        self.assertIsInstance(json_str, str)

    def test_legacy_implementations_availability(self):
        """Test that legacy parser and writer are still available for reference."""
        try:
            from sloth.legacy import MMCIFParser, MMCIFWriter
            
            # Just test that they can be imported and instantiated
            parser = MMCIFParser(None)
            writer = MMCIFWriter()
            
            self.assertIsNotNone(parser)
            self.assertIsNotNone(writer)
            
        except ImportError:
            self.fail("Legacy implementations should be available in sloth.legacy")

    def test_parameter_precedence(self):
        """Test that permissive parameter takes precedence in the new API."""
        handler = MMCIFHandler()
        
        # Import data using the new API - should only use permissive parameter
        container = handler.import_data(
            self.json_file, 
            ExportFormat.JSON, 
            StructureFormat.NESTED, 
            permissive=True
        )
        self.assertIsInstance(container, MMCIFDataContainer)
        
        # Export data using the new API - should only use permissive parameter
        output = handler.export(
            container,
            ExportFormat.JSON,
            StructureFormat.NESTED,
            permissive=True
        )
        self.assertIsInstance(output, str)

    def test_parameter_semantic_distinction(self):
        """Test that parameter names have correct semantic meaning."""
        # permissive=True should mean "skip schema validation"
        # permissive=False should mean "enforce schema validation"
        
        importer = JSONImporter(permissive=True)  # Skip validation by default
        
        # With permissive=True, should not perform strict validation
        container = importer.import_data(self.json_file, nested=True, permissive=True)
        self.assertIsInstance(container, MMCIFDataContainer)
        
        # With permissive=False, should perform strict validation
        container = importer.import_data(self.json_file, nested=True, permissive=False)
        self.assertIsInstance(container, MMCIFDataContainer)

    def test_method_signatures_consistency(self):
        """Test that all method signatures are consistent with new parameter naming."""
        # Test that import_data methods have permissive parameter
        json_importer = JSONImporter()
        xml_importer = XMLImporter()
        
        # Check method signatures (this will fail if parameters don't match)
        import inspect
        
        json_sig = inspect.signature(json_importer.import_data)
        xml_sig = inspect.signature(xml_importer.import_data)
        
        # Both should have 'permissive' parameter
        self.assertIn('permissive', json_sig.parameters)
        self.assertIn('permissive', xml_sig.parameters)
        
        # Test that export_data methods have permissive parameter
        json_exporter = JSONExporter()
        xml_exporter = XMLExporter()
        
        json_export_sig = inspect.signature(json_exporter.export_data)
        xml_export_sig = inspect.signature(xml_exporter.export_data)
        
        # Both should have 'permissive' parameter
        self.assertIn('permissive', json_export_sig.parameters)
        self.assertIn('permissive', xml_export_sig.parameters)


if __name__ == "__main__":
    unittest.main()
