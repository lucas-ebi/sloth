#!/usr/bin/env python3
"""
Test suite for import validation and basic instantiation.

This module contains unit tests that verify all critical imports work correctly
and that basic classes can be instantiated without errors.
"""

import unittest
import sys
from pathlib import Path


class TestImports(unittest.TestCase):
    """Test suite for import validation."""
    
    def test_main_converter_imports(self):
        """Test that main converter classes can be imported."""
        try:
            from sloth.pdbml_converter import PDBMLConverter, XMLMappingGenerator, DictionaryParser
        except ImportError as e:
            self.fail(f"Failed to import main converter classes: {e}")
    
    def test_enum_imports(self):
        """Test that all enum classes can be imported."""
        try:
            from sloth.pdbml_enums import (
                XMLLocation, ElementOnlyItem, AtomSiteDefault, AnisotropicParam,
                ProblematicField, NullValue, SpecialAttribute, ValidationRule,
                EssentialKey, RequiredAttribute, NumericField,
                get_element_only_items, get_atom_site_defaults, get_anisotropic_defaults,
                get_problematic_field_replacement, is_null_value, get_numeric_fields
            )
        except ImportError as e:
            self.fail(f"Failed to import enum classes: {e}")
    
    def test_parser_imports(self):
        """Test that parser classes can be imported."""
        try:
            from sloth.parser import MMCIFParser
            from sloth import MMCIFHandler
        except ImportError as e:
            self.fail(f"Failed to import parser classes: {e}")
    
    def test_schema_imports(self):
        """Test that schema validation classes can be imported."""
        try:
            from sloth.schemas import XMLSchemaValidator
            from sloth import SchemaValidator, ValidatorFactory
        except ImportError as e:
            self.fail(f"Failed to import schema classes: {e}")
    
    def test_basic_instantiation(self):
        """Test that basic classes can be instantiated."""
        from sloth.pdbml_converter import PDBMLConverter, XMLMappingGenerator, DictionaryParser
        from sloth.parser import MMCIFParser
        
        # Test instantiation without parameters
        converter = PDBMLConverter()
        self.assertIsNotNone(converter)
        
        mapping_gen = XMLMappingGenerator()
        self.assertIsNotNone(mapping_gen)
        
        dict_parser = DictionaryParser()
        self.assertIsNotNone(dict_parser)
        
        parser = MMCIFParser()
        self.assertIsNotNone(parser)
    
    def test_converter_with_parameters(self):
        """Test that converter can be instantiated with optional parameters."""
        from sloth.pdbml_converter import PDBMLConverter
        
        # Test with cache directory
        converter = PDBMLConverter(cache_dir="/tmp/test_cache")
        self.assertIsNotNone(converter)
        
        # Test with dictionary path (even if file doesn't exist)
        converter = PDBMLConverter(dictionary_path="/nonexistent/path.dic")
        self.assertIsNotNone(converter)
    
    def test_pipeline_imports(self):
        """Test that pipeline classes can be imported."""
        try:
            from sloth.pdbml_converter import MMCIFToPDBMLPipeline, RelationshipResolver
        except ImportError as e:
            self.fail(f"Failed to import pipeline classes: {e}")
    
    def test_pipeline_instantiation(self):
        """Test that pipeline classes can be instantiated."""
        from sloth.pdbml_converter import MMCIFToPDBMLPipeline, RelationshipResolver
        
        pipeline = MMCIFToPDBMLPipeline()
        self.assertIsNotNone(pipeline)
        
        resolver = RelationshipResolver()
        self.assertIsNotNone(resolver)


class TestModuleStructure(unittest.TestCase):
    """Test suite for module structure validation."""
    
    def test_sloth_package_structure(self):
        """Test that the sloth package has expected structure."""
        import sloth
        
        # Check that main classes are accessible from package root
        self.assertTrue(hasattr(sloth, 'MMCIFHandler'))
        self.assertTrue(hasattr(sloth, 'MMCIFParser'))
        self.assertTrue(hasattr(sloth, 'PDBMLConverter'))
        self.assertTrue(hasattr(sloth, 'SchemaValidator'))
    
    def test_submodule_accessibility(self):
        """Test that submodules are accessible."""
        try:
            import sloth.parser
            import sloth.pdbml_converter
            import sloth.schemas
            import sloth.models
        except ImportError as e:
            self.fail(f"Failed to import submodules: {e}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
