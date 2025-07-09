#!/usr/bin/env python3
"""
Test suite for import validation and basic instantiation.

This module contains unit tests that verify all critical imports work correctly
and that basic classes can be instantiated without errors.
"""

import unittest
import sys
from pathlib import Path
from sloth.serializers import CacheManager, get_cache_manager


class TestImports(unittest.TestCase):
    """Test suite for import validation."""
    
    def test_main_converter_imports(self):
        """Test that main converter classes can be imported."""
        try:
            from sloth.serializers import PDBMLConverter, MappingGenerator, DictionaryParser
        except ImportError as e:
            self.fail(f"Failed to import main converter classes: {e}")
    
    def test_enum_imports(self):
        """Test that remaining enum classes can be imported."""
        try:
            from sloth.schemas import (
                XMLLocation, NullValue, NumericDataType,
                is_null_value, get_numeric_fields
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
            from sloth.validators import XMLSchemaValidator
            from sloth import SchemaValidator, ValidatorFactory
        except ImportError as e:
            self.fail(f"Failed to import schema classes: {e}")
    
    def test_basic_instantiation(self):
        """Test that basic classes can be instantiated."""
        from sloth.serializers import PDBMLConverter, MappingGenerator, DictionaryParser, XSDParser
        from sloth.parser import MMCIFParser
        
        # Test instantiation without parameters
        # PDBMLConverter now requires a MappingGenerator
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache)
        xsd_parser = XSDParser(cache)  
        mapping_gen = MappingGenerator(dict_parser, xsd_parser, cache)
        converter = PDBMLConverter(mapping_gen)
        self.assertIsNotNone(converter)
        
        # MappingGenerator requires cache and parsers
        self.assertIsNotNone(mapping_gen)
        
        dict_parser = DictionaryParser(cache)
        self.assertIsNotNone(dict_parser)
        
        parser = MMCIFParser()
        self.assertIsNotNone(parser)
    
    def test_converter_with_parameters(self):
        """Test that converter can be instantiated with optional parameters."""
        from sloth.serializers import PDBMLConverter, DictionaryParser, XSDParser, MappingGenerator
        
        # Test with cache directory
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache)
        xsd_parser = XSDParser(cache)
        mapping_gen = MappingGenerator(dict_parser, xsd_parser, cache)
        converter = PDBMLConverter(mapping_gen)
        self.assertIsNotNone(converter)
        
        # Test with permissive mode
        converter = PDBMLConverter(mapping_gen, permissive=True)
        self.assertIsNotNone(converter)
        
        # Test with quiet mode to suppress warnings
        converter = PDBMLConverter(mapping_gen, quiet=True)
        self.assertIsNotNone(converter)
    
    def test_pipeline_imports(self):
        """Test that pipeline classes can be imported."""
        try:
            from sloth.serializers import MMCIFToPDBMLPipeline, RelationshipResolver
        except ImportError as e:
            self.fail(f"Failed to import pipeline classes: {e}")
    
    def test_pipeline_instantiation(self):
        """Test that pipeline classes can be instantiated."""
        from sloth.serializers import MMCIFToPDBMLPipeline, RelationshipResolver, DictionaryParser, XSDParser, MappingGenerator
        
        pipeline = MMCIFToPDBMLPipeline()
        self.assertIsNotNone(pipeline)
        
        # RelationshipResolver requires a mapping generator
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache)
        xsd_parser = XSDParser(cache)
        mapping_gen = MappingGenerator(dict_parser, xsd_parser, cache)
        resolver = RelationshipResolver(mapping_gen)
        self.assertIsNotNone(resolver)
    
    def test_enum_functionality(self):
        """Test that enum functions work correctly."""
        from sloth.schemas import XMLLocation, NullValue, NumericDataType, is_null_value, get_numeric_fields
        
        # Test XMLLocation enum
        self.assertEqual(XMLLocation.ATTRIBUTE.value, "attribute")
        self.assertEqual(XMLLocation.ELEMENT_CONTENT.value, "element_content")
        
        # Test null value detection
        self.assertTrue(is_null_value("?"))
        self.assertTrue(is_null_value("."))
        self.assertFalse(is_null_value("valid_value"))
        
        # Test numeric data types
        numeric_types = NumericDataType.get_type_names()
        self.assertIsInstance(numeric_types, set)
        self.assertIn("int", numeric_types)
        self.assertIn("float", numeric_types)
        
        # Test schema-driven numeric fields (returns empty set without mapping generator)
        numeric_fields = get_numeric_fields()
        self.assertIsInstance(numeric_fields, set)
    
    def test_converter_lazy_loading(self):
        """Test that converter components are properly lazy-loaded."""
        from sloth.serializers import PDBMLConverter, DictionaryParser, XSDParser, MappingGenerator
        
        # Create converter with required mapping generator
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache, quiet=True)
        xsd_parser = XSDParser(cache, quiet=True)
        mapping_gen = MappingGenerator(dict_parser, xsd_parser, cache, quiet=True)
        converter = PDBMLConverter(mapping_gen, quiet=True)
        
        # Test that converter is created successfully
        self.assertIsNotNone(converter)
        self.assertIsNotNone(converter.mapping_generator)
        
        # Test that mapping generator can provide mapping rules
        mapping_rules = converter.mapping_generator.get_mapping_rules()
        self.assertIsInstance(mapping_rules, dict)
    
    def test_mapping_generator_lazy_loading(self):
        """Test that XML mapping generator properly lazy-loads components."""
        from sloth.serializers import MappingGenerator, DictionaryParser, XSDParser
        
        # Create mapping generator in quiet mode
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache, quiet=True)
        xsd_parser = XSDParser(cache, quiet=True)
        mapping_gen = MappingGenerator(dict_parser, xsd_parser, cache, quiet=True)
        
        # Initially, mapping rules should be None
        self.assertIsNone(mapping_gen._mapping_rules)
        
        # Accessing mapping rules should trigger lazy loading
        mapping_rules = mapping_gen.get_mapping_rules()
        self.assertIsInstance(mapping_rules, dict)
        self.assertIsNotNone(mapping_gen._mapping_rules)
    
    def test_error_handling(self):
        """Test that error conditions are handled gracefully."""
        from sloth.serializers import PDBMLConverter, MappingGenerator, DictionaryParser, XSDParser
        
        # Test with non-existent dictionary file (should not crash)
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache, quiet=True)
        xsd_parser = XSDParser(cache, quiet=True)
        dict_parser.source = "/totally/nonexistent/path.dic"
        xsd_parser.source = "/totally/nonexistent/path.xsd"
        mapping_gen = MappingGenerator(dict_parser, xsd_parser, cache, quiet=True)
        converter = PDBMLConverter(mapping_gen, quiet=True)
        self.assertIsNotNone(converter)
        
        # Should still be able to access mapping rules (with fallbacks)
        mapping_rules = mapping_gen.get_mapping_rules()
        self.assertIsInstance(mapping_rules, dict)
        
        # Test mapping generator with invalid paths
        from sloth.serializers import DictionaryParser, XSDParser
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache, quiet=True)
        xsd_parser = XSDParser(cache, quiet=True)
        dict_parser.source = "/invalid/dict.dic"
        xsd_parser.source = "/invalid/schema.xsd"
        mapping_gen = MappingGenerator(dict_parser, xsd_parser, cache, quiet=True)
        self.assertIsNotNone(mapping_gen)
        
        # Should return empty but valid structures
        mapping_rules = mapping_gen.get_mapping_rules()
        self.assertIsInstance(mapping_rules, dict)


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
            import sloth.serializers
            import sloth.validators
            import sloth.models
        except ImportError as e:
            self.fail(f"Failed to import submodules: {e}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
