#!/usr/bin/env python3
"""
Test suite for import validation and basic instantiation.

This module contains unit tests that verify all critical imports work correctly
and that basic classes can be instantiated without errors.
"""

import unittest
import sys
from pathlib import Path
from sloth.mmcif.serializer import CacheManager, get_cache_manager


class TestImports(unittest.TestCase):
    """Test suite for import validation."""
    
    def test_main_converter_imports(self):
        """Test that main converter classes can be imported."""
        try:
            from sloth.mmcif.serializer import MappingGenerator, DictionaryParser
        except ImportError as e:
            self.fail(f"Failed to import main converter classes: {e}")
    
    def test_enum_imports(self):
        """Test that remaining enum classes can be imported."""
        try:
            from sloth.mmcif.defaults import (
                DataValue, DataType,
                is_null_value, get_numeric_fields
            )
        except ImportError as e:
            self.fail(f"Failed to import enum classes: {e}")
    
    def test_parser_imports(self):
        """Test that parser classes can be imported."""
        try:
            from sloth.mmcif.parser import MMCIFParser
            from sloth.mmcif import MMCIFHandler
        except ImportError as e:
            self.fail(f"Failed to import parser classes: {e}")
    
    def test_exporter_importer_imports(self):
        """Test that exporter and importer classes can be imported."""
        try:
            from sloth.mmcif import JSONExporter, JSONImporter
        except ImportError as e:
            self.fail(f"Failed to import exporter/importer classes: {e}")
    
    def test_basic_instantiation(self):
        """Test that basic classes can be instantiated."""
        from sloth.mmcif.serializer import MappingGenerator, DictionaryParser
        from sloth.mmcif.parser import MMCIFParser
        from sloth.mmcif import JSONExporter, JSONImporter
        
        # Test instantiation
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache)
        self.assertIsNotNone(dict_parser)
        
        mapping_gen = MappingGenerator(dict_parser, cache)
        self.assertIsNotNone(mapping_gen)
        
        parser = MMCIFParser()
        self.assertIsNotNone(parser)
        
        exporter = JSONExporter()
        self.assertIsNotNone(exporter)
        
        importer = JSONImporter()
        self.assertIsNotNone(importer)
    
    def test_exporter_with_parameters(self):
        """Test that exporter can be instantiated with optional parameters."""
        from sloth.mmcif import JSONExporter
        
        # Test with cache directory
        exporter = JSONExporter(cache_dir="/tmp/test_cache")
        self.assertIsNotNone(exporter)
        
        # Test with quiet mode to suppress warnings
        exporter = JSONExporter(quiet=True)
        self.assertIsNotNone(exporter)
    
    def test_pipeline_imports(self):
        """Test that relationship resolver can be imported."""
        try:
            from sloth.mmcif.serializer import RelationshipResolver
        except ImportError as e:
            self.fail(f"Failed to import RelationshipResolver: {e}")
    
    def test_resolver_instantiation(self):
        """Test that relationship resolver can be instantiated."""
        from sloth.mmcif.serializer import RelationshipResolver, DictionaryParser, MappingGenerator
        # RelationshipResolver requires a mapping generator
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache)
        mapping_gen = MappingGenerator(dict_parser, cache)
        resolver = RelationshipResolver(mapping_gen)
        self.assertIsNotNone(resolver)
    
    def test_enum_functionality(self):
        """Test that enum functions work correctly."""
        from sloth.mmcif.defaults import DataValue, DataType, is_null_value, get_numeric_fields
        
        # Test null value detection
        self.assertTrue(is_null_value("?"))
        self.assertTrue(is_null_value("."))
        self.assertFalse(is_null_value("valid_value"))
        
        # Test numeric data types
        numeric_types = DataType.get_numeric_types()
        self.assertIsInstance(numeric_types, set)
        self.assertIn("int", numeric_types)
        self.assertIn("float", numeric_types)
        
        # Test schema-driven numeric fields (returns empty set without mapping generator)
        numeric_fields = get_numeric_fields()
        self.assertIsInstance(numeric_fields, set)
    
    def test_exporter_lazy_loading(self):
        """Test that exporter components are properly lazy-loaded."""
        from sloth.mmcif import JSONExporter
        
        # Create exporter
        exporter = JSONExporter(quiet=True)
        
        # Test that exporter is created successfully
        self.assertIsNotNone(exporter)
        self.assertIsNotNone(exporter.resolver)
        
        # Test that mapping generator can provide mapping rules
        mapping_rules = exporter.resolver.mapping_rules
        self.assertIsInstance(mapping_rules, dict)
    
    def test_mapping_generator_lazy_loading(self):
        """Test that mapping generator properly lazy-loads components."""
        from sloth.mmcif.serializer import MappingGenerator, DictionaryParser
        
        # Create mapping generator in quiet mode
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache, quiet=True)
        mapping_gen = MappingGenerator(dict_parser, cache, quiet=True)
        
        # Initially, mapping rules should be None
        self.assertIsNone(mapping_gen._mapping_rules)
        
        # Accessing mapping rules should trigger lazy loading
        mapping_rules = mapping_gen.get_mapping_rules()
        self.assertIsInstance(mapping_rules, dict)
        self.assertIsNotNone(mapping_gen._mapping_rules)
    
    def test_error_handling(self):
        """Test that error conditions are handled gracefully."""
        from sloth.mmcif.serializer import MappingGenerator, DictionaryParser
        
        # Test with non-existent dictionary file (should not crash)
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache, quiet=True)
        dict_parser.source = "/totally/nonexistent/path.dic"
        mapping_gen = MappingGenerator(dict_parser, cache, quiet=True)
        self.assertIsNotNone(mapping_gen)
        
        # Should still be able to access mapping rules (with fallbacks)
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
    
    def test_submodule_accessibility(self):
        """Test that submodules are accessible."""
        try:
            import sloth.mmcif.parser
            import sloth.mmcif.serializer
            import sloth.mmcif.models
            import sloth.mmcif.exporter
            import sloth.mmcif.importer
        except ImportError as e:
            self.fail(f"Failed to import submodules: {e}")


if __name__ == '__main__':
    unittest.main(verbosity=2)

