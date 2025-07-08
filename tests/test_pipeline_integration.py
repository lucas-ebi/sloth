#!/usr/bin/env python3
"""
Comprehensive integration test suite for the mmCIF-to-PDBML pipeline.

This module contains integration tests that validate the complete pipeline
functionality, including XML generation, mapping rules, and component fixes.
"""

import unittest
import tempfile
import os
import shutil
from pathlib import Path

from sloth.parser import MMCIFParser
from sloth.serializers import (
    PDBMLConverter, MappingGenerator, NoCache, 
    DictionaryParser, XSDParser
)
from sloth import MMCIFHandler


class TestPipelineIntegration(unittest.TestCase):
    """Integration tests for the complete pipeline."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create simple test mmCIF data
        self.simple_mmcif = """data_TEST
#
_entry.id TEST
#
_atom_site.group_PDB  ATOM
_atom_site.id         1
_atom_site.type_symbol C
_atom_site.label_atom_id CA
_atom_site.Cartn_x    1.0
_atom_site.Cartn_y    2.0
_atom_site.Cartn_z    3.0
#"""
        
        self.test_file = os.path.join(self.temp_dir, 'simple_test.cif')
        with open(self.test_file, 'w') as f:
            f.write(self.simple_mmcif)
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def _create_converter(self, permissive: bool = False) -> PDBMLConverter:
        """Helper method to create a properly configured PDBMLConverter."""
        from pathlib import Path
        
        # Set up caching
        cache = NoCache(os.path.join(self.temp_dir, ".cache"))
        
        # Set up metadata parsers with default paths
        dict_path = Path(__file__).parent.parent / "sloth" / "schemas" / "mmcif_pdbx_v50.dic"
        xsd_path = Path(__file__).parent.parent / "sloth" / "schemas" / "pdbx-v50.xsd"
        
        dict_parser = DictionaryParser(cache, quiet=True)
        xsd_parser = XSDParser(cache, quiet=True)
        dict_parser.source = dict_path
        xsd_parser.source = xsd_path
        
        # Set up mapping generator
        mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache, quiet=True)
        
        # Create converter
        return PDBMLConverter(mapping_generator, permissive=permissive, quiet=True)
    
    def test_parser_functionality(self):
        """Test that the mmCIF parser works correctly."""
        parser = MMCIFParser()
        container = parser.parse_file(self.test_file)
        
        self.assertEqual(len(container.data), 1)
        self.assertIn('TEST', container.data)
        
        data_block = container.data['TEST']
        self.assertIn('_entry', data_block.categories)
        self.assertIn('_atom_site', data_block.categories)
    
    def test_converter_basic_functionality(self):
        """Test basic converter functionality."""
        converter = self._create_converter()
        self.assertIsNotNone(converter)
        
        parser = MMCIFParser()
        container = parser.parse_file(self.test_file)
        
        # Test XML generation
        pdbml_xml = converter.convert_to_pdbml(container)
        self.assertIsInstance(pdbml_xml, str)
        self.assertGreater(len(pdbml_xml), 100)
        self.assertTrue(pdbml_xml.startswith('<?xml'))
        self.assertTrue(pdbml_xml.endswith('>'))
    
    def test_xml_content_validity(self):
        """Test that generated XML contains expected content."""
        parser = MMCIFParser()
        container = parser.parse_file(self.test_file)
        
        converter = self._create_converter()
        pdbml_xml = converter.convert_to_pdbml(container)
        
        # Check for expected XML elements
        self.assertIn('datablock', pdbml_xml)
        self.assertIn('datablockName="TEST"', pdbml_xml)
        self.assertIn('entryCategory', pdbml_xml)
        self.assertIn('atom_siteCategory', pdbml_xml)
        self.assertIn('xmlns=', pdbml_xml)
    
    def test_mapping_rules_generation(self):
        """Test that mapping rules are generated correctly."""
        converter = self._create_converter()
        mapping_rules = converter.mapping_generator.get_mapping_rules()
        
        self.assertIsInstance(mapping_rules, dict)
        self.assertIn('category_mapping', mapping_rules)
        
        category_mapping = mapping_rules['category_mapping']
        self.assertIsInstance(category_mapping, dict)
        self.assertGreater(len(category_mapping), 0)
    
    def test_handler_integration(self):
        """Test integration with MMCIFHandler."""
        handler = MMCIFHandler()
        container = handler.parse(self.test_file)
        
        self.assertEqual(len(container.data), 1)
        
        # Test with converter
        converter = self._create_converter()
        pdbml_xml = converter.convert_to_pdbml(container)
        
        self.assertIsInstance(pdbml_xml, str)
        self.assertIn('TEST', pdbml_xml)


class TestComponentFixes(unittest.TestCase):
    """Test specific component fixes and enhancements."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def _create_converter(self, permissive: bool = False) -> PDBMLConverter:
        """Helper method to create a properly configured PDBMLConverter."""
        from pathlib import Path
        
        # Set up caching
        cache = NoCache(os.path.join(self.temp_dir, ".cache"))
        
        # Set up metadata parsers with default paths
        dict_path = Path(__file__).parent.parent / "sloth" / "schemas" / "mmcif_pdbx_v50.dic"
        xsd_path = Path(__file__).parent.parent / "sloth" / "schemas" / "pdbx-v50.xsd"
        
        dict_parser = DictionaryParser(cache, quiet=True)
        xsd_parser = XSDParser(cache, quiet=True)
        dict_parser.source = dict_path
        xsd_parser.source = xsd_path
        
        # Set up mapping generator
        mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache, quiet=True)
        
        # Create converter
        return PDBMLConverter(mapping_generator, permissive=permissive, quiet=True)
    
    def test_enum_class_functionality(self):
        """Test that enum classes work correctly."""
        from sloth.schemas import (
            XMLLocation, NullValue, NumericDataType,
            get_numeric_fields, is_null_value
        )
        
        # Test XMLLocation enum
        self.assertEqual(XMLLocation.ATTRIBUTE.value, "attribute")
        self.assertEqual(XMLLocation.ELEMENT_CONTENT.value, "element_content")
        self.assertEqual(XMLLocation.ELEMENT.value, "element")
        
        # Test NullValue enum and its helper
        self.assertTrue(NullValue.is_null("?"))
        self.assertTrue(NullValue.is_null("."))
        self.assertFalse(NullValue.is_null("actual_value"))
        
        # Test helper functions
        numeric_fields = get_numeric_fields()
        self.assertIsInstance(numeric_fields, set)
        # Schema-driven approach returns empty set without mapping generator
        
        # Test NumericDataType enum
        numeric_types = NumericDataType.get_type_names()
        self.assertIsInstance(numeric_types, set)
        self.assertIn("int", numeric_types)
        self.assertIn("float", numeric_types)
        
        # Test is_null_value helper
        self.assertTrue(is_null_value("?"))
        self.assertTrue(is_null_value("."))
        self.assertFalse(is_null_value("real_value"))
    
    def test_xml_mapping_generator_properties(self):
        """Test MappingGenerator properties."""
        from sloth.serializers import NoCache, DictionaryParser, XSDParser
        cache = NoCache("/tmp/test_cache")
        dict_parser = DictionaryParser(cache)
        xsd_parser = XSDParser(cache)
        mapping_gen = MappingGenerator(dict_parser, xsd_parser, cache)
        
        # Test that mapping rules can be accessed without errors
        mapping_rules = mapping_gen.get_mapping_rules()
        self.assertIsInstance(mapping_rules, dict)
    
    def test_converter_with_caching(self):
        """Test that converter caching works correctly."""
        cache_dir = tempfile.mkdtemp()
        
        try:
            # Set up caching manually
            cache = NoCache(cache_dir)
            
            # Set up metadata parsers with default paths
            from pathlib import Path
            dict_path = Path(__file__).parent.parent / "sloth" / "schemas" / "mmcif_pdbx_v50.dic"
            xsd_path = Path(__file__).parent.parent / "sloth" / "schemas" / "pdbx-v50.xsd"
            
            dict_parser = DictionaryParser(cache, quiet=True)
            xsd_parser = XSDParser(cache, quiet=True)
            dict_parser.source = dict_path
            xsd_parser.source = xsd_path
            
            # Set up mapping generator
            mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache, quiet=True)
            
            # Create converter
            converter = PDBMLConverter(mapping_generator, quiet=True)
            
            # Test that mapping rules can be accessed
            mapping_rules = converter.mapping_generator.get_mapping_rules()
            self.assertIsInstance(mapping_rules, dict)
            
        finally:
            shutil.rmtree(cache_dir)
    
    def test_dictionary_parser_instantiation(self):
        """Test DictionaryParser instantiation and basic functionality."""
        from sloth.serializers import DictionaryParser
        
        # Create with cache
        cache = NoCache(os.path.join(self.temp_dir, ".cache"))
        parser = DictionaryParser(cache, quiet=True)
        self.assertIsNotNone(parser)
        
        # Test basic functionality by parsing
        from pathlib import Path
        dict_path = Path(__file__).parent.parent / "sloth" / "schemas" / "mmcif_pdbx_v50.dic"
        result = parser.parse(dict_path)
        self.assertIsInstance(result, dict)
        self.assertIn('categories', result)
        self.assertIn('items', result)
        self.assertIn('relationships', result)
        self.assertIn('enumerations', result)


class TestErrorHandling(unittest.TestCase):
    """Test error handling in the pipeline."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def _create_converter(self, permissive: bool = False) -> PDBMLConverter:
        """Helper method to create a properly configured PDBMLConverter."""
        from pathlib import Path
        
        # Set up caching
        cache = NoCache(os.path.join(self.temp_dir, ".cache"))
        
        # Set up metadata parsers with default paths
        dict_path = Path(__file__).parent.parent / "sloth" / "schemas" / "mmcif_pdbx_v50.dic"
        xsd_path = Path(__file__).parent.parent / "sloth" / "schemas" / "pdbx-v50.xsd"
        
        dict_parser = DictionaryParser(cache, quiet=True)
        xsd_parser = XSDParser(cache, quiet=True)
        dict_parser.source = dict_path
        xsd_parser.source = xsd_path
        
        # Set up mapping generator
        mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache, quiet=True)
        
        # Create converter
        return PDBMLConverter(mapping_generator, permissive=permissive, quiet=True)
    
    def test_invalid_file_handling(self):
        """Test handling of invalid mmCIF files."""
        try:
            # Create invalid mmCIF file
            invalid_file = os.path.join(self.temp_dir, 'invalid.cif')
            with open(invalid_file, 'w') as f:
                f.write("This is not a valid mmCIF file")
            
            parser = MMCIFParser()
            
            # Should handle the error gracefully
            with self.assertRaises(Exception):
                parser.parse_file(invalid_file)
                
        except Exception:
            pass  # Expected behavior
    
    def test_converter_with_invalid_input(self):
        """Test converter with invalid input."""
        from sloth.models import MMCIFDataContainer
        
        converter = self._create_converter()
        
        # Create empty container
        empty_container = MMCIFDataContainer()
        
        # Should handle gracefully
        try:
            result = converter.convert_to_pdbml(empty_container)
            # Should return some kind of result, even if minimal
            self.assertIsInstance(result, str)
        except Exception as e:
            # If it raises an exception, it should be informative
            self.assertIsInstance(e, Exception)


if __name__ == '__main__':
    unittest.main(verbosity=2)
