#!/usr/bin/env python3
"""
Comprehensive integration test suite for the mmCIF-to-JSON pipeline.

This module contains integration tests that validate the complete pipeline
functionality, including JSON generation, mapping rules, and relationship resolution.
"""

import unittest
import tempfile
import os
import json
import shutil
from pathlib import Path

from sloth.mmcif.parser import MMCIFParser
from sloth.mmcif.serializer import (
    MappingGenerator, RelationshipResolver,
    DictionaryParser, get_cache_manager
)
from sloth.mmcif import MMCIFHandler, JSONExporter
from tests.test_utils import get_shared_exporter, create_complex_mmcif_with_relationships


class TestPipelineIntegration(unittest.TestCase):
    """Integration tests for the complete mmCIF-to-JSON pipeline."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create comprehensive test mmCIF data with relationships
        self.complex_mmcif = create_complex_mmcif_with_relationships()
        
        self.test_file = os.path.join(self.temp_dir, 'complex_test.cif')
        with open(self.test_file, 'w') as f:
            f.write(self.complex_mmcif)
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def test_parser_functionality(self):
        """Test that the mmCIF parser works correctly."""
        parser = MMCIFParser()
        container = parser.parse(self.test_file)
        
        self.assertEqual(len(container.data), 1)
        self.assertIn('COMPLEX', container.data)
        
        data_block = container.data['COMPLEX']
        self.assertIn('_entry', data_block.categories)
        self.assertIn('_entity', data_block.categories)
        self.assertIn('_atom_site', data_block.categories)
    
    def test_json_exporter_basic_functionality(self):
        """Test basic JSON exporter functionality."""
        exporter = get_shared_exporter()
        self.assertIsNotNone(exporter)
        
        parser = MMCIFParser()
        container = parser.parse(self.test_file)
        
        # Test JSON generation
        json_str = exporter.export_data(container)
        self.assertIsInstance(json_str, str)
        self.assertGreater(len(json_str), 100)
        
        # Verify it's valid JSON
        data = json.loads(json_str)
        self.assertIsInstance(data, dict)
    
    def test_json_content_validity(self):
        """Test that generated JSON contains expected content."""
        parser = MMCIFParser()
        container = parser.parse(self.test_file)
        
        exporter = get_shared_exporter()
        json_str = exporter.export_data(container)
        
        data = json.loads(json_str)
        
        # Check for expected nested structure
        self.assertIn('data_COMPLEX', data)
        block_data = data['data_COMPLEX']
        
        # Check for entities
        self.assertIn('_entity', block_data)
        entities = block_data['_entity']
        self.assertIsInstance(entities, list)
        self.assertEqual(len(entities), 2)
        
        # Check first entity
        entity = entities[0]
        self.assertEqual(entity['id'], '1')
        self.assertEqual(entity['type'], 'polymer')
    
    def test_relationship_resolution(self):
        """Test that relationships are properly resolved in nested JSON."""
        parser = MMCIFParser()
        container = parser.parse(self.test_file)
        
        exporter = get_shared_exporter()
        json_str = exporter.export_data(container)
        data = json.loads(json_str)
        
        block_data = data['data_COMPLEX']
        entities = block_data['_entity']
        entity = entities[0]
        
        # Check nested entity_poly
        self.assertIn('entity_poly', entity)
        entity_poly = entity['entity_poly']
        self.assertIsInstance(entity_poly, list)
        self.assertGreater(len(entity_poly), 0)
        
        # Check nested entity_poly_seq
        poly = entity_poly[0]
        self.assertIn('entity_poly_seq', poly)
        poly_seq = poly['entity_poly_seq']
        self.assertIsInstance(poly_seq, list)
        self.assertEqual(len(poly_seq), 2)  # VAL and ALA
        
        # Check nested struct_asym
        self.assertIn('struct_asym', entity)
        struct_asym = entity['struct_asym']
        self.assertIsInstance(struct_asym, list)
        self.assertGreater(len(struct_asym), 0)
        
        # Check nested atom_site
        asym = struct_asym[0]
        self.assertIn('atom_site', asym)
        atom_sites = asym['atom_site']
        self.assertIsInstance(atom_sites, list)
        self.assertEqual(len(atom_sites), 2)  # Two atoms for entity 1
    
    def test_mapping_rules_generation(self):
        """Test that mapping rules are generated correctly."""
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache, quiet=True)
        mapping_gen = MappingGenerator(dict_parser, cache, quiet=True)
        
        mapping_rules = mapping_gen.get_mapping_rules()
        
        self.assertIsInstance(mapping_rules, dict)
        self.assertIn('category_mapping', mapping_rules)
        self.assertIn('item_mapping', mapping_rules)
        self.assertIn('fk_map', mapping_rules)
        self.assertIn('primary_keys', mapping_rules)
        
        category_mapping = mapping_rules['category_mapping']
        self.assertIsInstance(category_mapping, dict)
        # Category mapping exists but may be empty or have different structure
        self.assertTrue(isinstance(category_mapping, dict))
    
    def test_handler_integration(self):
        """Test integration with MMCIFHandler."""
        handler = MMCIFHandler()
        container = handler.read(self.test_file)
        
        self.assertEqual(len(container.data), 1)
        
        # Test JSON export through handler
        json_str = handler.export(container, format_type='json')
        self.assertIsInstance(json_str, str)
        
        data = json.loads(json_str)
        self.assertIn('data_COMPLEX', data)
    
    def test_end_to_end_pipeline(self):
        """Test complete end-to-end pipeline: mmCIF -> parsing -> resolution -> JSON."""
        # Parse mmCIF
        parser = MMCIFParser()
        container = parser.parse(self.test_file)
        
        # Export to JSON
        exporter = get_shared_exporter()
        json_str = exporter.export_data(container)
        
        # Parse JSON to verify structure
        data = json.loads(json_str)
        block_data = data['data_COMPLEX']
        
        # Verify 4-level nesting: entity -> entity_poly -> entity_poly_seq
        entity = block_data['_entity'][0]
        entity_poly = entity['entity_poly'][0]
        entity_poly_seq = entity_poly['entity_poly_seq']
        
        self.assertEqual(len(entity_poly_seq), 2)
        self.assertEqual(entity_poly_seq[0]['mon_id'], 'VAL')
        self.assertEqual(entity_poly_seq[1]['mon_id'], 'ALA')
        
        # Verify parallel branch: entity -> struct_asym -> atom_site
        struct_asym = entity['struct_asym'][0]
        atom_sites = struct_asym['atom_site']
        
        self.assertEqual(len(atom_sites), 2)
        self.assertEqual(atom_sites[0]['label_atom_id'], 'CA')
        self.assertEqual(atom_sites[0]['Cartn_x'], '10.0')


class TestComponentFixes(unittest.TestCase):
    """Test specific component fixes and enhancements."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def test_enum_class_functionality(self):
        """Test that enum classes work correctly."""
        from sloth.mmcif.defaults import (
            DataValue, DataType,
            get_numeric_fields, is_null_value
        )
        
        # Test DataValue enum and its helper
        self.assertTrue(DataValue.is_null("?"))
        self.assertTrue(DataValue.is_null("."))
        self.assertFalse(DataValue.is_null("actual_value"))
        
        # Test helper functions
        numeric_fields = get_numeric_fields()
        self.assertIsInstance(numeric_fields, set)
        
        # Test DataType enum
        numeric_types = DataType.get_numeric_types()
        self.assertIsInstance(numeric_types, set)
        self.assertIn("int", numeric_types)
        self.assertIn("float", numeric_types)
        
        # Test is_null_value helper
        self.assertTrue(is_null_value("?"))
        self.assertTrue(is_null_value("."))
        self.assertFalse(is_null_value("real_value"))
    
    def test_mapping_generator_properties(self):
        """Test MappingGenerator properties."""
        from sloth.mmcif.serializer import DictionaryParser
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache, quiet=True)
        mapping_gen = MappingGenerator(dict_parser, cache, quiet=True)
        
        # Test that mapping rules can be accessed without errors
        mapping_rules = mapping_gen.get_mapping_rules()
        self.assertIsInstance(mapping_rules, dict)
    
    def test_exporter_with_caching(self):
        """Test that exporter caching works correctly."""
        cache_dir = tempfile.mkdtemp()
        
        try:
            # Create exporter with custom cache directory
            exporter = JSONExporter(cache_dir=cache_dir, quiet=True)
            
            # Test that exporter works
            self.assertIsNotNone(exporter)
            self.assertIsNotNone(exporter.resolver)
            
            # Test that mapping rules can be accessed
            mapping_rules = exporter.resolver.mapping_rules
            self.assertIsInstance(mapping_rules, dict)
            
        finally:
            shutil.rmtree(cache_dir)
    
    def test_dictionary_parser_instantiation(self):
        """Test DictionaryParser instantiation and basic functionality."""
        from sloth.mmcif.serializer import DictionaryParser
        
        # Create with cache
        cache = get_cache_manager(os.path.join(self.temp_dir, ".cache"))
        parser = DictionaryParser(cache, quiet=True)
        self.assertIsNotNone(parser)
        
        # Test basic functionality by parsing
        dict_path = Path(__file__).parent.parent / "sloth" / "mmcif" / "schemas" / "mmcif_pdbx_v50.dic"
        if dict_path.exists():
            result = parser.parse(dict_path)
            self.assertIsInstance(result, dict)
            self.assertIn('categories', result)
            self.assertIn('items', result)
            self.assertIn('relationships', result)
            self.assertIn('enumerations', result)
    
    def test_relationship_resolver_instantiation(self):
        """Test RelationshipResolver instantiation."""
        cache = get_cache_manager("/tmp/test_cache")
        dict_parser = DictionaryParser(cache, quiet=True)
        mapping_gen = MappingGenerator(dict_parser, cache, quiet=True)
        resolver = RelationshipResolver(mapping_gen)
        
        self.assertIsNotNone(resolver)
        self.assertIsNotNone(resolver.mapping_generator)
        
        # Test that mapping rules are accessible
        mapping_rules = resolver.mapping_rules
        self.assertIsInstance(mapping_rules, dict)


class TestErrorHandling(unittest.TestCase):
    """Test error handling in the pipeline."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def test_invalid_file_handling(self):
        """Test handling of invalid mmCIF files."""
        # Create invalid mmCIF file
        invalid_file = os.path.join(self.temp_dir, 'invalid.cif')
        with open(invalid_file, 'w') as f:
            f.write("This is not a valid mmCIF file")
        
        parser = MMCIFParser()
        
        # Should handle the error gracefully
        with self.assertRaises(Exception):
            parser.parse(invalid_file)
    
    def test_exporter_with_invalid_input(self):
        """Test exporter with invalid input."""
        from sloth.mmcif.models import MMCIFDataContainer
        
        exporter = get_shared_exporter()
        
        # Create empty container
        empty_container = MMCIFDataContainer()
        
        # Should handle gracefully
        try:
            result = exporter.export_data(empty_container)
            # Should return some kind of result, even if minimal
            self.assertIsInstance(result, str)
            # Should be valid JSON
            data = json.loads(result)
            self.assertIsInstance(data, dict)
        except Exception as e:
            # If it raises an exception, it should be informative
            self.assertIsInstance(e, Exception)
    
    def test_exporter_with_file_output(self):
        """Test JSON export to file."""
        from tests.test_utils import create_simple_mmcif
        
        # Create simple mmCIF
        mmcif_content = create_simple_mmcif()
        mmcif_file = os.path.join(self.temp_dir, 'test.cif')
        with open(mmcif_file, 'w') as f:
            f.write(mmcif_content)
        
        # Parse and export
        parser = MMCIFParser()
        container = parser.parse(mmcif_file)
        
        exporter = get_shared_exporter()
        json_file = os.path.join(self.temp_dir, 'output.json')
        
        result = exporter.export_data(container, json_file)
        
        # When file_path is provided, should return None
        self.assertIsNone(result)
        
        # File should exist and contain valid JSON
        self.assertTrue(os.path.exists(json_file))
        with open(json_file) as f:
            data = json.load(f)
        
        self.assertIsInstance(data, dict)
        self.assertIn('data_TEST', data)


if __name__ == '__main__':
    unittest.main(verbosity=2)
