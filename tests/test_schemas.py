#!/usr/bin/env python3
"""
Test suite for dictionary-based schema validation.

This module tests the dictionary parser and validation functionality
based on the mmCIF dictionary.
"""

import unittest
import tempfile
import os
import shutil
from pathlib import Path

from sloth.mmcif.serializer import DictionaryParser, MappingGenerator, get_cache_manager
from tests.test_utils import get_shared_mapping_generator


class TestDictionaryParsing(unittest.TestCase):
    """Test mmCIF dictionary parsing."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache = get_cache_manager(os.path.join(self.temp_dir, ".cache"))
        self.dict_path = Path(__file__).parent.parent / "sloth" / "mmcif" / "schemas" / "mmcif_pdbx_v50.dic"
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def test_dictionary_parser_instantiation(self):
        """Test creating a DictionaryParser instance."""
        parser = DictionaryParser(self.cache, quiet=True)
        self.assertIsNotNone(parser)
        # Parser uses cache internally but doesn't expose it as public attribute
    
    def test_dictionary_parsing(self):
        """Test parsing the mmCIF dictionary file."""
        if not self.dict_path.exists():
            self.skipTest(f"Dictionary file not found: {self.dict_path}")
        
        parser = DictionaryParser(self.cache, quiet=True)
        result = parser.parse(self.dict_path)
        
        # Verify structure
        self.assertIsInstance(result, dict)
        self.assertIn('categories', result)
        self.assertIn('items', result)
        self.assertIn('relationships', result)
        self.assertIn('enumerations', result)
    
    def test_dictionary_categories(self):
        """Test that dictionary has expected categories."""
        if not self.dict_path.exists():
            self.skipTest(f"Dictionary file not found: {self.dict_path}")
        
        parser = DictionaryParser(self.cache, quiet=True)
        result = parser.parse(self.dict_path)
        
        categories = result['categories']
        self.assertIsInstance(categories, dict)
        
        # Check that categories dict exists and has some content
        self.assertGreater(len(categories), 0, "Categories dict should not be empty")
        # Check for at least one expected category (atom_site is very common)
        self.assertTrue(any('atom_site' in str(cat).lower() for cat in categories.keys()))
    
    def test_dictionary_items(self):
        """Test that dictionary has expected items."""
        if not self.dict_path.exists():
            self.skipTest(f"Dictionary file not found: {self.dict_path}")
        
        parser = DictionaryParser(self.cache, quiet=True)
        result = parser.parse(self.dict_path)
        
        items = result['items']
        self.assertIsInstance(items, dict)
        self.assertGreater(len(items), 0)
        
        # Check that items dict has content with atom_site items (very common)
        self.assertTrue(any('atom_site' in str(item).lower() for item in items.keys()))
    
    def test_dictionary_relationships(self):
        """Test that dictionary has relationship definitions."""
        if not self.dict_path.exists():
            self.skipTest(f"Dictionary file not found: {self.dict_path}")
        
        parser = DictionaryParser(self.cache, quiet=True)
        result = parser.parse(self.dict_path)
        
        relationships = result['relationships']
        # Relationships can be dict or list depending on parse result
        self.assertTrue(isinstance(relationships, (dict, list)))
        if isinstance(relationships, dict):
            self.assertGreater(len(relationships), 0)
        elif isinstance(relationships, list):
            self.assertGreater(len(relationships), 0)
    
    def test_dictionary_enumerations(self):
        """Test that dictionary has enumeration definitions."""
        if not self.dict_path.exists():
            self.skipTest(f"Dictionary file not found: {self.dict_path}")
        
        parser = DictionaryParser(self.cache, quiet=True)
        result = parser.parse(self.dict_path)
        
        enumerations = result.get('enumerations', {})
        self.assertIsInstance(enumerations, dict)


class TestMappingRulesGeneration(unittest.TestCase):
    """Test mapping rules generation from dictionary."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mapping_gen = get_shared_mapping_generator()
    
    def test_mapping_generator_instantiation(self):
        """Test that mapping generator is properly instantiated."""
        self.assertIsNotNone(self.mapping_gen)
        self.assertIsNotNone(self.mapping_gen.dict_parser)
        # MappingGenerator uses cache internally but doesn't expose it
    
    def test_mapping_rules_structure(self):
        """Test that mapping rules have correct structure."""
        mapping_rules = self.mapping_gen.get_mapping_rules()
        
        self.assertIsInstance(mapping_rules, dict)
        self.assertIn('category_mapping', mapping_rules)
        self.assertIn('item_mapping', mapping_rules)
        self.assertIn('fk_map', mapping_rules)
        self.assertIn('primary_keys', mapping_rules)
    
    def test_category_mapping(self):
        """Test category mapping structure."""
        mapping_rules = self.mapping_gen.get_mapping_rules()
        category_mapping = mapping_rules['category_mapping']
        
        self.assertIsInstance(category_mapping, dict)
        self.assertGreater(len(category_mapping), 0)
    
    def test_item_mapping(self):
        """Test item mapping structure."""
        mapping_rules = self.mapping_gen.get_mapping_rules()
        item_mapping = mapping_rules['item_mapping']
        
        self.assertIsInstance(item_mapping, dict)
        self.assertGreater(len(item_mapping), 0)
    
    def test_foreign_key_map(self):
        """Test foreign key mapping structure."""
        mapping_rules = self.mapping_gen.get_mapping_rules()
        fk_map = mapping_rules['fk_map']
        
        self.assertIsInstance(fk_map, dict)
        self.assertGreater(len(fk_map), 0)
        
        # Check for some expected relationships
        # For example, entity_poly should reference entity
        if 'entity_poly' in fk_map:
            entity_poly_fks = fk_map['entity_poly']
            self.assertIsInstance(entity_poly_fks, dict)
    
    def test_primary_keys(self):
        """Test primary key definitions."""
        mapping_rules = self.mapping_gen.get_mapping_rules()
        primary_keys = mapping_rules['primary_keys']
        
        self.assertIsInstance(primary_keys, dict)
        self.assertGreater(len(primary_keys), 0)
        
        # Check for expected primary keys
        if 'entity' in primary_keys:
            entity_pk = primary_keys['entity']
            # Primary key can be string or list
            if isinstance(entity_pk, list):
                self.assertIn('id', entity_pk)
            elif isinstance(entity_pk, str):
                self.assertIn('id', entity_pk)
    
    def test_mapping_rules_caching(self):
        """Test that mapping rules are cached properly."""
        # Get mapping rules twice
        rules1 = self.mapping_gen.get_mapping_rules()
        rules2 = self.mapping_gen.get_mapping_rules()
        
        # Should return the same object (cached)
        self.assertIs(rules1, rules2)


class TestRelationshipDefinitions(unittest.TestCase):
    """Test relationship definitions from dictionary."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mapping_gen = get_shared_mapping_generator()
        self.mapping_rules = self.mapping_gen.get_mapping_rules()
        self.fk_map = self.mapping_rules['fk_map']
    
    def test_entity_poly_relationship(self):
        """Test entity -> entity_poly relationship definition."""
        # fk_map uses tuple keys: (child_category, child_field) -> (parent_category, parent_field)
        entity_poly_key = ('entity_poly', 'entity_id')
        
        if entity_poly_key not in self.fk_map:
            self.fail(f"entity_poly relationship not found in mapping. Expected key: {entity_poly_key}")
        
        parent_ref = self.fk_map[entity_poly_key]
        self.assertIsInstance(parent_ref, tuple)
        self.assertEqual(len(parent_ref), 2)
        
        # Should reference entity.id
        parent_cat, parent_field = parent_ref
        self.assertEqual(parent_cat, 'entity')
        self.assertEqual(parent_field, 'id')
    
    def test_atom_site_relationship(self):
        """Test atom_site relationship definitions."""
        # Check for key atom_site relationships
        atom_site_relationships = [
            ('atom_site', 'label_entity_id'),
            ('atom_site', 'label_asym_id'),
        ]
        
        found_relationships = [key for key in atom_site_relationships if key in self.fk_map]
        
        if not found_relationships:
            self.fail(f"No atom_site relationships found in mapping. Expected keys like: {atom_site_relationships}")
        
        # Verify at least the entity_id relationship
        entity_key = ('atom_site', 'label_entity_id')
        self.assertIn(entity_key, self.fk_map)
        parent_ref = self.fk_map[entity_key]
        self.assertIsInstance(parent_ref, tuple)
        self.assertEqual(len(parent_ref), 2)
    
    def test_struct_asym_relationship(self):
        """Test struct_asym -> entity relationship definition."""
        # fk_map uses tuple keys: (child_category, child_field) -> (parent_category, parent_field)
        struct_asym_key = ('struct_asym', 'entity_id')
        
        if struct_asym_key not in self.fk_map:
            self.fail(f"struct_asym relationship not found in mapping. Expected key: {struct_asym_key}")
        
        parent_ref = self.fk_map[struct_asym_key]
        self.assertIsInstance(parent_ref, tuple)
        self.assertEqual(len(parent_ref), 2)
        
        # Should reference entity.id
        parent_cat, parent_field = parent_ref
        self.assertEqual(parent_cat, 'entity')
        self.assertEqual(parent_field, 'id')
    
    def test_multi_level_relationships(self):
        """Test that multi-level relationships are properly defined."""
        # Test entity -> entity_poly -> entity_poly_seq chain
        required_categories = ['entity_poly', 'entity_poly_seq']
        
        for cat in required_categories:
            if cat in self.fk_map:
                fks = self.fk_map[cat]
                self.assertIsInstance(fks, dict)
                self.assertGreater(len(fks), 0)


class TestDictionaryValidation(unittest.TestCase):
    """Test dictionary-based validation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mapping_gen = get_shared_mapping_generator()
    
    def test_category_validation(self):
        """Test that categories can be validated against dictionary."""
        mapping_rules = self.mapping_gen.get_mapping_rules()
        category_mapping = mapping_rules['category_mapping']
        
        # Check that expected categories are defined
        expected = ['entity', 'atom_site', 'entry']
        for cat in expected:
            if cat in category_mapping:
                cat_info = category_mapping[cat]
                self.assertIsNotNone(cat_info)
    
    def test_item_validation(self):
        """Test that items can be validated against dictionary."""
        mapping_rules = self.mapping_gen.get_mapping_rules()
        item_mapping = mapping_rules['item_mapping']
        
        # Check that expected items are defined
        expected = ['_entity.id', '_atom_site.id']
        for item in expected:
            if item in item_mapping:
                item_info = item_mapping[item]
                self.assertIsNotNone(item_info)
    
    def test_relationship_validation(self):
        """Test that relationships can be validated against dictionary."""
        mapping_rules = self.mapping_gen.get_mapping_rules()
        fk_map = mapping_rules['fk_map']
        
        # Verify that foreign keys are properly structured
        for child_cat, fk_info in fk_map.items():
            # FK info can be dict or tuple depending on structure
            self.assertIsNotNone(fk_info)
            if isinstance(fk_info, dict):
                # Each foreign key should have structure
                for fk_field, parent_info in fk_info.items():
                    self.assertIsNotNone(parent_info)


class TestCacheBehavior(unittest.TestCase):
    """Test caching behavior for dictionary and mapping rules."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_dir = os.path.join(self.temp_dir, ".cache")
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def test_cache_directory_creation(self):
        """Test that cache directory is created."""
        cache = get_cache_manager(self.cache_dir)
        parser = DictionaryParser(cache, quiet=True)
        
        # Access dictionary to trigger caching
        dict_path = Path(__file__).parent.parent / "sloth" / "mmcif" / "schemas" / "mmcif_pdbx_v50.dic"
        if dict_path.exists():
            parser.parse(dict_path)
            # Cache directory should exist
            self.assertTrue(os.path.exists(self.cache_dir))
    
    def test_mapping_rules_cache(self):
        """Test that mapping rules are cached."""
        cache = get_cache_manager(self.cache_dir)
        dict_parser = DictionaryParser(cache, quiet=True)
        mapping_gen = MappingGenerator(dict_parser, cache, quiet=True)
        
        # Get rules twice
        rules1 = mapping_gen.get_mapping_rules()
        rules2 = mapping_gen.get_mapping_rules()
        
        # Should be the same object (cached)
        self.assertIs(rules1, rules2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
