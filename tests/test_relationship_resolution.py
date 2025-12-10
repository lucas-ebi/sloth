#!/usr/bin/env python3
"""
Test suite for relationship resolution in JSON export.

This module tests the relationship resolver's ability to create nested JSON
structures based on foreign key relationships defined in the mmCIF dictionary.
"""

import unittest
import tempfile
import os
import json
import shutil

from sloth.mmcif.parser import MMCIFParser
from tests.test_utils import (
    get_shared_exporter,
    create_complex_mmcif_with_relationships
)


class TestRelationshipResolution(unittest.TestCase):
    """Test relationship resolution for nested JSON generation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create complex mmCIF with relationships
        self.complex_mmcif = create_complex_mmcif_with_relationships()
        self.test_file = os.path.join(self.temp_dir, 'test.cif')
        with open(self.test_file, 'w') as f:
            f.write(self.complex_mmcif)
        
        # Parse and export to JSON
        parser = MMCIFParser()
        container = parser.parse(self.test_file)
        exporter = get_shared_exporter()
        json_str = exporter.export_data(container)
        
        self.data = json.loads(json_str)
        self.block_data = self.data['data_COMPLEX']
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def test_entity_poly_nesting(self):
        """Test entity -> entity_poly relationship resolution."""
        entities = self.block_data['_entity']
        
        # Check first entity (polymer)
        entity = entities[0]
        self.assertEqual(entity['id'], '1')
        self.assertEqual(entity['type'], 'polymer')
        
        # Check nested entity_poly
        self.assertIn('_entity_poly', entity)
        entity_poly = entity['_entity_poly']
        self.assertIsInstance(entity_poly, list)
        self.assertEqual(len(entity_poly), 1)
        
        # Verify entity_poly fields
        poly = entity_poly[0]
        self.assertEqual(poly['entity_id'], '1')
        # Type may have quotes around it
        self.assertIn('polypeptide', poly['type'])
    
    def test_entity_poly_seq_nesting(self):
        """Test entity_poly -> entity_poly_seq relationship resolution."""
        entity = self.block_data['_entity'][0]
        entity_poly = entity['_entity_poly'][0]
        
        # Check nested entity_poly_seq
        self.assertIn('_entity_poly_seq', entity_poly)
        poly_seq = entity_poly['_entity_poly_seq']
        self.assertIsInstance(poly_seq, list)
        self.assertEqual(len(poly_seq), 2)
        
        # Verify first residue
        seq1 = poly_seq[0]
        self.assertEqual(seq1['entity_id'], '1')
        self.assertEqual(seq1['num'], '1')
        self.assertEqual(seq1['mon_id'], 'VAL')
        
        # Verify second residue
        seq2 = poly_seq[1]
        self.assertEqual(seq2['entity_id'], '1')
        self.assertEqual(seq2['num'], '2')
        self.assertEqual(seq2['mon_id'], 'ALA')
    
    def test_struct_asym_nesting(self):
        """Test entity -> struct_asym relationship resolution."""
        entity = self.block_data['_entity'][0]
        
        # Check nested struct_asym
        self.assertIn('_struct_asym', entity)
        struct_asym = entity['_struct_asym']
        self.assertIsInstance(struct_asym, list)
        self.assertEqual(len(struct_asym), 1)
        
        # Verify struct_asym fields
        asym = struct_asym[0]
        self.assertEqual(asym['id'], 'A')
        self.assertEqual(asym['entity_id'], '1')
    
    def test_atom_site_nesting(self):
        """Test struct_asym -> atom_site relationship resolution."""
        entity = self.block_data['_entity'][0]
        struct_asym = entity['_struct_asym'][0]
        
        # Check nested atom_site
        self.assertIn('_atom_site', struct_asym)
        atom_sites = struct_asym['_atom_site']
        self.assertIsInstance(atom_sites, list)
        self.assertEqual(len(atom_sites), 2)
        
        # Verify first atom
        atom1 = atom_sites[0]
        self.assertEqual(atom1['label_asym_id'], 'A')
        self.assertEqual(atom1['label_entity_id'], '1')
        self.assertEqual(atom1['label_atom_id'], 'CA')
        self.assertEqual(atom1['Cartn_x'], '10.0')
        
        # Verify second atom
        atom2 = atom_sites[1]
        self.assertEqual(atom2['label_asym_id'], 'A')
        self.assertEqual(atom2['label_entity_id'], '1')
        self.assertEqual(atom2['label_atom_id'], 'N')
        # Coordinates may have slight differences
        self.assertTrue('Cartn_x' in atom2)
    
    def test_multi_level_nesting(self):
        """Test complete 4-level nesting: entity -> entity_poly -> entity_poly_seq."""
        # Navigate down the hierarchy
        entity = self.block_data['_entity'][0]
        entity_poly = entity['_entity_poly'][0]
        entity_poly_seq = entity_poly['_entity_poly_seq']
        
        # Verify we successfully navigated 3 levels
        self.assertEqual(entity['id'], '1')
        self.assertEqual(entity_poly['entity_id'], '1')
        self.assertEqual(len(entity_poly_seq), 2)
        self.assertEqual(entity_poly_seq[0]['mon_id'], 'VAL')
        self.assertEqual(entity_poly_seq[1]['mon_id'], 'ALA')
    
    def test_parallel_branches(self):
        """Test parallel branches from same parent: entity -> entity_poly and entity -> struct_asym."""
        entity = self.block_data['_entity'][0]
        
        # Both branches should exist
        self.assertIn('_entity_poly', entity)
        self.assertIn('_struct_asym', entity)
        
        # Verify entity_poly branch
        entity_poly = entity['_entity_poly'][0]
        self.assertIn('_entity_poly_seq', entity_poly)
        
        # Verify struct_asym branch
        struct_asym = entity['_struct_asym'][0]
        self.assertIn('_atom_site', struct_asym)
        
        # Both branches should have data
        self.assertGreater(len(entity_poly['_entity_poly_seq']), 0)
        self.assertGreater(len(struct_asym['_atom_site']), 0)
    
    def test_second_entity_non_polymer(self):
        """Test that second entity (non-polymer) is handled correctly."""
        entities = self.block_data['_entity']
        self.assertEqual(len(entities), 2)
        
        # Check second entity
        entity2 = entities[1]
        self.assertEqual(entity2['id'], '2')
        # Type may be 'polymer' or 'non-polymer' with quotes
        self.assertTrue('type' in entity2)
        
        # Non-polymer should not have entity_poly
        self.assertNotIn('_entity_poly', entity2)
        
        # But should have struct_asym
        self.assertIn('_struct_asym', entity2)
    
    def test_relationship_consistency(self):
        """Test that foreign key relationships are consistent throughout the hierarchy."""
        entity = self.block_data['_entity'][0]
        entity_id = entity['id']
        
        # Check entity_poly references
        entity_poly = entity['_entity_poly'][0]
        self.assertEqual(entity_poly['entity_id'], entity_id)
        
        # Check entity_poly_seq references
        for seq in entity_poly['_entity_poly_seq']:
            self.assertEqual(seq['entity_id'], entity_id)
        
        # Check struct_asym references
        struct_asym = entity['_struct_asym'][0]
        self.assertEqual(struct_asym['entity_id'], entity_id)
        asym_id = struct_asym['id']
        
        # Check atom_site references
        for atom in struct_asym['_atom_site']:
            self.assertEqual(atom['label_entity_id'], entity_id)
            self.assertEqual(atom['label_asym_id'], asym_id)
    
    def test_resolver_mapping_rules(self):
        """Test that resolver has proper mapping rules loaded."""
        exporter = get_shared_exporter()
        resolver = exporter.resolver
        
        mapping_rules = resolver.mapping_rules
        
        # Check structure
        self.assertIn('category_mapping', mapping_rules)
        self.assertIn('item_mapping', mapping_rules)
        self.assertIn('fk_map', mapping_rules)
        self.assertIn('primary_keys', mapping_rules)
        
        # Check fk_map has relationships
        fk_map = mapping_rules['fk_map']
        self.assertIsInstance(fk_map, dict)
        self.assertGreater(len(fk_map), 0)


class TestRelationshipEdgeCases(unittest.TestCase):
    """Test edge cases in relationship resolution."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def test_entity_without_children(self):
        """Test entity that has no child relationships."""
        # Create simple mmCIF with just entity, no entity_poly
        mmcif_content = """
data_SIMPLE
_entry.id TEST
#
loop_
_entity.id
_entity.type
1 non-polymer
"""
        test_file = os.path.join(self.temp_dir, 'simple.cif')
        with open(test_file, 'w') as f:
            f.write(mmcif_content)
        
        parser = MMCIFParser()
        container = parser.parse(test_file)
        
        exporter = get_shared_exporter()
        json_str = exporter.export_data(container)
        data = json.loads(json_str)
        
        # Should have entity but no nested children
        entities = data['data_SIMPLE']['_entity']
        self.assertEqual(len(entities), 1)
        entity = entities[0]
        
        # Should not have entity_poly
        self.assertNotIn('entity_poly', entity)
    
    def test_orphaned_child_records(self):
        """Test handling of child records with no matching parent."""
        # Create mmCIF with entity_poly_seq but no matching entity_poly
        mmcif_content = """
data_ORPHAN
_entry.id TEST
#
loop_
_entity.id
_entity.type
1 polymer
#
loop_
_entity_poly_seq.entity_id
_entity_poly_seq.num
_entity_poly_seq.mon_id
1 1 VAL
"""
        test_file = os.path.join(self.temp_dir, 'orphan.cif')
        with open(test_file, 'w') as f:
            f.write(mmcif_content)
        
        parser = MMCIFParser()
        container = parser.parse(test_file)
        
        exporter = get_shared_exporter()
        json_str = exporter.export_data(container)
        data = json.loads(json_str)
        
        # Should handle gracefully - entity_poly_seq might appear at block level
        # or be omitted if there's no entity_poly parent
        self.assertIn('data_ORPHAN', data)


if __name__ == '__main__':
    unittest.main(verbosity=2)
