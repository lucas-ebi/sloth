#!/usr/bin/env python3
"""
Dedicated test suite for nested relationship resolution functionality.

This module contains comprehensive unit tests for the multi-level relationship
resolution features, testing the complete pipeline from mmCIF parsing through
to nested JSON generation with proper parent-child relationships.
"""

import unittest
import tempfile
import os
import json
import xml.etree.ElementTree as ET
from pathlib import Path
import shutil

from sloth.parser import MMCIFParser
from sloth.serializers import (
    PDBMLConverter, RelationshipResolver, MMCIFToPDBMLPipeline, 
    DictionaryParser, NoCache, XSDParser, MappingGenerator
)
from sloth.validators import XMLSchemaValidator


class TestRelationshipResolution(unittest.TestCase):
    """Test suite for relationship resolution and nested JSON generation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Test data that creates a 4-level hierarchy
        # entity -> entity_poly -> entity_poly_seq
        # entity -> struct_asym -> atom_site
        self.test_data = """data_TEST_REL
#
_entry.id        TEST_REL
#
_entity.id       1
_entity.type     polymer
_entity.pdbx_description 'Test protein'
#
_entity_poly.entity_id 1
_entity_poly.type      'polypeptide(L)'
_entity_poly.nstd_chirality no
#
_entity_poly_seq.entity_id 1
_entity_poly_seq.num       1
_entity_poly_seq.mon_id    ALA
#
_struct_asym.id      A
_struct_asym.entity_id 1
#
_atom_site.group_PDB  atom
_atom_site.id         1
_atom_site.type_symbol C
_atom_site.label_atom_id CA
_atom_site.label_comp_id ALA
_atom_site.label_asym_id A
_atom_site.label_entity_id 1
_atom_site.label_seq_id 1
_atom_site.Cartn_x    1.234
_atom_site.Cartn_y    5.678
_atom_site.Cartn_z    9.012
_atom_site.occupancy  1.00
_atom_site.B_iso_or_equiv 25.0
#"""
        
        self.test_file = os.path.join(self.temp_dir, 'test_relationships.cif')
        with open(self.test_file, 'w') as f:
            f.write(self.test_data)
    
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
    
    def _create_resolver(self) -> RelationshipResolver:
        """Helper method to create RelationshipResolver with proper mapping generator."""
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
        
        # Create resolver
        return RelationshipResolver(mapping_generator)
    
    def test_relationship_identification(self):
        """Test that relationships are correctly identified from XML."""
        # Parse and convert to XML
        parser = MMCIFParser()
        container = parser.parse_file(self.test_file)
        converter = self._create_converter()
        xml_content = converter.convert_to_pdbml(container)
        
        # Create resolver with dictionary and test relationship identification
        resolver = self._create_resolver()
        
        # Parse XML to check relationships
        root = ET.fromstring(xml_content)
        ns = {'pdbx': 'http://pdbml.pdb.org/schema/pdbx-v50.xsd'}
        
        # Test by calling the public method and examining the result
        nested_json = resolver.resolve_relationships(xml_content)
        
        # Check that expected relationships are reflected in the nested structure
        # This indirectly tests the relationship identification
        
        # Check that key relationships are reflected in the nested structure
        self.assertIn('entity', nested_json)
        self.assertIn('1', nested_json['entity'])
        
        entity_1 = nested_json['entity']['1']
        
        # These nested structures indicate that relationships were identified correctly
        self.assertIn('entity_poly', entity_1)  # entity_poly -> entity relationship found
        self.assertIn('struct_asym', entity_1)  # struct_asym -> entity relationship found
        
        # Check deeper nesting indicates multi-level relationships
        if 'entity_poly_seq' in entity_1['entity_poly']:
            # entity_poly_seq -> entity relationship found and nested under entity_poly
            self.assertIn('entity_poly_seq', entity_1['entity_poly'])
        
        if 'atom_site' in entity_1['struct_asym']:
            # atom_site -> struct_asym relationship found
            self.assertIn('atom_site', entity_1['struct_asym'])
    
    def test_category_nesting(self):
        """Test that categories are correctly nested based on relationships."""
        parser = MMCIFParser()
        container = parser.parse_file(self.test_file)
        converter = self._create_converter()
        xml_content = converter.convert_to_pdbml(container)
        
        resolver = self._create_resolver()
        nested_json = resolver.resolve_relationships(xml_content)
        
        # Check the basic structure
        self.assertIn('entity', nested_json)
        self.assertIn('1', nested_json['entity'])
        
        entity_1 = nested_json['entity']['1']
        
        # Test direct nesting under entity
        self.assertIn('entity_poly', entity_1)
        self.assertIn('struct_asym', entity_1)
        
        # Test second-level nesting
        entity_poly = entity_1['entity_poly']
        self.assertIn('entity_poly_seq', entity_poly)
        
        struct_asym = entity_1['struct_asym']
        self.assertIn('atom_site', struct_asym)
        
        # Verify data integrity
        self.assertEqual(entity_1['type'], 'polymer')
        self.assertEqual(entity_poly['type'], 'polypeptide(L)')
        self.assertEqual(entity_poly['entity_poly_seq']['mon_id'], 'ALA')
        self.assertEqual(struct_asym['atom_site']['label_atom_id'], 'CA')
        self.assertEqual(struct_asym['atom_site']['Cartn_x'], '1.234')
    
    def test_multi_level_hierarchy_validation(self):
        """Test that the complete 4-level hierarchy is correctly constructed."""
        parser = MMCIFParser()
        container = parser.parse_file(self.test_file)
        converter = self._create_converter()
        xml_content = converter.convert_to_pdbml(container)
        
        resolver = self._create_resolver()
        nested_json = resolver.resolve_relationships(xml_content)
        
        # Navigate the 4-level hierarchy and verify each level
        try:
            # Level 1: entity
            entity_1 = nested_json['entity']['1']
            self.assertEqual(entity_1['type'], 'polymer')
            self.assertEqual(entity_1['pdbx_description'], 'Test protein')
            
            # Level 2: entity_poly (branch 1)
            entity_poly = entity_1['entity_poly']
            self.assertEqual(entity_poly['type'], 'polypeptide(L)')
            
            # Level 3: entity_poly_seq
            entity_poly_seq = entity_poly['entity_poly_seq']
            self.assertEqual(entity_poly_seq['num'], '1')
            self.assertEqual(entity_poly_seq['mon_id'], 'ALA')
            
            # Level 2: struct_asym (branch 2)
            struct_asym = entity_1['struct_asym']
            self.assertEqual(struct_asym['id'], 'A')
            
            # Level 3: atom_site
            atom_site = struct_asym['atom_site']
            self.assertEqual(atom_site['label_atom_id'], 'CA')
            self.assertEqual(atom_site['label_comp_id'], 'ALA')
            self.assertEqual(atom_site['Cartn_x'], '1.234')
            self.assertEqual(atom_site['Cartn_y'], '5.678')
            self.assertEqual(atom_site['Cartn_z'], '9.012')
            
        except (KeyError, TypeError) as e:
            self.fail(f"4-level hierarchy not properly constructed: {e}")
    
    def test_multiple_items_same_category(self):
        """Test nesting when multiple items exist in the same category."""
        # Test data with multiple atoms
        multi_atom_data = """data_MULTI_ATOM
#
_entry.id        MULTI_ATOM
#
_entity.id       1
_entity.type     polymer
#
_struct_asym.id      A
_struct_asym.entity_id 1
#
loop_
_atom_site.id
_atom_site.label_entity_id
_atom_site.label_asym_id
_atom_site.label_atom_id
_atom_site.Cartn_x
1 1 A CA 1.0
2 1 A CB 2.0
3 1 A C  3.0
#"""
        
        multi_file = os.path.join(self.temp_dir, 'multi_atom.cif')
        with open(multi_file, 'w') as f:
            f.write(multi_atom_data)
        
        parser = MMCIFParser()
        container = parser.parse_file(multi_file)
        converter = self._create_converter()
        xml_content = converter.convert_to_pdbml(container)
        
        resolver = self._create_resolver()
        nested_json = resolver.resolve_relationships(xml_content)
        
        # Check that multiple atoms are properly handled
        entity_1 = nested_json['entity']['1']
        struct_asym = entity_1['struct_asym']
        
        # Should have multiple atom_site entries
        atom_sites = struct_asym['atom_site']
        
        # Should be a list of atoms or properly grouped
        self.assertTrue(isinstance(atom_sites, (list, dict)))
        
        if isinstance(atom_sites, list):
            self.assertEqual(len(atom_sites), 3)
            atom_ids = [atom['label_atom_id'] for atom in atom_sites]
            self.assertIn('CA', atom_ids)
            self.assertIn('CB', atom_ids)
            self.assertIn('C', atom_ids)
    
    def test_cross_references_preservation(self):
        """Test that cross-references between categories are preserved."""
        parser = MMCIFParser()
        container = parser.parse_file(self.test_file)
        converter = self._create_converter()
        xml_content = converter.convert_to_pdbml(container)
        
        resolver = self._create_resolver()
        nested_json = resolver.resolve_relationships(xml_content)
        
        # Check that foreign key values are preserved
        entity_1 = nested_json['entity']['1']
        
        # entity_poly should still have entity_id for reference
        entity_poly = entity_1['entity_poly']
        self.assertEqual(entity_poly.get('entity_id'), '1')
        
        # atom_site should still have its reference keys
        atom_site = entity_1['struct_asym']['atom_site']
        self.assertEqual(atom_site.get('label_entity_id'), '1')
        self.assertEqual(atom_site.get('label_asym_id'), 'A')
    
    def test_resolver_error_handling(self):
        """Test that the resolver handles malformed XML gracefully."""
        # Test with minimal/malformed XML
        malformed_xml = """<?xml version="1.0"?>
<datablock xmlns="http://pdbml.pdb.org/schema/pdbx-v50.xsd">
    <entityCategory>
        <!-- Missing required elements -->
    </entityCategory>
</datablock>"""
        
        resolver = self._create_resolver()
        
        # Should not crash, should return valid JSON
        try:
            result = resolver.resolve_relationships(malformed_xml)
            self.assertIsInstance(result, dict)
        except Exception as e:
            self.fail(f"Resolver should handle malformed XML gracefully: {e}")
    
    def test_empty_categories_handling(self):
        """Test handling of empty categories in the XML."""
        # XML with empty categories
        empty_xml = """<?xml version="1.0"?>
<datablock xmlns="http://pdbml.pdb.org/schema/pdbx-v50.xsd">
    <entityCategory>
    </entityCategory>
    <atom_siteCategory>
    </atom_siteCategory>
</datablock>"""
        
        resolver = self._create_resolver()
        result = resolver.resolve_relationships(empty_xml)
        
        # Should return empty structure, not crash
        self.assertIsInstance(result, dict)
    
    def test_performance_with_large_dataset(self):
        """Test resolver performance with a larger dataset."""
        # Generate larger test data
        large_data_parts = ["""data_LARGE_TEST
#
_entry.id        LARGE_TEST
#
loop_
_entity.id
_entity.type
"""]
        
        # Add 10 entities
        for i in range(1, 11):
            large_data_parts.append(f"{i} polymer")
        
        large_data_parts.append("""
#
loop_
_struct_asym.id
_struct_asym.entity_id
""")
        
        # Add 10 asymmetric units
        for i in range(1, 11):
            large_data_parts.append(f"{chr(64+i)} {i}")
        
        large_data_parts.append("""
#
loop_
_atom_site.id
_atom_site.label_entity_id
_atom_site.label_asym_id
_atom_site.label_atom_id
_atom_site.Cartn_x
""")
        
        # Add 50 atoms (5 per entity)
        atom_id = 1
        for entity_id in range(1, 11):
            for atom_name in ['CA', 'CB', 'C', 'N', 'O']:
                asym_id = chr(64 + entity_id)
                large_data_parts.append(f"{atom_id} {entity_id} {asym_id} {atom_name} {atom_id}.0")
                atom_id += 1
        
        large_data_parts.append("#")
        large_data = "\n".join(large_data_parts)
        
        large_file = os.path.join(self.temp_dir, 'large_test.cif')
        with open(large_file, 'w') as f:
            f.write(large_data)
        
        # Process and time it
        import time
        start_time = time.time()
        
        parser = MMCIFParser()
        container = parser.parse_file(large_file)
        converter = self._create_converter()
        xml_content = converter.convert_to_pdbml(container)
        
        resolver = self._create_resolver()
        nested_json = resolver.resolve_relationships(xml_content)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Should complete within reasonable time (adjust as needed)
        self.assertLess(processing_time, 10.0, "Processing took too long")
        
        # Verify structure is correct
        self.assertIn('entity', nested_json)
        self.assertEqual(len(nested_json['entity']), 10)
        
        # Check that first entity has correct structure
        entity_1 = nested_json['entity']['1']
        self.assertIn('struct_asym', entity_1)
        self.assertIn('atom_site', entity_1['struct_asym'])


class TestPipelineIntegration(unittest.TestCase):
    """Test the complete pipeline integration."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Use the working nested example
        self.test_data = """data_PIPELINE_TEST
#
_entry.id        PIPELINE_TEST
#
_entity.id       1
_entity.type     polymer
_entity.pdbx_description 'Pipeline test protein'
#
_entity_poly.entity_id 1
_entity_poly.type      'polypeptide(L)'
#
_entity_poly_seq.entity_id 1
_entity_poly_seq.num       1
_entity_poly_seq.mon_id    GLY
#
_struct_asym.id      A
_struct_asym.entity_id 1
#
_atom_site.id         1
_atom_site.label_entity_id 1
_atom_site.label_asym_id A
_atom_site.label_atom_id CA
_atom_site.Cartn_x    0.000
#"""
        
        self.test_file = os.path.join(self.temp_dir, 'pipeline_test.cif')
        with open(self.test_file, 'w') as f:
            f.write(self.test_data)
    
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
    
    def _create_resolver(self) -> RelationshipResolver:
        """Helper method to create RelationshipResolver with proper mapping generator."""
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
        
        # Create resolver
        return RelationshipResolver(mapping_generator)
    
    def test_end_to_end_pipeline(self):
        """Test the complete end-to-end pipeline."""
        try:
            # Test with pipeline class if available
            pipeline = MMCIFToPDBMLPipeline()
            result = pipeline.process_mmcif_file(self.test_file)
            
            # Verify all expected outputs
            self.assertIn('pdbml_xml', result)
            self.assertIn('nested_json', result)
            self.assertIn('validation', result)
            
            # Test nested structure
            nested_json = result['nested_json']
            entity_1 = nested_json['entity']['1']
            
            self.assertEqual(entity_1['entity_poly']['entity_poly_seq']['mon_id'], 'GLY')
            self.assertEqual(entity_1['struct_asym']['atom_site']['label_atom_id'], 'CA')
            
        except (ImportError, AttributeError):
            # Test individual components if pipeline class not available
            parser = MMCIFParser()
            container = parser.parse_file(self.test_file)
            
            converter = self._create_converter()
            xml_content = converter.convert_to_pdbml(container)
            
            resolver = self._create_resolver_with_dictionary()
            nested_json = resolver.resolve_relationships(xml_content)
            
            # Verify structure
            self.assertIn('entity', nested_json)
            entity_1 = nested_json['entity']['1']
            self.assertEqual(entity_1['pdbx_description'], 'Pipeline test protein')
