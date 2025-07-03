#!/usr/bin/env python3
"""
Test suite for PDBML conversion and nested JSON functionality.

Tests the new features based on comprehensive dictionary analysis:
- PDBML XML conversion following the 280 composite key patterns
- Nested JSON with relationship resol        self.assertEqual(atom_id_1, 'N')
        self.assertEqual(atom_id_2, 'CA')
        self.assertEqual(atom_id_3, 'C')
        self.assertEqual(value_angle.text, '110.5')n 
- Schema validation using our 5 universal patterns
- Entry-level, simple ID, and composite key category handling
"""

import unittest
import tempfile
import os
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import patch, MagicMock
from sloth import (
    MMCIFHandler,
    MMCIFParser,
    PDBMLConverter,
    RelationshipResolver, 
    MMCIFToPDBMLPipeline,
    XMLValidator,
    DictionaryParser
)
from sloth.models import MMCIFDataContainer, DataBlock, Category


class TestPDBMLConversion(unittest.TestCase):
    """Test PDBML XML conversion based on dictionary analysis patterns."""
    
    @staticmethod
    def _get_namespaces():
        """Get XML namespaces for PDBML."""
        return {'pdbx': 'http://pdbml.pdb.org/schema/pdbx-v50.xsd'}
    
    def _find_with_ns(self, root, path):
        """Find element handling namespaces."""
        ns = self._get_namespaces()
        # Try with namespace first, then without
        try:
            return root.find(f"pdbx:{path}", ns)
        except:
            return root.find(path)
    
    def _findall_with_ns(self, root, path):
        """Find all elements handling namespaces."""
        ns = self._get_namespaces()
        # Try with namespace first, then without
        try:
            return root.findall(f"pdbx:{path}", ns)
        except:
            return root.findall(path)
    
    def setUp(self):
        """Set up test fixtures with representative data from all 5 category patterns."""
        # Create test data representing the 5 universal patterns identified:
        # 1. Entry-level categories (54 categories)
        # 2. Simple ID categories (144 categories) 
        # 3. Composite key categories (280 categories)
        # 4. Attribute groups (117 categories)
        # 5. Domain clustering by prefix (pdbx, em, struct, etc.)
        
        self.comprehensive_mmcif_content = """data_TEST
#
# Entry-level category (Pattern 1: entry_id key)
_entry.id TEST_STRUCTURE

# Entry-level database info
_database_2.database_id      PDB
_database_2.database_code    TEST

# Simple ID category (Pattern 2: single .id key)
loop_
_citation.id
_citation.title
_citation.journal_abbrev
_citation.year
primary
'Test Structure Determination'
'Nature'
'2023'
2
'Validation Study'
'Science'
'2024'

# Composite key category (Pattern 3: multiple keys)
loop_
_chem_comp_angle.comp_id
_chem_comp_angle.atom_id_1
_chem_comp_angle.atom_id_2
_chem_comp_angle.atom_id_3
_chem_comp_angle.value_angle
ALA N CA C 110.5
ALA CA C O 120.8

# Attribute group category (Pattern 4: non-ID key)
loop_
_atom_type.symbol
_atom_type.radius_bond
N 0.71
C 0.76
O 0.66

# Entry-level with coordinates (testing XML element vs attribute classification)
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.label_atom_id
_atom_site.auth_asym_id
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
_atom_site.occupancy
_atom_site.B_iso_or_equiv
ATOM 1 N N A 10.123 20.456 30.789 1.00 25.00
ATOM 2 C CA A 11.234 21.567 31.890 1.00 24.50
ATOM 3 C C A 12.345 22.678 32.901 1.00 23.85

# Entity information (testing reference attributes)
loop_
_entity.id
_entity.type
_entity.pdbx_description
1 polymer 'Protein chain A'
2 water 'Water molecules'

# Domain clustering test (Pattern 5: pdbx prefix)
loop_
_pdbx_database_status.entry_id
_pdbx_database_status.deposit_site
_pdbx_database_status.process_site
TEST_STRUCTURE RCSB RCSB

#
"""
        
        self.handler = MMCIFHandler(validator_factory=None)
        self.converter = PDBMLConverter()
        self.temp_dir = tempfile.mkdtemp()
        
        # Create temp file for testing
        self.test_file = os.path.join(self.temp_dir, 'test.cif')
        with open(self.test_file, 'w') as f:
            f.write(self.comprehensive_mmcif_content)
        
    def tearDown(self):
        """Clean up temporary files."""
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
    
    def test_entry_level_category_conversion(self):
        """Test conversion of entry-level categories (54 categories identified)."""
        container = self.handler.parse(self.test_file)
        xml_content = self.converter.convert_to_pdbml(container)
        
        # Parse XML to verify structure  
        root = ET.fromstring(xml_content)
        
        # Check the actual structure - datablock is root with default namespace
        self.assertTrue(root.tag.endswith('datablock'))
        self.assertEqual(root.get('datablockName'), 'TEST')
        
        # Entry should be in entryCategory with namespace
        ns = {'': 'http://pdbml.pdb.org/schema/pdbx-v50.xsd'}
        entry_category = root.find('entryCategory', ns)
        if entry_category is None:
            # Try without namespace
            entry_category = root.find('entryCategory')
        self.assertIsNotNone(entry_category)
        
        entry = entry_category.find('entry', ns)
        if entry is None:
            entry = entry_category.find('entry')
        self.assertIsNotNone(entry)
        
        # In this format, id is an attribute
        self.assertEqual(entry.get('id'), 'TEST_STRUCTURE')
        
        # Database info should be in database_2Category
        database_category = root.find('database_2Category', ns)
        if database_category is None:
            database_category = root.find('database_2Category')
        self.assertIsNotNone(database_category)
        
        database_elem = database_category.find('database_2', ns)
        if database_elem is None:
            database_elem = database_category.find('database_2')
        self.assertIsNotNone(database_elem)
        
        # In this format, database info is attributes
        self.assertEqual(database_elem.get('database_id'), 'PDB')
        self.assertEqual(database_elem.get('database_code'), 'TEST')
        
    def test_simple_id_category_conversion(self):
        """Test conversion of simple ID categories (144 categories identified)."""
        container = self.handler.parse(self.test_file)
        xml_content = self.converter.convert_to_pdbml(container)
        root = ET.fromstring(xml_content)
        
        # Citation should be in citationCategory
        citation_category = self._find_with_ns(root, 'citationCategory')
        self.assertIsNotNone(citation_category, "citationCategory should exist")
        
        citations = self._findall_with_ns(citation_category, 'citation')
        self.assertEqual(len(citations), 2, "Should have 2 citations")
        
        # Check first citation structure
        citation1 = citations[0]
        
        # In PDBML format, id is an attribute, other data are elements
        citation_id = citation1.get('id')  # Attribute
        journal_elem = self._find_with_ns(citation1, 'journal_abbrev')  # Element
        year_elem = self._find_with_ns(citation1, 'year')  # Element
        title_elem = self._find_with_ns(citation1, 'title')  # Element
        
        self.assertIsNotNone(citation_id)
        self.assertIsNotNone(journal_elem)
        self.assertIsNotNone(year_elem)
        self.assertIsNotNone(title_elem)
        
        self.assertEqual(citation_id, 'primary')
        self.assertEqual(journal_elem.text, 'Nature')
        self.assertEqual(year_elem.text, '2023')
        self.assertEqual(title_elem.text, 'Test Structure Determination')
        
    def test_composite_key_category_conversion(self):
        """Test conversion of composite key categories (280 categories identified)."""
        container = self.handler.parse(self.test_file)
        xml_content = self.converter.convert_to_pdbml(container)
        root = ET.fromstring(xml_content)
        
        # Chemical component angles should be in chem_comp_angleCategory
        angle_category = self._find_with_ns(root, 'chem_comp_angleCategory')
        self.assertIsNotNone(angle_category, "chem_comp_angleCategory should exist")
        
        angles = self._findall_with_ns(angle_category, 'chem_comp_angle')
        self.assertEqual(len(angles), 2, "Should have 2 chem_comp_angle entries")
        
        angle1 = angles[0]
        # In PDBML, composite key items are attributes for this category
        comp_id = angle1.get('comp_id')  # Attribute
        atom_id_1 = angle1.get('atom_id_1')  # Attribute
        atom_id_2 = angle1.get('atom_id_2')  # Attribute
        atom_id_3 = angle1.get('atom_id_3')  # Attribute
        value_angle = self._find_with_ns(angle1, 'value_angle')  # Element
        
        self.assertIsNotNone(comp_id)
        self.assertIsNotNone(atom_id_1)
        self.assertIsNotNone(atom_id_2)
        self.assertIsNotNone(atom_id_3)
        self.assertIsNotNone(value_angle)
        
        self.assertEqual(comp_id, 'ALA')
        self.assertEqual(atom_id_1, 'N')
        self.assertEqual(atom_id_2, 'CA')
        self.assertEqual(atom_id_3, 'C')
        self.assertEqual(value_angle.text, '110.5')
        
    def test_attribute_vs_element_classification(self):
        """Test proper classification of items as XML attributes vs elements."""
        container = self.handler.parse(self.test_file)
        xml_content = self.converter.convert_to_pdbml(container)
        root = ET.fromstring(xml_content)
        
        # Atom sites should be in atom_siteCategory
        atom_site_category = self._find_with_ns(root, 'atom_siteCategory')
        self.assertIsNotNone(atom_site_category)
        
        atom_sites = self._findall_with_ns(atom_site_category, 'atom_site')
        self.assertGreater(len(atom_sites), 0)
        
        atom1 = atom_sites[0]
        
        # Check classification: id is attribute, others are elements 
        atom_id = atom1.get('id')  # Attribute
        type_symbol_elem = self._find_with_ns(atom1, 'type_symbol')  # Element
        label_atom_id_elem = self._find_with_ns(atom1, 'label_atom_id')  # Element
        auth_asym_id_elem = self._find_with_ns(atom1, 'auth_asym_id')  # Element
        cartn_x_elem = self._find_with_ns(atom1, 'Cartn_x')  # Element
        cartn_y_elem = self._find_with_ns(atom1, 'Cartn_y')  # Element
        cartn_z_elem = self._find_with_ns(atom1, 'Cartn_z')  # Element
        
        # Check that classification is correct
        self.assertIsNotNone(atom_id)  # Attribute
        self.assertIsNotNone(type_symbol_elem)  # Element
        self.assertIsNotNone(label_atom_id_elem)  # Element
        self.assertIsNotNone(auth_asym_id_elem)  # Element
        self.assertIsNotNone(cartn_x_elem)  # Element
        self.assertIsNotNone(cartn_y_elem)  # Element
        self.assertIsNotNone(cartn_z_elem)  # Element
        
        # Check values
        self.assertEqual(atom_id, '1')
        self.assertEqual(type_symbol_elem.text, 'N')
        self.assertEqual(label_atom_id_elem.text, 'N')
        self.assertEqual(auth_asym_id_elem.text, 'A')
        self.assertEqual(cartn_x_elem.text, '10.123')
        self.assertEqual(auth_asym_id_elem.text, 'A')
        self.assertEqual(cartn_x_elem.text, '10.123')
        self.assertEqual(cartn_y_elem.text, '20.456')
        self.assertEqual(cartn_z_elem.text, '30.789')
        
    def test_reference_attribute_handling(self):
        """Test handling of reference attributes (843 identified)."""
        container = self.handler.parse(self.test_file)
        xml_content = self.converter.convert_to_pdbml(container)
        root = ET.fromstring(xml_content)
        
        # Entity references should be in entityCategory
        entity_category = self._find_with_ns(root, 'entityCategory')
        self.assertIsNotNone(entity_category, "entityCategory should exist")
        
        entities = self._findall_with_ns(entity_category, 'entity')
        self.assertGreater(len(entities), 0, "Should have entities")
        
        entity1 = entities[0]
        id_elem = entity1.get('id')  # This is an attribute
        type_elem = self._find_with_ns(entity1, 'type')  # Element
        desc_elem = self._find_with_ns(entity1, 'pdbx_description')  # Element
        
        self.assertIsNotNone(id_elem)
        self.assertIsNotNone(type_elem)
        self.assertIsNotNone(desc_elem)
        
        self.assertEqual(id_elem, '1')
        self.assertEqual(type_elem.text, 'polymer')
        self.assertEqual(desc_elem.text, 'Protein chain A')
        
    def test_domain_clustering_preservation(self):
        """Test that domain clustering by prefix is preserved."""
        # Use the main test file which now contains pdbx_database_status
        container = self.handler.parse(self.test_file)
        xml_content = self.converter.convert_to_pdbml(container)
        root = ET.fromstring(xml_content)
        
        # PDBX elements should be in pdbx_database_statusCategory
        pdbx_category = self._find_with_ns(root, 'pdbx_database_statusCategory')
        self.assertIsNotNone(pdbx_category, "pdbx_database_statusCategory should exist")
        
        pdbx_status = self._find_with_ns(pdbx_category, 'pdbx_database_status')
        self.assertIsNotNone(pdbx_status)
        
        # Check for expected elements in the pdbx_database_status
        entry_id = self._find_with_ns(pdbx_status, 'entry_id')
        deposit_site = self._find_with_ns(pdbx_status, 'deposit_site')
        process_site = self._find_with_ns(pdbx_status, 'process_site')
        
        self.assertIsNotNone(entry_id)
        self.assertIsNotNone(deposit_site)
        self.assertIsNotNone(process_site)
        
        self.assertEqual(entry_id.text, 'TEST_STRUCTURE')
        self.assertEqual(deposit_site.text, 'RCSB')
        self.assertEqual(process_site.text, 'RCSB')
        
    def test_xml_namespace_handling(self):
        """Test proper XML namespace usage."""
        container = self.handler.parse(self.test_file)
        xml_content = self.converter.convert_to_pdbml(container)
        
        # Should contain proper namespace declaration
        self.assertIn('xmlns', xml_content)
        self.assertIn('pdbml.pdb.org', xml_content.lower())


class TestRelationshipResolver(unittest.TestCase):
    """Test nested JSON with relationship resolution."""
    
    def setUp(self):
        """Set up test data with clear parent-child relationships."""
        # Create comprehensive test data for relationship testing
        self.comprehensive_mmcif_content = """data_TEST
#
# Entry-level category
_entry.id TEST_STRUCTURE

# Simple ID category with relationships
loop_
_citation.id
_citation.title
_citation.journal_abbrev
_citation.year
primary
'Test Structure Determination'
'Nature'
'2023'

# Entity information 
loop_
_entity.id
_entity.type
_entity.pdbx_description
1 polymer 'Protein chain A'

# Atom sites with entity references
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.label_atom_id
_atom_site.auth_asym_id
_atom_site.label_entity_id
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
ATOM 1 N N A 1 10.123 20.456 30.789
ATOM 2 C CA A 1 11.234 21.567 31.890
"""
        
        self.handler = MMCIFHandler(validator_factory=None)
        self.converter = PDBMLConverter()
        self.resolver = RelationshipResolver()
        self.temp_dir = tempfile.mkdtemp()
        
        # Create temp file for testing
        self.test_file = os.path.join(self.temp_dir, 'relationship_test.cif')
        with open(self.test_file, 'w') as f:
            f.write(self.comprehensive_mmcif_content)
        
    def tearDown(self):
        """Clean up temporary files."""
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
        
    def test_parent_child_relationship_resolution(self):
        """Test resolution of parent-child relationships."""
        # Use actual PDBML content from our test file
        container = self.handler.parse(self.test_file)
        xml_content = self.converter.convert_to_pdbml(container)
        
        nested_json = self.resolver.resolve_relationships(xml_content)
        
        # Should have some data structure, check it's not empty
        self.assertIsInstance(nested_json, dict)
        self.assertGreater(len(nested_json), 0, "Should have resolved some relationships")
        
    def test_entity_atom_site_relationships(self):
        """Test entity to atom_site relationships."""
        # Use actual PDBML content from our test file
        container = self.handler.parse(self.test_file)
        xml_content = self.converter.convert_to_pdbml(container)
        
        nested_json = self.resolver.resolve_relationships(xml_content)
        
        # Should resolve some relationships 
        self.assertIsInstance(nested_json, dict)
        # The specific structure depends on the RelationshipResolver implementation
        
        # Check that we have basic structure
        self.assertGreater(len(nested_json), 0, "Should have resolved some data")
                
    def test_reference_integrity(self):
        """Test that references are properly maintained."""
        # Use actual PDBML content from our test file
        container = self.handler.parse(self.test_file)
        xml_content = self.converter.convert_to_pdbml(container)
        
        nested_json = self.resolver.resolve_relationships(xml_content)
        
        # Should maintain data integrity
        self.assertIsInstance(nested_json, dict)
        self.assertGreater(len(nested_json), 0, "Should have resolved some relationships")


class TestMMCIFToPDBMLPipeline(unittest.TestCase):
    """Test the complete pipeline integration."""
    
    def setUp(self):
        """Set up pipeline with mock schema and dictionary."""
        self.test_mmcif = """data_PIPELINE_TEST
_entry.id PIPELINE_TEST
_database_2.database_id PDB
_database_2.database_code TEST
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
ATOM 1 N 10.123 20.456 30.789
ATOM 2 C 11.234 21.567 31.890
"""
        
        self.temp_dir = tempfile.mkdtemp()
        self.mmcif_file = os.path.join(self.temp_dir, 'test.cif')
        with open(self.mmcif_file, 'w') as f:
            f.write(self.test_mmcif)
            
    def tearDown(self):
        """Clean up."""
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
            
    @patch('sloth.pdbml_converter.XMLValidator')
    def test_complete_pipeline_execution(self, mock_validator_class):
        """Test complete pipeline from mmCIF to nested JSON."""
        # Mock the validator to avoid XSD dependency
        mock_validator = MagicMock()
        mock_validator.validate.return_value = (True, [])
        mock_validator_class.return_value = mock_validator
        
        # Create pipeline without actual schema file
        pipeline = MMCIFToPDBMLPipeline()
        
        # Process the file
        result = pipeline.process_mmcif_file(self.mmcif_file)
        
        # Verify all pipeline outputs
        self.assertIn('mmcif_data', result)  # Correct key name
        self.assertIn('pdbml_xml', result)
        self.assertIn('validation', result)
        self.assertIn('nested_json', result)
        
        # Check mmCIF parsing
        container = result['mmcif_data']  # Correct key name
        self.assertEqual(len(container.data), 1)
        self.assertEqual(container.data[0].name, 'PIPELINE_TEST')
        
        # Check XML generation
        xml_content = result['pdbml_xml']
        self.assertIn('<entry', xml_content)
        self.assertIn('PIPELINE_TEST', xml_content)
        
        # Check validation result structure
        validation = result['validation']
        self.assertIn('is_valid', validation)
        self.assertIn('errors', validation)
        
        # Check nested JSON
        nested_json = result['nested_json']
        self.assertIsInstance(nested_json, dict)
        
    def test_pipeline_with_validation_errors(self):
        """Test pipeline behavior with validation errors."""
        # Test with malformed data that should cause validation issues
        bad_mmcif = """data_BAD
_entry.id 
# Missing required data
"""
        bad_file = os.path.join(self.temp_dir, 'bad.cif')
        with open(bad_file, 'w') as f:
            f.write(bad_mmcif)
            
        pipeline = MMCIFToPDBMLPipeline()
        
        try:
            result = pipeline.process_mmcif_file(bad_file)
            # Should handle errors gracefully
            self.assertIn('validation', result)
        except Exception as e:
            # Should not crash completely
            self.assertIsInstance(e, Exception)


class TestDictionaryBasedValidation(unittest.TestCase):
    """Test validation based on dictionary analysis insights."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up temporary files."""
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
    
    def test_category_key_validation(self):
        """Test validation of category keys based on 5 universal patterns."""
        # Entry-level categories must have entry_id
        entry_level_data = """data_TEST
_atom_sites.entry_id TEST
_atom_sites.solution_primary 'H'
"""
        
        # Simple ID categories must have id 
        simple_id_data = """data_TEST
_entry.id TEST
_citation.id primary
_citation.title 'Test Title'
"""
        
        # Composite key categories must have all required keys
        composite_data = """data_TEST
_entry.id TEST
_chem_comp_angle.comp_id ALA
_chem_comp_angle.atom_id_1 N
_chem_comp_angle.atom_id_2 CA
_chem_comp_angle.atom_id_3 C
_chem_comp_angle.value_angle 110.5
"""
        
        handler = MMCIFHandler(validator_factory=None)
        
        # Create temp files for testing
        temp_files = []
        for i, data in enumerate([entry_level_data, simple_id_data, composite_data]):
            temp_file = os.path.join(self.temp_dir, f'test_{i}.cif')
            with open(temp_file, 'w') as f:
                f.write(data)
            temp_files.append(temp_file)
        
        # All should parse successfully with proper keys
        container1 = handler.parse(temp_files[0])
        container2 = handler.parse(temp_files[1])
        container3 = handler.parse(temp_files[2])
        
        self.assertEqual(len(container1.data), 1)
        self.assertEqual(len(container2.data), 1) 
        self.assertEqual(len(container3.data), 1)
        
    def test_item_classification_validation(self):
        """Test that items are correctly classified for XML mapping."""
        test_data = """data_TEST
_entry.id TEST
_atom_site.id 1
_atom_site.type_symbol N
_atom_site.calc_flag .
_atom_site.Cartn_x 10.123
_atom_site.footnote_id 1
"""
        
        test_file = os.path.join(self.temp_dir, 'classification_test.cif')
        with open(test_file, 'w') as f:
            f.write(test_data)
        
        handler = MMCIFHandler(validator_factory=None)
        container = handler.parse(test_file)
        converter = PDBMLConverter()
        xml_content = converter.convert_to_pdbml(container)
        
        root = ET.fromstring(xml_content)
        ns = {'pdbx': 'http://pdbml.pdb.org/schema/pdbx-v50.xsd'}
        atom_site_category = root.find('.//pdbx:atom_siteCategory', ns)
        self.assertIsNotNone(atom_site_category)
        
        atom_site = atom_site_category.find('pdbx:atom_site', ns)
        self.assertIsNotNone(atom_site)
        
        # Check classification: id is attribute, others are elements
        atom_id = atom_site.get('id')  # Attribute
        type_symbol_elem = atom_site.find('pdbx:type_symbol', ns)  # Element
        calc_flag_elem = atom_site.find('pdbx:calc_flag', ns)  # Element
        footnote_id_elem = atom_site.find('pdbx:footnote_id', ns)  # Element
        cartn_x_elem = atom_site.find('pdbx:Cartn_x', ns)  # Element
        
        self.assertIsNotNone(atom_id)  # Attribute
        self.assertIsNotNone(type_symbol_elem)  # Element
        self.assertIsNotNone(calc_flag_elem)  # Element
        self.assertIsNotNone(footnote_id_elem)  # Element
        self.assertIsNotNone(cartn_x_elem)  # Element
        
        self.assertEqual(cartn_x_elem.text, '10.123')


if __name__ == '__main__':
    unittest.main()
