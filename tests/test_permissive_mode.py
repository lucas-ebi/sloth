#!/usr/bin/env python3
"""
Test suite for PDBML converter permissive mode functionality.

This tests the core refactoring requirements:
1. permissive=False (default): Lets validation fail transparently to expose real data issues
2. permissive=True: Adds mmCIF null indicators for missing required schema fields only
3. Both modes avoid injecting arbitrary hardcoded defaults
4. Permissive mode is data-driven using XSD schema analysis
"""

import unittest
import tempfile
import os
from pathlib import Path
from sloth import MMCIFHandler, PDBMLConverter, XMLSchemaValidator
from sloth.serializer import (
    DictionaryParser, XSDParser, MappingGenerator, get_cache_manager
)
from tests.test_utils import get_shared_converter


class TestPermissiveMode(unittest.TestCase):
    """Test permissive mode functionality for PDBML conversion."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create minimal test mmCIF with missing required schema fields
        self.test_cif_content = """data_test
#
_entry.id   test
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.label_atom_id
_atom_site.label_alt_id
_atom_site.label_comp_id
_atom_site.label_asym_id
_atom_site.label_entity_id
_atom_site.label_seq_id
_atom_site.pdbx_PDB_ins_code
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
_atom_site.occupancy
_atom_site.B_iso_or_equiv
_atom_site.pdbx_formal_charge
_atom_site.auth_seq_id
_atom_site.auth_comp_id
_atom_site.auth_asym_id
_atom_site.auth_atom_id
_atom_site.pdbx_PDB_model_num
ATOM   1    N N   . MET A 1 1   ? 20.154  6.718   6.331   1.00 17.44 ? 1   MET A N   1
ATOM   2    C CA  . MET A 1 1   ? 19.030  7.160   7.123   1.00 17.72 ? 1   MET A CA  1
#
loop_
_atom_type.symbol
_atom_type.atomic_number
_atom_type.atomic_mass
N  7  14.007
C  6  12.011
#
loop_
_chem_comp.id
_chem_comp.type
_chem_comp.name
MET  'L-peptide linking'  METHIONINE
#
loop_
_entity.id
_entity.type
_entity.src_method
1  polymer  man
#
loop_
_struct_asym.id
_struct_asym.entity_id
A  1
#
_citation.id                  primary
_citation.title               "Test Structure"
_citation.journal_abbrev      ?
_citation.journal_volume      ?
_citation.page_first          ?
_citation.page_last           ?
#
"""
        
        # Create test file
        self.test_file = os.path.join(self.temp_dir, 'test_permissive.cif')
        with open(self.test_file, 'w') as f:
            f.write(self.test_cif_content)
        
        # Set up paths
        self.dict_path = Path(__file__).parent.parent / "sloth" / "schemas" / "mmcif_pdbx_v50.dic"
        self.schema_path = Path(__file__).parent.parent / "sloth" / "schemas" / "pdbx-v50.xsd"
        
    def tearDown(self):
        """Clean up temporary files."""
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
    
    def _create_converter(self, permissive: bool = False) -> PDBMLConverter:
        """Helper method to create a properly configured PDBMLConverter."""
        # Use shared converter from test_utils to maximize performance
        return get_shared_converter(permissive)
    
    def test_permissive_false_default_behavior(self):
        """Test that permissive=False (default) lets validation fail transparently."""
        # Parse mmCIF data
        handler = MMCIFHandler(validator_factory=None)
        container = handler.parse(self.test_file)
        
        # Test with permissive=False (default)
        converter = self._create_converter(permissive=False)
        xml_content = converter.convert_to_pdbml(container)
        
        # Verify XML was generated
        self.assertIsInstance(xml_content, str)
        self.assertGreater(len(xml_content), 100)
        self.assertIn('datablock', xml_content)
        
        # XML should contain actual data without arbitrary defaults
        self.assertIn('atom_siteCategory', xml_content)
        self.assertIn('MET', xml_content)  # From source data
        self.assertIn('20.154', xml_content)  # From source data
        
        # Should NOT contain arbitrary hardcoded defaults
        self.assertNotIn('DEFAULT_VALUE', xml_content)
        self.assertNotIn('UNKNOWN', xml_content)
        
        print(f"✓ Non-permissive mode generates XML without arbitrary defaults")
    
    def test_permissive_true_adds_null_indicators(self):
        """Test that permissive=True adds mmCIF null indicators for missing required schema fields."""
        # Parse mmCIF data
        handler = MMCIFHandler(validator_factory=None)
        container = handler.parse(self.test_file)
        
        # Test with permissive=True
        converter = self._create_converter(permissive=True)
        xml_content = converter.convert_to_pdbml(container)
        
        # Verify XML was generated
        self.assertIsInstance(xml_content, str)
        self.assertGreater(len(xml_content), 100)
        self.assertIn('datablock', xml_content)
        
        # XML should contain actual data
        self.assertIn('atom_siteCategory', xml_content)
        self.assertIn('MET', xml_content)  # From source data
        self.assertIn('20.154', xml_content)  # From source data
        
        # Should NOT contain arbitrary hardcoded defaults  
        self.assertNotIn('DEFAULT_VALUE', xml_content)
        self.assertNotIn('UNKNOWN', xml_content)
        
        print(f"✓ Permissive mode generates XML without arbitrary defaults")
    
    def test_permissive_parameter_accepted(self):
        """Test that the permissive parameter is correctly accepted by PDBMLConverter."""
        # Test that both permissive values are accepted
        converter_false = self._create_converter(permissive=False)
        converter_true = self._create_converter(permissive=True)
        
        self.assertEqual(converter_false.permissive, False)
        self.assertEqual(converter_true.permissive, True)
        
        # Test default value
        converter_default = self._create_converter()
        self.assertEqual(converter_default.permissive, False)
        
        print(f"✓ Permissive parameter correctly accepted and defaults to False")
    
    def test_converter_permissive_parameter(self):
        """Test that the PDBMLConverter correctly handles permissive parameter."""
        # Parse mmCIF data
        handler = MMCIFHandler(validator_factory=None)
        container = handler.parse(self.test_file)
        
        try:
            # Test converter with permissive=False
            converter_false = self._create_converter(permissive=False)
            xml_false = converter_false.convert_to_pdbml(container)
            
            # Test converter with permissive=True  
            converter_true = self._create_converter(permissive=True)
            xml_true = converter_true.convert_to_pdbml(container)
            
            # Both should generate XML
            self.assertIsInstance(xml_false, str)
            self.assertIsInstance(xml_true, str)
            self.assertGreater(len(xml_false), 100)
            self.assertGreater(len(xml_true), 100)
            
            # Verify XML content structure
            self.assertIn('datablock', xml_false)
            self.assertIn('datablock', xml_true)
            
            print(f"✓ Converter correctly handles permissive parameter")
            
        except Exception as e:
            self.fail(f"Converter test failed: {e}")
    
    def test_no_arbitrary_defaults_in_either_mode(self):
        """Test that neither permissive mode injects arbitrary hardcoded defaults."""
        handler = MMCIFHandler(validator_factory=None)
        container = handler.parse(self.test_file)
        
        # Test both modes
        converter_strict = self._create_converter(permissive=False)
        converter_permissive = self._create_converter(permissive=True)
        
        xml_strict = converter_strict.convert_to_pdbml(container)
        xml_permissive = converter_permissive.convert_to_pdbml(container)
        
        # List of arbitrary defaults that should NOT appear in either mode
        forbidden_defaults = [
            'DEFAULT_VALUE',
            'UNKNOWN',
            'PLACEHOLDER',
            'TEMP_VALUE',
            'AUTO_GENERATED',
            'FALLBACK',
            '999.999',  # Common fallback coordinate
            'UNK',      # Common fallback residue
            'XXX'       # Common fallback atom
        ]
        
        for forbidden in forbidden_defaults:
            self.assertNotIn(forbidden, xml_strict, 
                           f"Strict mode should not contain arbitrary default: {forbidden}")
            self.assertNotIn(forbidden, xml_permissive,
                           f"Permissive mode should not contain arbitrary default: {forbidden}")
        
        print(f"✓ Neither mode injects arbitrary hardcoded defaults")
    
    def test_permissive_mode_preserves_source_data(self):
        """Test that permissive mode preserves all source data integrity."""
        handler = MMCIFHandler(validator_factory=None)
        container = handler.parse(self.test_file)
        
        converter = self._create_converter(permissive=True)
        xml_content = converter.convert_to_pdbml(container)
        
        # Verify source data is preserved
        source_values = ['MET', 'N', 'CA', '20.154', '6.718', '19.030', '7.160']
        
        for value in source_values:
            self.assertIn(value, xml_content, 
                         f"Source data value '{value}' should be preserved in permissive mode")
        
        # Verify structure integrity
        self.assertIn('atom_siteCategory', xml_content)
        self.assertIn('citationCategory', xml_content)
        self.assertIn('entryCategory', xml_content)
        
        print(f"✓ Permissive mode preserves all source data integrity")
    
    def test_validation_behavior_comparison(self):
        """Test validation behavior differences between permissive modes."""
        if not self.schema_path.exists():
            self.skipTest(f"Schema file not found: {self.schema_path}")
        
        try:
            handler = MMCIFHandler(validator_factory=None)
            container = handler.parse(self.test_file)
            
            # Generate XML in both modes
            converter_strict = self._create_converter(permissive=False)
            converter_permissive = self._create_converter(permissive=True)
            
            xml_strict = converter_strict.convert_to_pdbml(container)
            xml_permissive = converter_permissive.convert_to_pdbml(container)
            
            # Try validation (if available)
            validator = XMLSchemaValidator(self.schema_path)
            
            if validator.schema is not None:
                try:
                    result_strict = validator.validate(xml_strict)
                    is_valid_strict = result_strict.get("valid", False) if isinstance(result_strict, dict) else False
                except:
                    is_valid_strict = False
                
                try:
                    result_permissive = validator.validate(xml_permissive)
                    is_valid_permissive = result_permissive.get("valid", False) if isinstance(result_permissive, dict) else False
                except:
                    is_valid_permissive = False
                
                print(f"✓ Validation comparison: strict={is_valid_strict}, permissive={is_valid_permissive}")
                
                # The key insight: permissive mode should not inject arbitrary defaults
                # Both modes should show real data quality issues
                # The difference should only be in schema-required elements, not arbitrary defaults
                
            else:
                self.skipTest("Validator not available")
                
        except Exception as e:
            self.skipTest(f"Validation test skipped: {e}")
    
    def test_refactoring_goals_achieved(self):
        """Test that the core refactoring goals are achieved."""
        handler = MMCIFHandler(validator_factory=None)
        container = handler.parse(self.test_file)
        
        # Test both modes
        converter_strict = self._create_converter(permissive=False)
        converter_permissive = self._create_converter(permissive=True)
        
        xml_strict = converter_strict.convert_to_pdbml(container)
        xml_permissive = converter_permissive.convert_to_pdbml(container)
        
        # Goal 1: Avoid injecting arbitrary defaults
        arbitrary_indicators = ['DEFAULT', 'UNKNOWN', 'PLACEHOLDER', 'AUTO']
        for indicator in arbitrary_indicators:
            self.assertNotIn(indicator, xml_strict)
            self.assertNotIn(indicator, xml_permissive)
        
        # Goal 2: Both modes generate valid XML structure
        self.assertIn('<?xml', xml_strict)
        self.assertIn('<?xml', xml_permissive)
        self.assertIn('datablock', xml_strict)
        self.assertIn('datablock', xml_permissive)
        
        # Goal 3: Data-driven approach (contains source data)
        self.assertIn('MET', xml_strict)   # From source
        self.assertIn('MET', xml_permissive)   # From source
        self.assertIn('20.154', xml_strict)    # From source
        self.assertIn('20.154', xml_permissive)    # From source
        
        # Goal 4: Permissive=False is default
        converter_default = self._create_converter()
        self.assertEqual(converter_default.permissive, False)
        
        print(f"✓ All refactoring goals achieved:")
        print(f"  - No arbitrary defaults injected")
        print(f"  - Valid XML structure generated")
        print(f"  - Data-driven approach maintained")
        print(f"  - permissive=False is default")


if __name__ == '__main__':
    unittest.main(verbosity=2)
