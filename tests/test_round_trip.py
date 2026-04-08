#!/usr/bin/env python3
"""
Comprehensive round-trip tests for SLOTH.

Tests JSON export/import with both flat and nested structures,
ensuring data integrity is preserved across conversions.
"""

import json
import unittest
import tempfile
import shutil
import os
from sloth.mmcif import MMCIFHandler
from sloth.mmcif.importer import JSONImporter
# JSON export is always nested


class TestRoundTripSimple(unittest.TestCase):
    """Test round-trip with simple data (no nesting)."""

    def setUp(self):
        """Set up test fixtures with simple data."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Simple data with no parent-child relationships
        self.test_cif_path = os.path.join(self.temp_dir, "simple.cif")
        with open(self.test_cif_path, "w") as f:
            f.write(
                """data_simple
_entry.id test_structure
_database_2.database_id PDB
_database_2.database_code ABC123
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
            )
        
        self.handler = MMCIFHandler()
        self.mmcif = self.handler.read(self.test_cif_path)

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_round_trip_preserves_categories(self):
        """Verify all categories are preserved in round-trip."""
        # Export to JSON
        json_path = os.path.join(self.temp_dir, "test.json")
        self.handler.export(self.mmcif, file_path=json_path)
        
        # Import back
        imported = self.handler.load(json_path)
        
        # Compare categories
        original_cats = set(self.mmcif.data[0].categories)
        imported_cats = set(imported.data[0].categories)
        
        self.assertEqual(original_cats, imported_cats, 
                        f"Category mismatch: Original={original_cats}, Imported={imported_cats}")

    def test_round_trip_preserves_values(self):
        """Verify data values are preserved in round-trip."""
        # Export and import
        json_path = os.path.join(self.temp_dir, "test.json")
        self.handler.export(self.mmcif, file_path=json_path)
        imported = self.handler.load(json_path)
        
        # Check specific values
        original_entry = self.mmcif.data[0]["_entry"]["id"][0]
        imported_entry = imported.data[0]["_entry"]["id"][0]
        self.assertEqual(original_entry, imported_entry)
        
        # Check atom_site data
        original_coords = self.mmcif.data[0]["_atom_site"]["Cartn_x"]
        imported_coords = imported.data[0]["_atom_site"]["Cartn_x"]
        self.assertEqual(original_coords, imported_coords)


class TestRoundTripNested(unittest.TestCase):
    """Test round-trip with nested data structures."""

    def setUp(self):
        """Set up test fixtures with data that has parent-child relationships."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Data with entity -> entity_poly -> entity_poly_seq hierarchy
        self.test_cif_path = os.path.join(self.temp_dir, "nested.cif")
        with open(self.test_cif_path, "w") as f:
            f.write(
                """data_nested_test
#
_entry.id nested_test
#
# Entity information (parent)
#
loop_
_entity.id
_entity.type
_entity.pdbx_description
1 polymer 'Protein A'
2 polymer 'Protein B'
#
# Entity poly (child of entity)
#
loop_
_entity_poly.entity_id
_entity_poly.type
_entity_poly.pdbx_seq_one_letter_code
1 'polypeptide(L)' ACDEFGH
2 'polypeptide(L)' MNPQRST
#
# Entity poly seq (child of entity_poly)
#
loop_
_entity_poly_seq.entity_id
_entity_poly_seq.num
_entity_poly_seq.mon_id
1 1 ALA
1 2 CYS
1 3 ASP
2 1 MET
2 2 ASN
#
# Struct asym (child of entity)
#
loop_
_struct_asym.id
_struct_asym.entity_id
_struct_asym.details
A 1 'Chain A'
B 2 'Chain B'
#
# Atom site (child of struct_asym)
#
loop_
_atom_site.label_asym_id
_atom_site.label_entity_id
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
A 1 1 N 10.123
A 1 2 C 11.234
B 2 3 N 12.345
B 2 4 C 13.456
"""
            )
        
        self.handler = MMCIFHandler()
        self.mmcif = self.handler.read(self.test_cif_path)

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_nested_export_reduces_categories(self):
        """Verify nested export creates hierarchical structure."""
        # Export to nested JSON
        json_path = os.path.join(self.temp_dir, "nested.json")
        self.handler.export(
            self.mmcif, 
            file_path=json_path
        )
        
        # Read the JSON to check structure
        with open(json_path) as f:
            data = json.load(f)
            block = list(data.values())[0]
            top_level_cats = list(block.keys())
        
        # Should have fewer top-level categories due to nesting
        original_cat_count = len(self.mmcif.data[0].categories)
        nested_cat_count = len(top_level_cats)
        
        self.assertLess(nested_cat_count, original_cat_count,
                       f"Nested structure should have fewer top-level categories. "
                       f"Original={original_cat_count}, Nested={nested_cat_count}")

    def test_round_trip_preserves_all_categories(self):
        """Verify nested export + import preserves ALL categories."""
        # Export to nested JSON
        json_path = os.path.join(self.temp_dir, "nested.json")
        self.handler.export(
            self.mmcif,
            file_path=json_path
        )
        
        # Import back
        imported = self.handler.load(json_path)
        
        # Compare categories - should be identical after flattening
        original_cats = sorted(self.mmcif.data[0].categories)
        imported_cats = sorted(imported.data[0].categories)
        
        self.assertEqual(original_cats, imported_cats,
                        f"Category mismatch after round-trip:\n"
                        f"Original ({len(original_cats)}): {original_cats}\n"
                        f"Imported ({len(imported_cats)}): {imported_cats}\n"
                        f"Missing: {set(original_cats) - set(imported_cats)}\n"
                        f"Extra: {set(imported_cats) - set(original_cats)}")

    def test_round_trip_preserves_entity_data(self):
        """Verify entity data is preserved through nesting/flattening."""
        # Export and import
        json_path = os.path.join(self.temp_dir, "nested.json")
        self.handler.export(self.mmcif, file_path=json_path)
        imported = self.handler.load(json_path)
        
        # Check entity data
        original_entity = self.mmcif.data[0]["_entity"]
        imported_entity = imported.data[0]["_entity"]
        
        self.assertEqual(original_entity["id"], imported_entity["id"])
        self.assertEqual(original_entity["type"], imported_entity["type"])
        self.assertEqual(original_entity["pdbx_description"], imported_entity["pdbx_description"])

    def test_round_trip_preserves_entity_poly_data(self):
        """Verify entity_poly data is preserved (nested child category)."""
        # Export and import
        json_path = os.path.join(self.temp_dir, "nested.json")
        self.handler.export(self.mmcif, file_path=json_path)
        imported = self.handler.load(json_path)
        
        # Check entity_poly data
        original_poly = self.mmcif.data[0]["_entity_poly"]
        imported_poly = imported.data[0]["_entity_poly"]
        
        self.assertEqual(original_poly["entity_id"], imported_poly["entity_id"])
        self.assertEqual(original_poly["type"], imported_poly["type"])
        self.assertEqual(original_poly["pdbx_seq_one_letter_code"], 
                        imported_poly["pdbx_seq_one_letter_code"])

    def test_round_trip_preserves_entity_poly_seq_data(self):
        """Verify entity_poly_seq data is preserved (deeply nested child)."""
        # Export and import
        json_path = os.path.join(self.temp_dir, "nested.json")
        self.handler.export(self.mmcif, file_path=json_path)
        imported = self.handler.load(json_path)
        
        # Check entity_poly_seq data
        original_seq = self.mmcif.data[0]["_entity_poly_seq"]
        imported_seq = imported.data[0]["_entity_poly_seq"]
        
        self.assertEqual(len(original_seq["entity_id"]), len(imported_seq["entity_id"]))
        self.assertEqual(original_seq["entity_id"], imported_seq["entity_id"])
        self.assertEqual(original_seq["num"], imported_seq["num"])
        self.assertEqual(original_seq["mon_id"], imported_seq["mon_id"])

    def test_round_trip_preserves_atom_site_data(self):
        """Verify atom_site data is preserved (nested under struct_asym under entity)."""
        # Export and import
        json_path = os.path.join(self.temp_dir, "nested.json")
        self.handler.export(self.mmcif, file_path=json_path)
        imported = self.handler.load(json_path)
        
        # Check atom_site data
        original_atoms = self.mmcif.data[0]["_atom_site"]
        imported_atoms = imported.data[0]["_atom_site"]
        
        self.assertEqual(len(original_atoms["id"]), len(imported_atoms["id"]))
        self.assertEqual(original_atoms["label_asym_id"], imported_atoms["label_asym_id"])
        self.assertEqual(original_atoms["label_entity_id"], imported_atoms["label_entity_id"])
        self.assertEqual(original_atoms["type_symbol"], imported_atoms["type_symbol"])
        self.assertEqual(original_atoms["Cartn_x"], imported_atoms["Cartn_x"])


class TestRoundTripComplex(unittest.TestCase):
    """Test round-trip with complex multi-level nesting."""

    def setUp(self):
        """Set up test fixtures with complex nested data."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Complex data with 3-level nesting: entity -> struct_asym -> atom_site
        # and entity -> entity_poly -> entity_poly_seq
        self.test_cif_path = os.path.join(self.temp_dir, "complex.cif")
        with open(self.test_cif_path, "w") as f:
            f.write(
                """data_complex
_entry.id complex_test
#
loop_
_entity.id
_entity.type
1 polymer
2 non-polymer
#
loop_
_entity_poly.entity_id
_entity_poly.type
1 'polypeptide(L)'
#
loop_
_entity_poly_seq.entity_id
_entity_poly_seq.num
_entity_poly_seq.mon_id
1 1 ALA
1 2 CYS
1 3 ASP
#
loop_
_struct_asym.id
_struct_asym.entity_id
A 1
B 1
C 2
#
loop_
_atom_site.label_asym_id
_atom_site.label_entity_id
_atom_site.id
_atom_site.type_symbol
A 1 1 N
A 1 2 C
B 1 3 N
C 2 4 O
"""
            )
        
        self.handler = MMCIFHandler()
        self.mmcif = self.handler.read(self.test_cif_path)
        self.original_category_count = len(self.mmcif.data[0].categories)

    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_multiple_nesting_levels_preserved(self):
        """Verify 3-level nesting is correctly flattened on import."""
        # Export to nested JSON
        json_path = os.path.join(self.temp_dir, "complex.json")
        self.handler.export(self.mmcif, file_path=json_path)
        
        # Import back
        imported = self.handler.load(json_path)
        
        # Should have same number of categories after round-trip
        imported_category_count = len(imported.data[0].categories)
        
        self.assertEqual(self.original_category_count, imported_category_count,
                        f"Category count mismatch: Original={self.original_category_count}, "
                        f"Imported={imported_category_count}")
        
        # Verify all categories present
        original_cats = set(self.mmcif.data[0].categories)
        imported_cats = set(imported.data[0].categories)
        
        self.assertEqual(original_cats, imported_cats)

    def test_data_integrity_across_nesting_levels(self):
        """Verify data values are preserved across all nesting levels."""
        # Export and import
        json_path = os.path.join(self.temp_dir, "complex.json")
        self.handler.export(self.mmcif, file_path=json_path)
        imported = self.handler.load(json_path)
        
        # Check each level
        orig_block = self.mmcif.data[0]
        imp_block = imported.data[0]
        
        # Level 1: entity
        self.assertEqual(orig_block["_entity"]["id"], imp_block["_entity"]["id"])
        
        # Level 2: entity_poly (child of entity)
        self.assertEqual(orig_block["_entity_poly"]["entity_id"], 
                        imp_block["_entity_poly"]["entity_id"])
        
        # Level 3: entity_poly_seq (child of entity_poly)
        self.assertEqual(orig_block["_entity_poly_seq"]["mon_id"],
                        imp_block["_entity_poly_seq"]["mon_id"])
        
        # Level 2: struct_asym (child of entity)
        self.assertEqual(orig_block["_struct_asym"]["id"],
                        imp_block["_struct_asym"]["id"])
        
        # Level 3: atom_site (child of struct_asym)
        self.assertEqual(orig_block["_atom_site"]["type_symbol"],
                        imp_block["_atom_site"]["type_symbol"])


if __name__ == "__main__":
    unittest.main()
