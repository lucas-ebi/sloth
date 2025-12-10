#!/usr/bin/env python3
"""
SLOTH Demo - Lazy by design. Fast by default.

Demonstrates parsing, validation, modification, and writing of mmCIF files
using SLOTH's ultra-simple API with high-performance gemmi backend.
"""

import argparse
import os
import json
import copy
import shutil
from sloth.mmcif import (
    MMCIFHandler,
    MMCIFWriter,
    ValidatorFactory,
    DataSourceFormat,
    StructureFormat,
)
from typing import Dict, Optional, Any, Union
from pathlib import Path

# Comprehensive embedded demo mmCIF data - realistic protein complex structure
COMPREHENSIVE_DEMO_MMCIF = """data_DEMO
#
# Entry-level metadata
#
_entry.id                      DEMO
_entry.type                    'experimental model'
#
# Database cross-references
#
loop_
_database_2.database_id
_database_2.database_code
_database_2.database_chain
PDB DEMO ?
RCSB DEMO ?
WWPDB DEMO ?
#
# Publication information with complete author lists
#
loop_
_citation.id
_citation.title
_citation.journal_abbrev
_citation.journal_volume
_citation.page_first
_citation.page_last
_citation.year
_citation.journal_id_ISSN
_citation.country
_citation.journal_id_ASTM
_citation.journal_id_CSD
primary 'High-resolution crystal structure of a model protein complex' 'Nat. Struct. Mol. Biol.' 28 245 252 2021 1545-9985 US NSMHCP ?
2 'Structural insights into protein folding mechanisms' 'Science' 374 1234 1240 2021 0036-8075 US SCIEAS ?
3 'Computational methods for protein structure determination' 'J. Mol. Biol.' 433 166891 166891 2021 0022-2836 UK JMOBAK ?
#
loop_
_citation_author.citation_id
_citation_author.name
_citation_author.ordinal
primary 'Smith, J.A.' 1
primary 'Johnson, K.L.' 2
primary 'Williams, R.M.' 3
primary 'Brown, S.E.' 4
primary 'Davis, M.T.' 5
2 'Wilson, P.D.' 1
2 'Miller, L.R.' 2
2 'Garcia, A.M.' 3
3 'Anderson, T.B.' 1
3 'Thompson, C.J.' 2
3 'White, N.K.' 3
3 'Lewis, H.P.' 4
#
# Experimental details
#
loop_
_exptl.entry_id
_exptl.method
_exptl.crystals_number
_exptl.details
DEMO 'X-RAY DIFFRACTION' 1 'Data collected at 100K using synchrotron radiation'
#
# Crystal information
#
_exptl_crystal.id                     1
_exptl_crystal.density_diffrn         1.385
_exptl_crystal.density_method         'calculated from crystal cell and contents'
_exptl_crystal.description            'Prismatic colorless crystal'
_exptl_crystal.F_000                  1680
_exptl_crystal.preparation            'Vapor diffusion hanging drop'
#
# Space group and cell parameters
#
_space_group.id                       1
_space_group.crystal_system           orthorhombic
_space_group.IT_number                19
_space_group.name_H-M_alt             'P 21 21 21'
_space_group.name_Hall                'P 2ac 2ab'
#
_cell.entry_id                        DEMO
_cell.length_a                        52.123
_cell.length_b                        87.456
_cell.length_c                        134.789
_cell.angle_alpha                     90.00
_cell.angle_beta                      90.00
_cell.angle_gamma                     90.00
_cell.Z_PDB                           4
#
# Structure description
#
loop_
_struct.entry_id
_struct.title
_struct.pdbx_descriptor
_struct.pdbx_model_details
DEMO 'Crystal structure of a model protein-ligand complex at 1.8 Angstrom resolution' 'Model protein complex' 'High resolution X-ray structure'
#
# Keywords and classification
#
loop_
_struct_keywords.entry_id
_struct_keywords.pdbx_keywords
_struct_keywords.text
DEMO 'TRANSFERASE/DNA' 'Protein-DNA complex, transferase activity, enzyme mechanism'
#
# Molecular entities (proteins, nucleic acids, ligands, water)
#
loop_
_entity.id
_entity.type
_entity.src_method
_entity.pdbx_description
_entity.formula_weight
_entity.pdbx_number_of_molecules
_entity.details
_entity.pdbx_mutation
_entity.pdbx_fragment
1 polymer man 'Catalytic domain of model transferase' 24567.3 1 'Recombinant protein expression in E. coli' ? 'residues 45-234'
2 polymer man 'DNA-binding domain' 18934.7 1 'Recombinant protein co-expressed with domain 1' ? 'residues 1-167'
3 non-polymer syn 'ATP analog inhibitor' 507.2 1 'Competitive inhibitor' ? ?
4 non-polymer nat 'Magnesium ion' 24.3 2 'Cofactor required for activity' ? ?
5 non-polymer nat 'Water' 18.0 347 'Solvent molecules' ? ?
#
# Polymer sequence information
#
loop_
_entity_poly.entity_id
_entity_poly.type
_entity_poly.nstd_linkage
_entity_poly.nstd_monomer
_entity_poly.pdbx_seq_one_letter_code
_entity_poly.pdbx_seq_one_letter_code_can
1 'polypeptide(L)' no no 'MKHLVVGAYGVGKSSLLRTLNAKDNVKSVYVPTSGQMKVEKELGISAAVTTIKEDLKQMQDDVSQKHNLLQHQGSDQTADKVPVSVIYGSDPYDMAAEFLNHKKDHSN' 'MKHLVVGAYGVGKSSLLRTLNAKDNVKSVYVPTSGQMKVEKELGISAAVTTIKEDLKQMQDDVSQKHNLLQHQGSDQTADKVPVSVIYGSDPYDMAAEFLNHKKDHSN'
2 'polypeptide(L)' no no 'MADEIKLNVQNSKRSLETIKELLKLMGDVEYIFVPTSGQFSLDNFQRRGQTADKVPVSVIYGSDPYDMAQALANHKKDHSD' 'MADEIKLNVQNSKRSLETIKELLKLMGDVEYIFVPTSGQFSLDNFQRRGQTADKVFVSIIGNSPYDMAQALANHKKDHSD'
#
# Detailed polymer sequence information
#
loop_
_entity_poly_seq.entity_id
_entity_poly_seq.num
_entity_poly_seq.mon_id
_entity_poly_seq.hetero
1 1 MET n
1 2 LYS n
1 3 HIS n
1 4 LEU n
1 5 VAL n
1 6 VAL n
1 7 GLY n
1 8 ALA n
1 9 TYR n
1 10 GLY n
2 1 MET n
2 2 ALA n
2 3 ASP n
2 4 GLU n
2 5 ILE n
2 6 LYS n
2 7 LEU n
2 8 ASN n
2 9 VAL n
2 10 GLN n
#
# Asymmetric unit assignments
#
loop_
_struct_asym.id
_struct_asym.entity_id
_struct_asym.details
_struct_asym.pdbx_blank_PDB_chainid_flag
_struct_asym.pdbx_modified
_struct_asym.pdbx_order
A 1 'Chain A - Catalytic domain' N N 1
B 2 'Chain B - DNA-binding domain' N N 2
C 3 'ATP analog ligand' N N 3
D 4 'Magnesium cofactor site 1' N N 4
E 4 'Magnesium cofactor site 2' N N 5
F 5 'Solvent water molecules' N N 6
#
# Detailed atomic coordinates (representative atoms from different residues/ligands)
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.label_atom_id
_atom_site.label_comp_id
_atom_site.label_asym_id
_atom_site.label_entity_id
_atom_site.label_seq_id
_atom_site.auth_asym_id
_atom_site.auth_atom_id
_atom_site.auth_comp_id
_atom_site.auth_seq_id
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
_atom_site.occupancy
_atom_site.B_iso_or_equiv
_atom_site.pdbx_PDB_model_num
_atom_site.label_alt_id
_atom_site.pdbx_PDB_ins_code
_atom_site.pdbx_formal_charge
_atom_site.U_iso_or_equiv
# Chain A - Catalytic domain (first few residues)
ATOM 1 N N MET A 1 1 A N MET 1 20.154 6.718 46.973 1.00 25.00 1 ? ? 0 0.0316
ATOM 2 C CA MET A 1 1 A CA MET 1 21.618 6.765 47.254 1.00 24.50 1 ? ? 0 0.0309
ATOM 3 C C MET A 1 1 A C MET 1 22.147 8.178 47.451 1.00 23.85 1 ? ? 0 0.0301
ATOM 4 O O MET A 1 1 A O MET 1 21.393 9.133 47.651 1.00 24.52 1 ? ? 0 0.0310
ATOM 5 C CB MET A 1 1 A CB MET 1 22.456 5.892 46.312 1.00 26.13 1 ? ? 0 0.0330
ATOM 6 N N LYS A 1 2 A N LYS 2 23.421 8.371 47.389 1.00 22.76 1 ? ? 0 0.0287
ATOM 7 C CA LYS A 1 2 A CA LYS 2 24.067 9.652 47.612 1.00 21.89 1 ? ? 0 0.0276
ATOM 8 C C LYS A 1 2 A C LYS 2 23.924 10.124 49.045 1.00 20.45 1 ? ? 0 0.0258
# Chain B - DNA-binding domain (first few residues)
ATOM 1001 N N MET B 2 1 B N MET 1 15.234 12.456 52.123 1.00 23.45 1 ? ? 0 0.0296
ATOM 1002 C CA MET B 2 1 B CA MET 1 16.543 13.089 51.892 1.00 22.67 1 ? ? 0 0.0286
ATOM 1003 C C MET B 2 1 B C MET 1 17.234 12.567 50.645 1.00 21.23 1 ? ? 0 0.0268
ATOM 1004 O O MET B 2 1 B O MET 1 16.789 11.723 49.987 1.00 22.11 1 ? ? 0 0.0279
# ATP analog ligand (Chain C)
HETATM 2001 P P1 ATP C 3 . C P1 ATP 1 12.345 15.678 35.432 1.00 18.56 1 ? ? 0 0.0234
HETATM 2002 O O1P ATP C 3 . C O1P ATP 1 11.234 16.789 36.123 1.00 19.23 1 ? ? 0 0.0243
HETATM 2003 N N9 ATP C 3 . C N9 ATP 1 14.567 13.234 37.891 1.00 17.89 1 ? ? 0 0.0226
HETATM 2004 C C8 ATP C 3 . C C8 ATP 1 15.234 12.456 38.789 1.00 18.34 1 ? ? 0 0.0231
# Magnesium ions
HETATM 3001 MG MG MG D 4 . D MG MG 1 18.234 20.567 42.345 1.00 15.67 1 ? ? 2 0.0198
HETATM 3002 MG MG MG E 4 . E MG MG 2 25.678 18.234 38.912 1.00 16.23 1 ? ? 2 0.0205
# Water molecules (representative)
HETATM 4001 O O HOH F 5 . F O HOH 1 30.123 25.456 45.789 1.00 35.67 1 ? ? 0 0.0450
HETATM 4002 O O HOH F 5 . F O HOH 2 8.456 19.234 51.678 1.00 42.34 1 ? ? 0 0.0534
HETATM 4003 O O HOH F 5 . F O HOH 3 19.789 8.567 39.234 1.00 38.91 1 ? ? 0 0.0491
#
# Atom type information for scattering factors
#
loop_
_atom_type.symbol
_atom_type.number_in_cell
_atom_type.scat_dispersion_real
_atom_type.scat_dispersion_imag
_atom_type.scat_length_neutron
_atom_type.scat_source
C 1 0.0033 0.0016 6.646 'International Tables Vol C Tables 4.2.6.8 and 6.1.1.4'
N 1 0.0061 0.0033 9.36 'International Tables Vol C Tables 4.2.6.8 and 6.1.1.4'
O 1 0.0106 0.0060 5.803 'International Tables Vol C Tables 4.2.6.8 and 6.1.1.4'
P 1 0.1023 0.0942 5.13 'International Tables Vol C Tables 4.2.6.8 and 6.1.1.4'
MG 1 0.0486 0.0363 5.375 'International Tables Vol C Tables 4.2.6.8 and 6.1.1.4'
#
# Chemical component definitions
#
loop_
_chem_comp.id
_chem_comp.type
_chem_comp.mon_nstd_flag
_chem_comp.name
_chem_comp.formula
_chem_comp.formula_weight
_chem_comp.pdbx_synonyms
_chem_comp.pdbx_formal_charge
MET 'L-peptide linking' y METHIONINE 'C5 H11 N O2 S' 149.211 'L-Met, Met' 0
LYS 'L-peptide linking' y LYSINE 'C6 H14 N2 O2' 146.188 'L-Lys, Lys' 0
HIS 'L-peptide linking' y HISTIDINE 'C6 H9 N3 O2' 155.154 'L-His, His' 0
LEU 'L-peptide linking' y LEUCINE 'C6 H13 N O2' 131.173 'L-Leu, Leu' 0
VAL 'L-peptide linking' y VALINE 'C5 H11 N O2' 117.146 'L-Val, Val' 0
GLY 'L-peptide linking' y GLYCINE 'C2 H5 N O2' 75.067 'L-Gly, Gly' 0
ALA 'L-peptide linking' y ALANINE 'C3 H7 N O2' 89.094 'L-Ala, Ala' 0
TYR 'L-peptide linking' y TYROSINE 'C9 H11 N O3' 181.189 'L-Tyr, Tyr' 0
ASP 'L-peptide linking' y 'ASPARTIC ACID' 'C4 H7 N O4' 133.104 'L-Asp, Asp' 0
GLU 'L-peptide linking' y 'GLUTAMIC ACID' 'C5 H9 N O4' 147.130 'L-Glu, Glu' 0
ILE 'L-peptide linking' y ISOLEUCINE 'C6 H13 N O2' 131.173 'L-Ile, Ile' 0
ASN 'L-peptide linking' y ASPARAGINE 'C4 H8 N2 O3' 132.119 'L-Asn, Asn' 0
GLN 'L-peptide linking' y GLUTAMINE 'C5 H10 N2 O3' 146.145 'L-Gln, Gln' 0
ATP 'non-polymer' n 'ADENOSINE-5'-TRIPHOSPHATE ANALOG' 'C10 H16 N5 O13 P3' 507.181 'ATP, adenosine triphosphate' -4
MG 'non-polymer' n 'MAGNESIUM ION' 'Mg' 24.305 'Mg(2+), magnesium(II)' 2
HOH 'non-polymer' n WATER 'H2 O' 18.015 'water, H2O' 0
#
# Secondary structure assignments
#
# Define secondary structure types first
loop_
_struct_conf_type.id
_struct_conf_type.criteria
_struct_conf_type.reference
HELX_P 'Right-handed alpha helix' 'Ramachandran angles and hydrogen bonding pattern'
STRN 'Extended beta strand' 'Backbone hydrogen bonding in beta sheet'
#
loop_
_struct_conf.conf_type_id
_struct_conf.id
_struct_conf.pdbx_PDB_helix_id
_struct_conf.beg_label_comp_id
_struct_conf.beg_label_asym_id
_struct_conf.beg_label_seq_id
_struct_conf.end_label_comp_id
_struct_conf.end_label_asym_id
_struct_conf.end_label_seq_id
_struct_conf.pdbx_PDB_helix_class
_struct_conf.details
HELX_P H1 1 MET A 5 LYS A 18 'Right-handed alpha' 'Active site helix'
HELX_P H2 2 VAL B 12 LEU B 28 'Right-handed alpha' 'DNA-binding helix'
#
loop_
_struct_sheet.id
_struct_sheet.type
_struct_sheet.number_strands
_struct_sheet.details
S1 beta 4 'Central beta sheet in catalytic domain'
S2 beta 3 'Anti-parallel beta sheet in binding domain'
#
# Binding sites and functional annotations
#
loop_
_struct_site.id
_struct_site.pdbx_evidence_code
_struct_site.pdbx_auth_insert_code
_struct_site.pdbx_auth_comp_id
_struct_site.pdbx_auth_asym_id
_struct_site.pdbx_auth_seq_id
_struct_site.details
ATP_SITE 'Software' ? ATP C . 'ATP binding site - competitive inhibitor'
MG_SITE1 'Software' ? MG D . 'Metal coordination site 1'
MG_SITE2 'Software' ? MG E . 'Metal coordination site 2'
HYDRO_1 'Software' ? VAL A 15 'Hydrophobic binding pocket'
HYDRO_2 'Software' ? LEU A 23 'Hydrophobic binding pocket'
#
"""

# Simple demo mmCIF data for basic demonstrations
SIMPLE_DEMO_MMCIF = """data_1ABC
#
_entry.id 1ABC_STRUCTURE
#
_database_2.database_id      PDB
_database_2.database_code    1ABC
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
ATOM   1    N  10.123 20.456 30.789
ATOM   2    C  11.234 21.567 31.890
ATOM   3    C  12.345 22.678 32.901
#
"""





def category_validator(category_name):
    """Example validator function."""
    print(f"✅ Validating category: {category_name}")


def cross_checker(category_name_1, category_name_2):
    """Example cross-checker function."""
    print(f"🔗 Cross-checking: {category_name_1} ↔ {category_name_2}")


def modify_data(mmcif):
    """Example data modification."""
    if not mmcif.data:
        print("❌ No data blocks found")
        return

    block = mmcif.data[0]
    print(f"📋 Working with block: {block.name}")

    # Try to modify database information
    if "_database_2" in block.categories:
        # Direct dot notation access - the most elegant way!
        db_category = block._database_2  # This is dot notation in action!
        if hasattr(db_category, "database_id") and db_category.database_id:
            original = db_category.database_id[-1]
            db_category.database_id[
                -1
            ] = "RCSB"  # Simple assignment with dot notation - using valid schema value
            print(f"✏️  Modified database_id: '{original}' → 'RCSB'")
            print(
                f"   Using elegant dot notation: block._database_2.database_id[-1] = 'RCSB'"
            )
        else:
            print("ℹ️  No database_id found to modify")
    else:
        print("ℹ️  No _database_2 category found")


def show_file_info(mmcif):
    """Display information about the parsed file."""
    print(f"\n📊 File Information:")
    print(f"   Data blocks: {len(mmcif.data)}")

    for i, block in enumerate(mmcif.data):
        print(f"   Block {i+1}: '{block.name}' with {len(block.categories)} categories")

        # Show first few categories
        category_names = block.categories[:5]
        if category_names:
            print(f"   Categories: {', '.join(category_names)}")
            if len(block.categories) > 5:
                print(f"   ... and {len(block.categories) - 5} more")





def parse_embedded_demo_data():
    """Parse the embedded comprehensive demo data."""
    import tempfile
    import os
    
    print(f"📝 Using embedded comprehensive demo data")
    print(f"   Data source: Comprehensive protein-ligand complex structure")
    print(f"   Features: Multiple entities, citations, authors, coordinates, ligands")
    
    # Create a temporary file with the embedded data
    with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as tmp_file:
        tmp_file.write(COMPREHENSIVE_DEMO_MMCIF)
        tmp_filename = tmp_file.name
    
    try:
        # Parse the temporary file
        handler = MMCIFHandler()
        mmcif = handler.read(tmp_filename)
        return mmcif
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_filename):
            os.remove(tmp_filename)


def demonstrate_2d_slicing(mmcif):
    """Demonstrate 2D slicing functionality with emphasis on dot notation."""
    if not mmcif.data:
        print("❌ No data blocks found")
        return

    block = mmcif.data[0]
    print(f"\n🔢 Demonstrating 2D slicing with dot notation:")
    print(
        f"   The power of SLOTH's dot notation makes data access elegant and intuitive!"
    )

    # Find an appropriate category with multiple rows for demonstration
    demo_categories = ["_atom_site", "_entity_poly_seq", "_struct_conn"]
    demo_category = None

    for cat_name in demo_categories:
        if cat_name in block.categories:
            # Use dot notation to access category - this is the elegant way!
            if cat_name == "_atom_site":
                demo_category = block._atom_site  # Direct dot notation!
            elif cat_name == "_entity_poly_seq":
                demo_category = block._entity_poly_seq  # Direct dot notation!
            elif cat_name == "_struct_conn":
                demo_category = block._struct_conn  # Direct dot notation!

            if demo_category and demo_category.row_count >= 3:
                print(
                    f"   Using category: {cat_name} with {demo_category.row_count} rows"
                )
                print(f"   Accessed using elegant dot notation: block.{cat_name}")
                break

    if not demo_category:
        print("   No suitable category found with multiple rows for demonstration")
        return

    print("\n📊 Column-wise access with dot notation (the Pythonic way):")
    # Show all item names in the category
    print(f"   Available items: {', '.join(demo_category.items)}")

    # Get the first two item names for demonstration
    item_names = demo_category.items[:2]

    # Show dot notation access for typical items
    if "group_PDB" in demo_category.items:
        values = demo_category.group_PDB  # Direct dot notation!
        print(
            f"   demo_category.group_PDB: {values[:3]} {'...' if len(values) > 3 else ''}"
        )
    if "id" in demo_category.items:
        values = demo_category.id  # Direct dot notation!
        print(f"   demo_category.id: {values[:3]} {'...' if len(values) > 3 else ''}")
    if "type_symbol" in demo_category.items:
        values = demo_category.type_symbol  # Direct dot notation!
        print(
            f"   demo_category.type_symbol: {values[:3]} {'...' if len(values) > 3 else ''}"
        )

    # For comparison, show dictionary style access (less elegant)
    print("\n   Alternative dictionary access (less intuitive):")
    for item_name in item_names:
        values = demo_category[item_name]
        print(f"   demo_category['{item_name}']: {values[:3]}")

    print("\n📋 Row-wise access with dot notation (elegant and readable):")
    # Show first row with dot notation access
    first_row = demo_category[0]
    print(f"   Row 0 (clean dot notation access):")

    # Use direct dot notation for common attributes
    if "group_PDB" in demo_category.items:
        print(f"     row.group_PDB: {first_row.group_PDB}")  # Direct dot notation!
    if "id" in demo_category.items:
        print(f"     row.id: {first_row.id}")  # Direct dot notation!
    if "type_symbol" in demo_category.items:
        print(f"     row.type_symbol: {first_row.type_symbol}")  # Direct dot notation!

    # Show row.data
    print(f"\n   Complete row data as dictionary (row.data):")
    row_data = first_row.data
    for item_name, value in list(row_data.items())[:3]:
        print(f"     {item_name}: {value}")
    if len(row_data) > 3:
        print(f"     ... and {len(row_data) - 3} more items")

    # Show row slicing with dot notation access
    if demo_category.row_count >= 3:
        print(f"\n📑 Row slicing with dot notation:")
        rows = demo_category[0:3]
        print(f"   Slicing category[0:3] returns {len(rows)} rows")

        # Use dot notation for common attributes (the most elegant approach)
        if "group_PDB" in demo_category.items and "id" in demo_category.items:
            for i, row in enumerate(rows):
                # Direct dot notation access - this is the key pattern to highlight!
                print(f"   Row {i}: row.group_PDB={row.group_PDB}, row.id={row.id}")
        else:
            # Fallback for other attributes
            for i, row in enumerate(rows):
                item1 = item_names[0]
                item2 = item_names[1]
                print(
                    f"   Row {i}: row.{item1}={getattr(row, item1)}, row.{item2}={getattr(row, item2)}"
                )

    print("\n💡 Dot Notation Usage Tips (Pythonic best practices):")
    print("   1. Access data blocks: data.data_BLOCKNAME")
    print("   2. Access categories: block._category_name")
    print("   3. Access item values: category.item_name")
    print("   4. Access row values: row.item_name")
    print("   5. Complex example: data.data[0]._atom_site.Cartn_x[0]")
    print("   6. With slices: for row in category[0:3]: print(row.item_name)")
    print("\n   💪 Dot notation makes your code more readable, elegant, and Pythonic!")


def demonstrate_export_functionality(mmcif, output_dir):
    """Demonstrate JSON export functionality with flat and nested structures."""
    print(f"\n📊 Demonstrating export functionality:")
    print(f"   Supporting JSON format in both flat and nested structures")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create handler
    handler = MMCIFHandler()

    print(f"\n🔧 JSON Export - Nested Structure (Default):")
    # Export to JSON (nested structure)
    json_nested_path = os.path.join(output_dir, "exported_nested.json")
    handler.export(mmcif, format_type="json", file_path=json_nested_path, 
                  structure=StructureFormat.NESTED, permissive=True)
    print(f"   ✅ Exported nested JSON: {json_nested_path}")
    
    print(f"\n🔧 JSON Export - Flat Structure:")
    # Export to JSON (flat structure)
    json_flat_path = os.path.join(output_dir, "exported_flat.json")
    handler.export(mmcif, format_type="json", file_path=json_flat_path, 
                  structure=StructureFormat.FLAT, permissive=True)
    print(f"   ✅ Exported flat JSON: {json_flat_path}")

    print(f"\n📁 Export Summary:")
    export_files = [
        ("JSON Nested", json_nested_path),
        ("JSON Flat", json_flat_path)
    ]
    
    for format_name, file_path in export_files:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"   ✅ {format_name}: {size:,} bytes")
        else:
            print(f"   ❌ {format_name}: Export failed")

    return {
        "json_nested": json_nested_path,
        "json_flat": json_flat_path
    }

 


def demonstrate_import_functionality(output_dir):
    """Demonstrate JSON import functionality with flat and nested structures."""
    
    print(f"\n📥 Demonstrating import functionality:")
    print(f"   Supporting JSON format in both flat and nested structures")

    # Create handler
    handler = MMCIFHandler()

    print(f"\n🔍 Available import methods:")
    print(f"   ✅ mmCIF files: handler.read(file_path) - Full support")
    print(f"   ⚠️ JSON files: handler.load(file_path, format_type='json') - Schema validation required")
    
    print(f"\n📋 Current limitations:")
    print(f"   • JSON import requires strict schema compliance (no None values)")
    
    print(f"\n💡 Recommended workflow:")
    print(f"   1. Parse mmCIF files: container = handler.read('file.cif')")
    print(f"   2. Export to formats: handler.export(container, format_type='json', file_path='out.json')")
    print(f"   3. For import: Focus on mmCIF as primary input format")
    
    # Demonstrate what actually works: mmCIF parsing
    print(f"\n✅ Demonstrating working mmCIF import:")
    try:
        # Show that we can re-parse the mmCIF files we created
        test_files = ['sample_manual.cif', 'sample_programmatic.cif', 'sample_dot_notation.cif']
        
        for test_file in test_files:
            if os.path.exists(test_file):
                container = handler.read(test_file)
                print(f"   ✅ Successfully parsed: {test_file}")
                print(f"      Data blocks: {len(container.data)}")
                if container.data:
                    print(f"      Categories in first block: {len(container.data[0].categories)}")
                break
        else:
            print("   ℹ️ No test mmCIF files found to demonstrate parsing")
            
    except Exception as e:
        print(f"   ❌ mmCIF parsing demonstration failed: {e}")

    # Show file verification for exported formats (without claiming to import them)
    print(f"\n📁 Verifying exported files exist:")
    json_file = os.path.join(output_dir, "exported_data.json")
    
    if os.path.exists(json_file):
        size = os.path.getsize(json_file)
        print(f"   ✅ JSON: {json_file} ({size} bytes)")
    else:
        print(f"   ❌ JSON: {json_file} not found")

    return {"status": "Export verification complete, import capabilities limited"}


def demonstrate_round_trip(mmcif, imported_container, format_name):
    """Demonstrate round-trip validation between original and imported data."""
    print(f"\n🔄 Demonstrating round-trip validation ({format_name}):")

    # Check if imported_container is actually a container object or just a string
    if isinstance(imported_container, str):
        print(f"   ℹ️ {imported_container}")
        print(f"   ⚠️ Round-trip validation skipped: {format_name} import returned metadata, not container")
        return False

    if not hasattr(imported_container, 'data') or not mmcif.data:
        print("   ❌ Missing data blocks for comparison")
        return False

    # Check if blocks match
    if len(mmcif.data) != len(imported_container.data):
        print(
            f"   ❌ Block count mismatch: Original={len(mmcif.data)}, Imported={len(imported_container.data)}"
        )
        return False

    # Compare first block
    original_block = mmcif.data[0]
    imported_block = imported_container.data[0]

    # Compare category count
    if len(original_block.categories) != len(imported_block.categories):
        print(
            f"   ⚠️ Category count differs: Original={len(original_block.categories)}, Imported={len(imported_block.categories)}"
        )

    # Verify key categories exist in both
    common_categories = set(original_block.categories).intersection(
        set(imported_block.categories)
    )
    print(f"   ✓ Found {len(common_categories)} common categories")

    # Check a few sample values
    if common_categories:
        example_category = list(common_categories)[0]
        print(f"   🔍 Checking values in category: {example_category}")

        original_cat = original_block[example_category]
        imported_cat = imported_block[example_category]

        # Compare item names
        original_items = set(original_cat.items)
        imported_items = set(imported_cat.items)
        common_items = original_items.intersection(imported_items)

        if common_items:
            sample_item = list(common_items)[0]
            original_values = original_cat[sample_item]
            imported_values = imported_cat[sample_item]

            # Check if array lengths match
            if len(original_values) == len(imported_values):
                print(
                    f"   ✓ Item '{sample_item}' has {len(original_values)} values in both datasets"
                )

                # Sample check first value
                if original_values[0] == imported_values[0]:
                    print(f"   ✓ First value matches: '{original_values[0]}'")
                else:
                    print(
                        f"   ⚠️ First value differs: Original='{original_values[0]}', Imported='{imported_values[0]}'"
                    )
            else:
                print(
                    f"   ⚠️ Value count differs: Original={len(original_values)}, Imported={len(imported_values)}"
                )

    print(f"   ✅ Round-trip validation complete")
    return True


def demonstrate_sample_data_creation():
    """Demonstrate both manual and programmatic approaches to creating sample data."""
    print("\n📝 Sample Data Creation Methods:")

    # Method 1: Manual file creation (like the existing demo)
    print("\n🖋️  Method 1: Manual mmCIF file creation")
    sample_content = """data_1ABC
_entry.id 1ABC_STRUCTURE
_database_2.database_id PDB
_database_2.database_code 1ABC
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

    manual_file = "sample_manual.cif"
    with open(manual_file, "w") as f:
        f.write(sample_content)
    print(f"   ✅ Created manual sample: {manual_file}")

    # Method 2: Programmatic creation using SLOTH's API with dictionary notation
    print("\n⚙️  Method 2: Programmatic creation using dictionary notation")
    try:
        from sloth.mmcif.models import MMCIFDataContainer, DataBlock, Category

        # Create container and block
        mmcif = MMCIFDataContainer()
        block = DataBlock("1ABC")

        # Create categories and add data using dictionary-style assignment
        # Entry information
        entry_category = Category("_entry")
        entry_category["id"] = ["1ABC_STRUCTURE"]

        # Database information
        database_category = Category("_database_2")
        database_category["database_id"] = ["PDB"]
        database_category["database_code"] = ["1ABC"]

        # Atom site information
        atom_site_category = Category("_atom_site")
        atom_site_category["group_PDB"] = ["ATOM", "ATOM"]
        atom_site_category["id"] = ["1", "2"]
        atom_site_category["type_symbol"] = ["N", "C"]
        atom_site_category["Cartn_x"] = ["10.123", "11.234"]
        atom_site_category["Cartn_y"] = ["20.456", "21.567"]
        atom_site_category["Cartn_z"] = ["30.789", "31.890"]

        # Add categories to block
        block["_entry"] = entry_category
        block["_database_2"] = database_category
        block["_atom_site"] = atom_site_category

        # Add block to container
        mmcif["1ABC"] = block

        # Write using SLOTH
        programmatic_file = "sample_programmatic.cif"
        handler = MMCIFHandler()
        writer = MMCIFWriter()
        with open(programmatic_file, "w") as f:
            writer.write(f, mmcif)
        print(f"   ✅ Created programmatic sample: {programmatic_file}")

        # Method 3: NEW! Auto-creation with Elegant Dot Notation (README example)
        print(
            "\n✨ Method 3: ✨ Auto-creation with Elegant Dot Notation (README example)"
        )
        print(
            "   SLOTH can automatically create nested objects with elegant dot notation!"
        )

        # Create an empty container
        mmcif = MMCIFDataContainer()

        # Use dot notation to auto-create everything - just like in the README!
        mmcif.data_1ABC._entry.id = ["1ABC_STRUCTURE"]
        mmcif.data_1ABC._database_2.database_id = ["PDB"]
        mmcif.data_1ABC._database_2.database_code = ["1ABC"]

        # Add atom data
        mmcif.data_1ABC._atom_site.group_PDB = ["ATOM", "ATOM"]
        mmcif.data_1ABC._atom_site.id = ["1", "2"]
        mmcif.data_1ABC._atom_site.type_symbol = ["N", "C"]
        mmcif.data_1ABC._atom_site.Cartn_x = ["10.123", "11.234"]
        mmcif.data_1ABC._atom_site.Cartn_y = ["20.456", "21.567"]
        mmcif.data_1ABC._atom_site.Cartn_z = ["30.789", "31.890"]

        # Write using SLOTH (just like in the README)
        dot_notation_file = "sample_dot_notation.cif"
        writer = MMCIFWriter()
        with open(dot_notation_file, "w") as f:
            writer.write(f, mmcif)
        print(f"   ✅ Created dot notation sample: {dot_notation_file}")

        # Parse all files to verify they work
        manual_mmcif = handler.read(manual_file)
        programmatic_mmcif = handler.read(programmatic_file)
        auto_creation_mmcif = handler.read(dot_notation_file)

        print(f"\n🔍 Verification:")
        print(f"   Manual approach: {len(manual_mmcif.data[0].categories)} categories")
        print(
            f"   Dictionary approach: {len(programmatic_mmcif.data[0].categories)} categories"
        )
        print(
            f"   Dot notation approach: {len(auto_creation_mmcif.data[0].categories)} categories"
        )

        # Demonstrate the elegance of dot notation access
        print(f"\n💡 Demonstrating dot notation elegance:")
        print(
            f"   auto_creation_mmcif.data_1ABC._entry.id[0]: {auto_creation_mmcif.data_1ABC._entry.id[0]}"
        )
        print(
            f"   auto_creation_mmcif.data_1ABC._atom_site.type_symbol: {auto_creation_mmcif.data_1ABC._atom_site.type_symbol}"
        )

        return manual_file, programmatic_file, dot_notation_file

    except ImportError as e:
        print(f"   ⚠️ Programmatic approach not available: {e}")
        print("   📋 Using manual approach only")
        return manual_file, None, None
    except Exception as e:
        print(f"   ❌ Error in programmatic approach: {e}")
        return manual_file, None, None


def demonstrate_auto_creation():
    """Demonstrate the auto-creation feature as described in the README."""
    print("\n🪄 Auto-Creation Feature Demonstration")
    print("=" * 50)
    print("✨ SLOTH can automatically create nested objects with elegant dot notation!")
    print("   This is the exact example from the README.md file.\n")

    try:
        from sloth.mmcif.models import MMCIFDataContainer
        from sloth.mmcif.handler import MMCIFHandler

        # Create an empty container - this is all you need!
        print("📝 Creating an empty container...")
        mmcif = MMCIFDataContainer()
        print("   mmcif = MMCIFDataContainer()")

        # Use dot notation to auto-create everything
        print("\n🚀 Using dot notation to auto-create everything...")
        print("   mmcif.data_1ABC._entry.id = ['1ABC_STRUCTURE']")
        mmcif.data_1ABC._entry.id = ["1ABC_STRUCTURE"]

        print("   mmcif.data_1ABC._database_2.database_id = ['PDB']")
        mmcif.data_1ABC._database_2.database_id = ["PDB"]

        print("   mmcif.data_1ABC._database_2.database_code = ['1ABC']")
        mmcif.data_1ABC._database_2.database_code = ["1ABC"]

        # Add atom data
        print("\n🧬 Adding atom data...")
        print("   mmcif.data_1ABC._atom_site.group_PDB = ['ATOM', 'ATOM']")
        mmcif.data_1ABC._atom_site.group_PDB = ["ATOM", "ATOM"]

        print("   mmcif.data_1ABC._atom_site.type_symbol = ['N', 'C']")
        mmcif.data_1ABC._atom_site.type_symbol = ["N", "C"]

        print("   mmcif.data_1ABC._atom_site.Cartn_x = ['10.123', '11.234']")
        mmcif.data_1ABC._atom_site.Cartn_x = ["10.123", "11.234"]

        print("   mmcif.data_1ABC._atom_site.Cartn_y = ['20.456', '21.567']")
        mmcif.data_1ABC._atom_site.Cartn_y = ["20.456", "21.567"]

        print("   mmcif.data_1ABC._atom_site.Cartn_z = ['30.789', '31.890']")
        mmcif.data_1ABC._atom_site.Cartn_z = ["30.789", "31.890"]

        # Show what was created automatically
        print(f"\n🔍 What was auto-created:")
        print(f"   📦 Container: {len(mmcif)} block(s)")
        print(f"   🧱 Block '1ABC': {len(mmcif.data_1ABC.categories)} categories")
        print(f"   📂 Categories: {', '.join(mmcif.data_1ABC.categories)}")

        # Show elegant access
        print(f"\n💎 Elegant data access:")
        print(f"   Entry ID: {mmcif.data_1ABC._entry.id[0]}")
        print(f"   Database: {mmcif.data_1ABC._database_2.database_id[0]}")
        print(f"   Atom types: {mmcif.data_1ABC._atom_site.type_symbol}")
        print(f"   X coordinates: {mmcif.data_1ABC._atom_site.Cartn_x}")

        # Write using SLOTH
        print(f"\n💾 Writing to file...")
        writer = MMCIFWriter()
        output_file = "auto_creation_demo.cif"
        with open(output_file, "w") as f:
            writer.write(f, mmcif)
        print(f"   ✅ Saved to: {output_file}")

        # Parse it back to verify
        print(f"\n🔄 Verifying by parsing the file back...")
        handler = MMCIFHandler()
        parsed = handler.read(output_file)
        print(f"   ✅ Successfully parsed {len(parsed)} block(s)")
        print(f"   ✅ Entry ID matches: {parsed.data_1ABC._entry.id[0]}")
        print(f"   ✅ Atom count: {len(parsed.data_1ABC._atom_site.type_symbol)} atoms")

        print(f"\n🎉 Dot notation demonstration completed successfully!")
        print(f"💡 No manual DataBlock or Category creation required!")
        print(f"🚀 Just write what you want, SLOTH creates what you need!")

        return output_file

    except Exception as e:
        print(f"   ❌ Error in auto-creation demonstration: {e}")
        import traceback

        traceback.print_exc()
        return None


def parse_embedded_demo_data_as_string():
    """Return the embedded comprehensive demo data as a string."""
    print(f"📝 Using embedded comprehensive demo data (string mode)")
    print(f"   Data source: Comprehensive protein-ligand complex structure") 
    print(f"   Features: Multiple entities, citations, authors, coordinates, ligands")
    
    return COMPREHENSIVE_DEMO_MMCIF


def main():
    parser = argparse.ArgumentParser(
        description="SLOTH - Structural Loader with On-demand Traversal Handling | Lazy by design. Fast by default.",
        epilog="""Examples:
  python demo.py input.cif output.cif
  python demo.py input.cif output.cif --categories _database_2 _atom_site
  python demo.py input.cif output.cif --validate
  python demo.py --demo  # Run comprehensive demo with sample data
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("input", nargs="?", help="Path to input mmCIF file")
    parser.add_argument("output", nargs="?", help="Path to write modified mmCIF file")
    parser.add_argument(
        "--categories", nargs="+", help="Specific categories to process", default=None
    )
    parser.add_argument(
        "--validate", action="store_true", help="Run validation on categories"
    )
    # Removed --schema-validate flag as it's always included in demo mode
    parser.add_argument("--demo", action="store_true", help="Run comprehensive demo with sample data")

    args = parser.parse_args()

    # Handle demo mode
    if args.demo:
        print("🦥 SLOTH Demo")
        print("Lazy by design. Fast by default.")
        print("=" * 40)
        print("⚡ Now using gemmi backend by default for high-performance parsing!")
        print("   Same elegant API, optimal performance")
        print("   Legacy parser and writer available in sloth.legacy for reference")
        print()

        # Use embedded demo data instead of creating a file
        args.input = None  # No file needed
        args.output = "demo_modified.cif"
        args.validate = True
        args.schema_validate = True

    # Validate arguments
    if not args.demo and (not args.input or not args.output):
        parser.error("Both input and output files are required (or use --demo)")

    # Handle demo mode with embedded data
    if args.demo:
        print(f"\n🔍 Processing: Embedded comprehensive demo data")
        if args.categories:
            print(f"📂 Categories: {', '.join(args.categories)}")
        
        # Setup handler
        validator_factory = ValidatorFactory() if args.validate else None
        handler = MMCIFHandler(validator_factory=validator_factory)
        
        print("⚡ Using gemmi backend for high-performance parsing")
        
        # Parse embedded data
        print("⚡ Parsing embedded demo data...")
        mmcif = parse_embedded_demo_data()
    else:
        # Check input file exists
        if not os.path.exists(args.input):
            print(f"❌ Error: Input file '{args.input}' not found")
            return 1

        print(f"\n🔍 Processing: {args.input}")
        if args.categories:
            print(f"📂 Categories: {', '.join(args.categories)}")

        # Setup handler
        validator_factory = ValidatorFactory() if args.validate else None
        handler = MMCIFHandler(validator_factory=validator_factory)
        
        print("⚡ Using gemmi backend for high-performance parsing")

        # Parse the file
        print("⚡ Parsing file...")
        mmcif = handler.read(args.input, categories=args.categories)

    try:
        # Show file information
        show_file_info(mmcif)

        # Demonstrate sample data creation methods (in demo mode)
        if args.demo:
            demonstrate_sample_data_creation()

            # Demonstrate the auto-creation feature
            demonstrate_auto_creation()

        # Setup validation if requested
        if args.validate and mmcif.data:
            print(f"\n🛡️  Setting up validation...")
            block = mmcif.data[0]

            # Register validators for available categories
            available_categories = block.categories[:2]  # First 2 for demo
            for cat_name in available_categories:
                validator_factory.register_validator(cat_name, category_validator)

            # Register cross-checker if we have multiple categories
            if len(available_categories) >= 2:
                cat_pair = (available_categories[0], available_categories[1])
                validator_factory.register_cross_checker(cat_pair, cross_checker)

            # Run validation
            print(f"🔍 Running validation...")
            for cat_name in available_categories:
                if cat_name in block.categories:
                    try:
                        # Get the validator function from the factory and call it
                        validator_func = validator_factory.get_validator(cat_name)
                        if validator_func:
                            validator_func(cat_name)
                            print(f"   ✅ {cat_name} validation completed")
                        else:
                            print(f"   ⚠️ No validator registered for {cat_name}")
                    except Exception as e:
                        print(f"   ⚠️ {cat_name} validation warning: {e}")

            # Run cross-validation if available
            if len(available_categories) >= 2:
                cat1_name, cat2_name = available_categories[0], available_categories[1]
                if cat1_name in block.categories and cat2_name in block.categories:
                    try:
                        # Get the cross-checker function from the factory and call it
                        cross_checker_func = validator_factory.get_cross_checker((cat1_name, cat2_name))
                        if cross_checker_func:
                            cross_checker_func(cat1_name, cat2_name)
                            print(f"   ✅ Cross-validation between {cat1_name} and {cat2_name} completed")
                        else:
                            print(f"   ⚠️ No cross-checker registered for ({cat1_name}, {cat2_name})")
                    except Exception as e:
                        print(f"   ⚠️ Cross-validation warning: {e}")

        # Demonstrate 2D slicing
        demonstrate_2d_slicing(mmcif)

        # Modify data
        print(f"\n✏️  Modifying data...")
        modify_data(mmcif)

        # Write output
        print(f"\n💾 Writing to: {args.output}")
        writer = MMCIFWriter()
        with open(args.output, "w") as f:
            writer.write(f, mmcif)

        print(f"✅ Successfully processed!")

        # Verify the output
        print(f"\n🔍 Verifying output...")
        verify_data = handler.read(args.output)
        print(f"✅ Output file contains {len(verify_data.data)} data block(s)")

        # Demonstrate 2D slicing if available
        if hasattr(handler, "demonstrate_2d_slicing"):
            demonstrate_2d_slicing(mmcif)

        # Demonstrate export functionality
        output_dir = "exports"
        demonstrate_export_functionality(mmcif, output_dir)

        # Demonstrate import functionality
        import_result = demonstrate_import_functionality(output_dir)

        # Show that we're skipping round-trip validation since import is limited
        print(f"\n🔄 Round-trip Validation Status:")
        print(f"   ℹ️ Round-trip validation requires functional import API")
        print(f"   ⚠️ Currently limited to file verification and mmCIF re-parsing")
        print(f"   💡 Focus: SLOTH excels at mmCIF parsing and JSON export")

        # Clean up demo files if created
        if args.demo and os.path.exists("demo_structure.cif"):
            os.remove("demo_structure.cif")
            print("🧹 Cleaned up demo files")

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
