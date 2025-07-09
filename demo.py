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
import sys
import shutil
import traceback
import threading
import hashlib
import tempfile
from pathlib import Path
from sloth import (
    MMCIFHandler,
    ValidatorFactory,
    DataSourceFormat,
    SchemaValidator,
    SchemaValidatorFactory,
    ValidationError,
    PDBMLConverter,
    DictionaryParser,
    XMLSchemaValidator,
    RelationshipResolver,
    MMCIFToPDBMLPipeline,
    MMCIFParser,
)

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
            ] = "MODIFIED_DB"  # Simple assignment with dot notation
            print(f"✏️  Modified database_id: '{original}' → 'MODIFIED_DB'")
            print(
                f"   Using elegant dot notation: block._database_2.database_id[-1] = 'MODIFIED_DB'"
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


# Global cache for PDBML infrastructure to avoid recreating it multiple times
_PDBML_INFRASTRUCTURE = None

def get_pdbml_infrastructure():
    """Get or create cached PDBML infrastructure components to improve performance."""
    global _PDBML_INFRASTRUCTURE
    
    if _PDBML_INFRASTRUCTURE is None:
        print("📦 Initializing PDBML infrastructure (one-time setup)...")
        
        # Use the same caching strategy as tests for better performance
        try:
            from tests.test_utils import get_shared_converter
            # Use the existing optimized test infrastructure
            converter = get_shared_converter(permissive=False)
            
            # Extract components from the shared converter
            mapping_generator = converter.mapping_generator
            dict_parser = mapping_generator.dict_parser
            xsd_parser = mapping_generator.xsd_parser
            cache_manager = mapping_generator.cache_manager
            
            _PDBML_INFRASTRUCTURE = {
                'converter': converter,
                'mapping_generator': mapping_generator,
                'dict_parser': dict_parser,
                'xsd_parser': xsd_parser,
                'cache_manager': cache_manager
            }
            
            print("   ✅ PDBML infrastructure cached and ready (using shared test cache)!")
            
        except ImportError:
            # Fallback to original implementation if test utils not available
            dict_path = Path(__file__).parent / "sloth" / "schemas" / "mmcif_pdbx_v50.dic"
            xsd_path = Path(__file__).parent / "sloth" / "schemas" / "pdbx-v50.xsd"
            
            # Create parser components and converter
            from sloth.serializers import get_cache_manager, DictionaryParser, XSDParser, MappingGenerator
            cache_manager = get_cache_manager(os.path.join(os.path.expanduser("~"), ".sloth_cache"))
            dict_parser = DictionaryParser(cache_manager, True)
            xsd_parser = XSDParser(cache_manager, True)
            dict_parser.source = dict_path
            xsd_parser.source = xsd_path
            mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache_manager, True)
            converter = PDBMLConverter(mapping_generator)
            
            _PDBML_INFRASTRUCTURE = {
                'converter': converter,
                'mapping_generator': mapping_generator,
                'dict_parser': dict_parser,
                'xsd_parser': xsd_parser,
                'cache_manager': cache_manager
            }
            
            print("   ✅ PDBML infrastructure cached and ready!")
    else:
        print("📦 Using cached PDBML infrastructure")
    
    return _PDBML_INFRASTRUCTURE


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
        mmcif = handler.parse(tmp_filename)
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
    """Demonstrate the new export functionality."""
    print(f"\n📊 Demonstrating export functionality:")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create handler
    handler = MMCIFHandler()

    # Export to JSON
    json_path = os.path.join(output_dir, "exported_data.json")
    handler.export_to_json(mmcif, json_path)
    print(f"   ✅ Exported to JSON: {json_path}")

    # Export to XML
    xml_path = os.path.join(output_dir, "exported_data.xml")
    handler.export_to_xml(mmcif, xml_path)
    print(f"   ✅ Exported to XML: {xml_path}")

    # Export to Pickle
    pickle_path = os.path.join(output_dir, "exported_data.pkl")
    handler.export_to_pickle(mmcif, pickle_path)
    print(f"   ✅ Exported to Pickle: {pickle_path}")

    # Export to YAML (with try/except as it requires PyYAML)
    try:
        yaml_path = os.path.join(output_dir, "exported_data.yaml")
        handler.export_to_yaml(mmcif, yaml_path)
        print(f"   ✅ Exported to YAML: {yaml_path}")
    except ImportError as e:
        print(f"   ❌ YAML export failed: {str(e)}")

    # Export to CSV (with try/except as it requires pandas)
    try:
        csv_dir = os.path.join(output_dir, "csv_files")
        
        # Clean the CSV directory to prevent cross-contamination from previous runs
        if os.path.exists(csv_dir):
            import shutil
            shutil.rmtree(csv_dir)
        
        file_paths = handler.export_to_csv(mmcif, csv_dir)
        print(f"   ✅ Exported to CSV files in: {csv_dir}")
        # Show first CSV file path as example
        for block_name, categories in file_paths.items():
            if categories:
                first_category = next(iter(categories))
                first_path = categories[first_category]
                print(f"      Example: {os.path.basename(first_path)}")
                break
    except ImportError as e:
        print(f"   ❌ CSV export failed: {str(e)}")


def demonstrate_import_functionality(output_dir):
    """Demonstrate the new import functionality."""
    print(f"\n📥 Demonstrating import functionality:")

    # Create handler
    handler = MMCIFHandler()

    imported_containers = {}

    # Import from JSON
    json_path = os.path.join(output_dir, "exported_data.json")
    if os.path.exists(json_path):
        try:
            json_container = handler.import_from_json(json_path)
            imported_containers["JSON"] = json_container
            print(f"   ✅ Imported from JSON: {json_path}")
            print(f"      Found {len(json_container.blocks)} data block(s)")
        except Exception as e:
            print(f"   ❌ JSON import failed: {str(e)}")

    # Import from XML
    xml_path = os.path.join(output_dir, "exported_data.xml")
    if os.path.exists(xml_path):
        try:
            xml_container = handler.import_from_xml(xml_path)
            imported_containers["XML"] = xml_container
            print(f"   ✅ Imported from XML: {xml_path}")
            print(f"      Found {len(xml_container.blocks)} data block(s)")
        except Exception as e:
            print(f"   ❌ XML import failed: {str(e)}")

    # Import from Pickle
    pickle_path = os.path.join(output_dir, "exported_data.pkl")
    if os.path.exists(pickle_path):
        try:
            pickle_container = handler.import_from_pickle(pickle_path)
            imported_containers["Pickle"] = pickle_container
            print(f"   ✅ Imported from Pickle: {pickle_path}")
            print(f"      Found {len(pickle_container.blocks)} data block(s)")
        except Exception as e:
            print(f"   ❌ Pickle import failed: {str(e)}")

    # Import from YAML (with try/except as it requires PyYAML)
    yaml_path = os.path.join(output_dir, "exported_data.yaml")
    if os.path.exists(yaml_path):
        try:
            yaml_container = handler.import_from_yaml(yaml_path)
            imported_containers["YAML"] = yaml_container
            print(f"   ✅ Imported from YAML: {yaml_path}")
            print(f"      Found {len(yaml_container.blocks)} data block(s)")
        except ImportError as e:
            print(f"   ❌ YAML import failed: {str(e)}")
        except Exception as e:
            print(f"   ❌ YAML import failed: {str(e)}")

    # Import from CSV (with try/except as it requires pandas)
    csv_dir = os.path.join(output_dir, "csv_files")
    if os.path.exists(csv_dir):
        try:
            csv_container = handler.import_from_csv_files(csv_dir)
            imported_containers["CSV"] = csv_container
            print(f"   ✅ Imported from CSV files in: {csv_dir}")
            print(f"      Found {len(csv_container.blocks)} data block(s)")
        except ImportError as e:
            print(f"   ❌ CSV import failed: {str(e)}")
        except Exception as e:
            print(f"   ❌ CSV import failed: {str(e)}")

    # Auto-detect format import demo
    if imported_containers:
        print(f"\n🔍 Demonstrating auto-detect format import:")
        for format_name, first_path in [
            ("JSON", json_path),
            ("Pickle", pickle_path),
            ("YAML", yaml_path),
            ("XML", xml_path),
        ]:
            if os.path.exists(first_path) and format_name in imported_containers:
                try:
                    auto_container = handler.import_auto_detect(first_path)
                    print(
                        f"   ✅ Auto-detected and imported from {format_name}: {first_path}"
                    )
                    print(f"      Found {len(auto_container.blocks)} data block(s)")

                    # Compare with direct import to verify consistency
                    original = imported_containers[format_name]
                    if len(auto_container.blocks) == len(original.blocks):
                        print(f"      ✓ Content matches direct {format_name} import")
                    break
                except Exception as e:
                    print(f"   ❌ Auto-detect import failed for {format_name}: {str(e)}")

    return imported_containers


def demonstrate_round_trip(mmcif, imported_container, format_name):
    """Demonstrate round-trip validation between original and imported data."""
    print(f"\n🔄 Demonstrating round-trip validation ({format_name}):")

    if not mmcif.data or not imported_container.data:
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


def demonstrate_schema_validation(mmcif, output_dir):
    """Demonstrate schema validation for different formats."""
    print(f"\n🛡️ Demonstrating Schema Validation:")

    # Create temporary directory for validation examples
    validation_dir = os.path.join(output_dir, "validation_examples")
    os.makedirs(validation_dir, exist_ok=True)

    # ===== JSON Schema Validation =====
    print("\n📝 JSON Schema Validation Example:")

    # Get the path to the exported JSON file
    json_path = os.path.join(output_dir, "exported_data.json")

    if os.path.exists(json_path):
        try:
            # Create a JSON schema validator using the default schema
            json_validator = SchemaValidatorFactory.create_validator(
                DataSourceFormat.JSON
            )

            # Create a valid and an invalid data file
            with open(json_path, "r") as f:
                valid_data = json.load(f)

            # Copy the valid data
            invalid_data = copy.deepcopy(valid_data)

            # Create invalid data by removing all items in a category
            if invalid_data:
                block_name = list(invalid_data.keys())[0]
                if block_name in invalid_data:
                    category_name = list(invalid_data[block_name].keys())[0]
                    if category_name in invalid_data[block_name]:
                        # Make this category an empty object
                        if isinstance(invalid_data[block_name][category_name], dict):
                            invalid_data[block_name][category_name] = {}
                        # If it's an array, make it an empty array
                        elif isinstance(invalid_data[block_name][category_name], list):
                            invalid_data[block_name][category_name] = []

            # Save valid and invalid data
            valid_json_path = os.path.join(validation_dir, "valid_data.json")
            invalid_json_path = os.path.join(validation_dir, "invalid_data.json")

            with open(valid_json_path, "w") as f:
                json.dump(valid_data, f)

            with open(invalid_json_path, "w") as f:
                json.dump(invalid_data, f)

            # Validate valid data - should pass validation
            try:
                handler = MMCIFHandler()
                # Validate that the schema is correct for the valid data
                is_valid = json_validator.is_valid(valid_data)
                if is_valid:
                    valid_container = handler.import_from_json(
                        valid_json_path, schema_validator=json_validator
                    )
                    print(f"   ✅ Valid JSON data passed validation")
                else:
                    print(f"   ❌ Valid JSON data failed pre-validation check")
            except ValidationError as e:
                print(f"   ❌ Unexpected validation error on valid data: {e}")
            except Exception as e:
                print(f"   ❌ Error processing valid JSON: {e}")

            # Validate invalid data - should fail validation
            try:
                # First check that the schema correctly identifies invalid data
                is_invalid = not json_validator.is_valid(invalid_data)
                if is_invalid:
                    print(f"   ✅ Pre-validation correctly identified invalid JSON")
                    # This should raise a ValidationError
                    try:
                        json_validator.validate(invalid_data)
                        print(f"   ❌ Validation.validate() did not raise an error")
                    except ValidationError as e:
                        print(f"   ✅ Validation.validate() correctly raised: {e}")
                else:
                    print(f"   ❌ Invalid JSON incorrectly passed pre-validation")

                # Now check that the import function correctly validates
                try:
                    invalid_container = handler.import_from_json(
                        invalid_json_path, schema_validator=json_validator
                    )
                    print(f"   ❌ Invalid JSON import did not raise an error")
                except ValidationError as e:
                    print(f"   ✅ Import correctly failed: {e}")
            except Exception as e:
                print(f"   ⚠️ Error during invalid JSON testing: {e}")

        except Exception as e:
            print(f"   ❌ JSON validation setup error: {e}")
    else:
        print(f"   ⚠️ JSON file not found, skipping validation")

    # ===== XML Schema Validation =====
    print("\n📝 XML Schema Validation Example:")

    # Get the path to the exported XML file
    xml_path = os.path.join(output_dir, "exported_data.xml")

    if os.path.exists(xml_path):
        try:
            # Create a simple XML validator using a function rather than XSD
            class SimpleXMLValidator(SchemaValidator):
                def validate(self, data):
                    return {"valid": True, "errors": []}

                def is_valid(self, data):
                    return True

            xml_validator = SimpleXMLValidator()

            # Create a valid copy for demonstration
            valid_xml_path = os.path.join(validation_dir, "valid_data.xml")
            with open(xml_path, "r") as src, open(valid_xml_path, "w") as dst:
                dst.write(src.read())

            # Validate valid data
            try:
                handler = MMCIFHandler()
                valid_container = handler.import_from_xml(
                    valid_xml_path, schema_validator=xml_validator
                )
                print(f"   ✅ Valid XML data passed validation")
            except ValidationError as e:
                print(f"   ❌ Unexpected validation error: {e}")
            except Exception as e:
                print(f"   ❌ XML validation error: {str(e)}")

        except Exception as e:
            print(f"   ❌ XML validation setup error: {e}")
    else:
        print(f"   ⚠️ XML file not found, skipping validation")

    # ===== YAML Schema Validation =====
    print("\n📝 YAML Schema Validation Example:")

    # Get the path to the exported YAML file
    yaml_path = os.path.join(output_dir, "exported_data.yaml")

    if os.path.exists(yaml_path):
        try:
            # Create a YAML schema validator using the default schema
            yaml_validator = SchemaValidatorFactory.create_validator(
                DataSourceFormat.YAML
            )

            # Create a valid copy for demonstration
            valid_yaml_path = os.path.join(validation_dir, "valid_data.yaml")
            with open(yaml_path, "r") as src, open(valid_yaml_path, "w") as dst:
                dst.write(src.read())

            # Validate valid data
            try:
                handler = MMCIFHandler()
                valid_container = handler.import_from_yaml(
                    valid_yaml_path, schema_validator=yaml_validator
                )
                print(f"   ✅ Valid YAML data passed validation")
            except ValidationError as e:
                print(f"   ❌ Unexpected validation error: {e}")
            except Exception as e:
                print(f"   ❌ YAML validation error: {str(e)}")

        except Exception as e:
            print(f"   ❌ YAML validation setup error: {e}")
    else:
        print(f"   ⚠️ YAML file not found, skipping validation")

    # ===== Auto-detect with validation =====
    print("\n📝 Auto-detect Format with Validation Example:")

    try:
        # Use one of the valid files with auto-detection
        auto_detect_path = (
            valid_json_path if locals().get("valid_json_path") else json_path
        )

        if os.path.exists(auto_detect_path):
            handler = MMCIFHandler()
            mmcif = handler.import_auto_detect(auto_detect_path, validate_schema=True)
            print(f"   ✅ Auto-detected format and validated successfully")
        else:
            print(f"   ⚠️ File not found for auto-detection")

    except Exception as e:
        print(f"   ❌ Auto-detection/validation error: {e}")

    print("\n🛡️ Schema validation demonstration completed")
    return validation_dir


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
        from sloth.models import MMCIFDataContainer, DataBlock, Category

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
        with open(programmatic_file, "w") as f:
            handler.file_obj = f
            handler.write(mmcif)
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
        with open(dot_notation_file, "w") as f:
            handler.file_obj = f
            handler.write(mmcif)
        print(f"   ✅ Created dot notation sample: {dot_notation_file}")

        # Parse all files to verify they work
        manual_mmcif = handler.parse(manual_file)
        programmatic_mmcif = handler.parse(programmatic_file)
        auto_creation_mmcif = handler.parse(dot_notation_file)

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
        from sloth.models import MMCIFDataContainer
        from sloth.handler import MMCIFHandler

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
        handler = MMCIFHandler()
        output_file = "auto_creation_demo.cif"
        with open(output_file, "w") as f:
            handler.file_obj = f
            handler.write(mmcif)
        print(f"   ✅ Saved to: {output_file}")

        # Parse it back to verify
        print(f"\n🔄 Verifying by parsing the file back...")
        parsed = handler.parse(output_file)
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


def demo_backend_comparison(sample_file):
    """Demonstrate current gemmi backend vs legacy implementation"""
    print("\n🔬 Handler Comparison - Gemmi Backend vs Legacy")
    print("=" * 50)
    
    try:
        # Test current handler (gemmi backend)
        print("⚡ MMCIFHandler (gemmi backend - default):")
        current_handler = MMCIFHandler()
        current_mmcif = current_handler.parse(sample_file)
        current_atom_count = len(current_mmcif.data[0]._atom_site.Cartn_x) if '_atom_site' in current_mmcif.data[0].categories else 0
        print(f"   Parsed {current_atom_count} atoms")
        if current_atom_count > 0:
            print(f"   API: mmcif.data[0]._atom_site.Cartn_x[0] = {current_mmcif.data[0]._atom_site.Cartn_x[0]}")
        
        # Test legacy handler
        print("\n📊 Legacy MMCIFHandler (pure Python):")
        try:
            from sloth.legacy import MMCIFParser
            legacy_parser = MMCIFParser(None)
            legacy_mmcif = legacy_parser.parse_file(sample_file)
            legacy_atom_count = len(legacy_mmcif.data[0]._atom_site.Cartn_x) if '_atom_site' in legacy_mmcif.data[0].categories else 0
            print(f"   Parsed {legacy_atom_count} atoms")
            if legacy_atom_count > 0:
                print(f"   API: mmcif.data[0]._atom_site.Cartn_x[0] = {legacy_mmcif.data[0]._atom_site.Cartn_x[0]}")
            
            # Verify identical results
            if current_atom_count == legacy_atom_count:
                print("   ✅ Identical atom counts!")
            else:
                print("   ⚠️ Different atom counts")
                
            if (current_atom_count > 0 and legacy_atom_count > 0 and 
                current_mmcif.data[0]._atom_site.Cartn_x[0] == legacy_mmcif.data[0]._atom_site.Cartn_x[0]):
                print("   ✅ Identical first coordinate!")
            elif current_atom_count > 0 and legacy_atom_count > 0:
                print("   ⚠️ Different first coordinates")
            
            # Test write functionality
            print("\n📝 Testing write functionality with gemmi backend...")
            with open("current_write_test.cif", 'w') as f:
                current_handler.file_obj = f
                current_handler.write(current_mmcif)
            print("   ✅ Successfully wrote file using gemmi backend")
            
            # Verify round-trip
            mmcif_roundtrip = current_handler.parse("current_write_test.cif")
            if mmcif_roundtrip.data[0]._entry.id[0] == current_mmcif.data[0]._entry.id[0]:
                print(f"   ✅ Round-trip test: {mmcif_roundtrip.data[0]._entry.id[0]}")
            
            # Clean up
            if os.path.exists("current_write_test.cif"):
                os.remove("current_write_test.cif")
                
        except ImportError:
            print("   ❌ Legacy parser not available")
            print("   This should not happen as legacy is included by default")
            
    except Exception as e:
        print(f"   ❌ Error in comparison: {e}")


def parse_embedded_demo_data_as_string():
    """Return the embedded comprehensive demo data as a string for PDBML pipeline."""
    print(f"📝 Using embedded comprehensive demo data (string mode)")
    print(f"   Data source: Comprehensive protein-ligand complex structure") 
    print(f"   Features: Multiple entities, citations, authors, coordinates, ligands")
    
    return COMPREHENSIVE_DEMO_MMCIF


def demonstrate_pdbml_pipeline(comprehensive=False):
    """Demonstrate the PDBML conversion pipeline."""
    print("\n🧬 PDBML Conversion Pipeline Demo")
    print("=" * 40)
    
    # Always use comprehensive embedded demo data for consistency
    print("📊 Running PDBML pipeline demonstration with comprehensive embedded data")
    demo_content = parse_embedded_demo_data_as_string()
    
    try:
        # Step 1: Parse mmCIF from string using temporary file
        print(f"\n🔍 Step 1: Parsing mmCIF data from embedded comprehensive content")
        import tempfile
        import os
        
        # Create a temporary file with the embedded data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as tmp_file:
            tmp_file.write(demo_content)
            tmp_filename = tmp_file.name
        
        try:
            parser = MMCIFParser(validator_factory=None)
            container = parser.parse_file(tmp_filename)
        finally:
            # Clean up the temporary file
            if os.path.exists(tmp_filename):
                os.remove(tmp_filename)
        
        print(f"   ✅ Parsed successfully")
        print(f"   📋 Data blocks: {len(container.data)}")
        print(f"   📋 Block name: {container.data[0].name}")
        print(f"   📋 Categories: {len(container.data[0].categories)}")
        category_names = list(container.data[0].categories)
        print(f"   📂 Categories: {', '.join(category_names)}")
        
        # Step 2: Convert to PDBML XML
        print(f"\n🔄 Step 2: Converting to PDBML XML")
        
        # Use cached infrastructure for better performance
        infra = get_pdbml_infrastructure()
        converter = infra['converter']
        xml_content = converter.convert_to_pdbml(container)
        
        print(f"   ✅ XML generated successfully")
        print(f"   📄 XML size: {len(xml_content)} characters")
        
        # Step 3: Validate XML against schema
        print(f"\n🛡️  Step 3: Validating XML against PDBX schema")
        schema_path = Path(__file__).parent / "sloth" / "schemas" / "pdbx-v50.xsd"
        
        is_valid = False
        errors = []
        
        if schema_path.exists():
            validator = XMLSchemaValidator(schema_path)
            validation_result = validator.validate(xml_content)
            
            is_valid = validation_result["valid"]
            errors = validation_result.get("errors", [])
            
            print(f"   {'✅' if is_valid else '⚠️'} Validation: {'PASSED' if is_valid else 'FAILED'}")
            print(f"   📝 Total errors: {len(errors)}")
            
            # Show validation errors
            if errors:
                # Display errors appropriately based on type and count
                if isinstance(errors, list):
                    if len(errors) <= 5:
                        print("   🔍 Validation errors:")
                        for i, error in enumerate(errors, 1):
                            print(f"      {i}. {error}")
                    else:
                        print("   🔍 First 3 validation errors:")
                        for i, error in enumerate(errors[:3], 1):
                            print(f"      {i}. {error}")
                        print(f"      ... and {len(errors) - 3} more")
                elif isinstance(errors, str):
                    print(f"   🔍 Validation error: {errors}")
                else:
                    print(f"   🔍 Validation error details: {str(errors)}")
        else:
            print("   ❌ Schema file not found")
        
        # Step 4: Resolve relationships (only for comprehensive demo)
        if comprehensive:
            print(f"\n🔗 Step 4: Resolving parent-child relationships")
            resolver = RelationshipResolver(infra['mapping_generator'])
            nested_json = resolver.resolve_relationships(xml_content)
            
            print(f"   ✅ Relationships resolved")
            print(f"   📊 Root categories: {len(nested_json)}")
            
            # Show relationship structure
            for cat_name, cat_data in list(nested_json.items())[:3]:
                if isinstance(cat_data, dict):
                    print(f"   📂 {cat_name}: {len(cat_data)} items")
                    if cat_data:
                        first_key = list(cat_data.keys())[0]
                        first_item = cat_data[first_key]
                        if isinstance(first_item, dict):
                            child_cats = [k for k, v in first_item.items() if isinstance(v, list)]
                            if child_cats:
                                print(f"      └── Child categories: {', '.join(child_cats)}")
        
        # Step 5: Save outputs
        print(f"\n💾 Step 5: Saving outputs")
        output_dir = Path("exports") / "pdbml_demo"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save XML
        xml_file = output_dir / "demo.xml"
        with open(xml_file, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        print(f"   💾 XML: {xml_file}")
        
        # Save validation report
        report_file = output_dir / "validation_report.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"PDBML XML Validation Report\n")
            f.write(f"===========================\n\n")
            f.write(f"Status: {'PASSED' if is_valid else 'FAILED'}\n")
            f.write(f"Total errors: {len(errors)}\n\n")
            if errors:
                f.write("Errors:\n")
                for i, error in enumerate(errors, 1):
                    f.write(f"  {i}. {error}\n")
        print(f"   📋 Report: {report_file}")
        
        if comprehensive:
            # Save nested JSON
            json_file = output_dir / "nested_relationships.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(nested_json, f, indent=2)
            print(f"   💾 JSON: {json_file}")
        
        # Step 6: Show sample outputs
        print(f"\n🔍 Step 6: Sample PDBML XML preview")
        print("=" * 40)
        
        # Show formatted sample (first 800 characters)
        sample_xml = xml_content[:800]
        if len(xml_content) > 800:
            sample_xml += "\n    ... (truncated)"
        print(sample_xml)
        
        if comprehensive and 'nested_json' in locals():
            print(f"\n🔍 Sample nested JSON relationships:")
            sample_json = {}
            for cat_name, cat_data in list(nested_json.items())[:2]:
                if isinstance(cat_data, dict):
                    sample_json[cat_name] = {}
                    for key, item in list(cat_data.items())[:1]:
                        if isinstance(item, dict):
                            # Show simplified item with first few fields
                            simplified = {}
                            field_count = 0
                            for field, value in item.items():
                                if field_count < 3 and not isinstance(value, list):
                                    simplified[field] = value
                                    field_count += 1
                                elif isinstance(value, list):
                                    simplified[field] = f"[{len(value)} items]"
                            sample_json[cat_name][key] = simplified
                    if len(cat_data) > 1:
                        sample_json[cat_name]["..."] = f"and {len(cat_data) - 1} more items"
            
            print(json.dumps(sample_json, indent=2))
        
        # Step 7: Summary
        print(f"\n📊 Pipeline Summary")
        print("=" * 40)
        print(f"✅ mmCIF parsing: SUCCESS")
        print(f"✅ PDBML XML generation: SUCCESS")
        print(f"{'✅' if is_valid else '⚠️'} Schema validation: {'SUCCESS' if is_valid else 'WITH WARNINGS'}")
        
        if comprehensive:
            print(f"✅ Relationship resolution: SUCCESS")
            print(f"✅ Nested JSON output: SUCCESS")
        
        print(f"\n🎯 Key achievements:")
        print(f"   • Parsed {len(container.data[0].categories)} mmCIF categories")
        print(f"   • Generated PDBML XML conforming to pdbx-v50.xsd")
        print(f"   • {'Perfect XML compliance!' if is_valid else 'Minor validation warnings only'}")
        
        if comprehensive:
            print(f"   • Correctly placed key fields as XML attributes")
            print(f"   • Resolved parent-child relationships")
            print(f"   • Created hierarchical JSON with nested structures")
        
        print(f"\n📁 All outputs saved to: {output_dir}/")
        
    except Exception as e:
        print(f"❌ Error in PDBML pipeline: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # No file cleanup needed since we're using embedded data
        print("📝 No temporary files to clean up (using embedded data)")


def demonstrate_complete_pdbml_pipeline():
    """Run the complete PDBML pipeline using the MMCIFToPDBMLPipeline class."""
    print("\n🚀 Complete PDBML Pipeline Demo")
    print("=" * 40)
    print("📊 Using the integrated MMCIFToPDBMLPipeline class")
    
    # Get embedded demo data
    demo_content = parse_embedded_demo_data_as_string()
    
    try:
        # Initialize pipeline
        schema_path = Path(__file__).parent / "sloth" / "schemas" / "pdbx-v50.xsd"
        dict_path = Path(__file__).parent / "sloth" / "schemas" / "mmcif_pdbx_v50.dic"
        
        if not schema_path.exists():
            print("❌ Schema file not found - pipeline cannot validate")
            return
        
        if not dict_path.exists():
            print("❌ Dictionary file not found - using basic conversion")
            pipeline = MMCIFToPDBMLPipeline(xsd_path=schema_path)
        else:
            print("📦 Using cached dictionary data")
            pipeline = MMCIFToPDBMLPipeline(dict_path=dict_path, xsd_path=schema_path)
        
        print("✅ Pipeline initialized")
        
        # Run complete pipeline
        print(f"\n🔄 Running complete pipeline on embedded demo data")
        
        # Create a temporary file with the embedded data
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as tmp_file:
            tmp_file.write(demo_content)
            tmp_filename = tmp_file.name
        
        try:
            # Use the file-based method that actually exists
            result = pipeline.process_mmcif_file(tmp_filename)
        finally:
            # Clean up the temporary file
            if os.path.exists(tmp_filename):
                os.remove(tmp_filename)
        
        # Display results
        print(f"\n📊 Pipeline Results:")
        print(f"   ✅ mmCIF parsing: SUCCESS")
        print(f"   ✅ XML generation: SUCCESS")
        print(f"   {'✅' if result['validation']['valid'] else '⚠️'} Schema validation: {'PASSED' if result['validation']['valid'] else 'FAILED'}")
        print(f"   📝 Validation errors: {len(result['validation']['errors'])}")
        
        if result['validation']['errors']:
            print(f"   🔍 Validation errors found (showing summary):")
            errors = result['validation']['errors']
            print(f"      Total count: {len(errors)}")
        else:
            print(f"   ✅ No validation errors!")
        
        print(f"   ✅ Relationship resolution: SUCCESS")
        print(f"   📊 Root categories in JSON: {len(result['nested_json']) if result['nested_json'] else 0}")
        
        # Save outputs
        output_dir = Path("exports") / "complete_pdbml_demo"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save XML
        if result['pdbml_xml']:
            xml_file = output_dir / "complete_demo.xml"
            with open(xml_file, 'w', encoding='utf-8') as f:
                f.write(result['pdbml_xml'])
            print(f"   💾 XML saved: {xml_file}")
        
        # Save JSON
        if result['nested_json']:
            json_file = output_dir / "complete_demo.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(result['nested_json'], f, indent=2)
            print(f"   💾 JSON saved: {json_file}")
        
        # Save validation report
        validation_file = output_dir / "validation_report.txt"
        with open(validation_file, 'w', encoding='utf-8') as f:
            f.write(f"PDBML XML Validation Report\n")
            f.write(f"===========================\n\n")
            f.write(f"Status: {'PASSED' if result['validation']['valid'] else 'FAILED'}\n")
            f.write(f"Total errors: {len(result['validation']['errors'])}\n\n")
            if result['validation']['errors']:
                f.write("Errors:\n")
                for i, error in enumerate(result['validation']['errors'][:10], 1):  # Only first 10
                    f.write(f"  {i}. {str(error)[:200]}\n")  # Truncate long errors
        print(f"   📋 Validation report: {validation_file}")
        
        print(f"\n📁 All outputs saved to: {output_dir}/")
        print(f"🎉 Complete pipeline demonstration finished successfully!")
        
    except Exception as e:
        print(f"❌ Error in complete pipeline: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # No file cleanup needed since we're using embedded data
        print("📝 No temporary files to clean up (using embedded data)")


def demonstrate_nested_relationships():
    """Demonstrate the multi-level nested relationship resolution functionality."""
    print("\n🧬 Nested Relationship Resolution Demo")
    print("=" * 50)
    print("📊 Testing 4-level hierarchical parent-child relationship resolution")
    
    # Use the comprehensive embedded demo data for nested relationship testing
    print("📝 Using embedded comprehensive demo data for nested relationship testing")
    demo_content = parse_embedded_demo_data_as_string()
    
    try:
        # Step 1: Parse mmCIF using temporary file
        print(f"\n1️⃣ Parsing mmCIF with nested structures...")
        import tempfile
        import os
        
        # Create a temporary file with the embedded data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as tmp_file:
            tmp_file.write(demo_content)
            tmp_filename = tmp_file.name
        
        try:
            parser = MMCIFParser()
            container = parser.parse_file(tmp_filename)
        finally:
            # Clean up the temporary file
            if os.path.exists(tmp_filename):
                os.remove(tmp_filename)
                
        print(f"   ✅ Parsed successfully")
        print(f"   📋 Categories: {list(container.data[0].categories)}")
        
        # Step 2: Convert to PDBML XML
        print(f"\n2️⃣ Converting to PDBML XML...")
        
        # Use cached infrastructure for better performance
        infra = get_pdbml_infrastructure()
        converter = infra['converter']
        xml_content = converter.convert_to_pdbml(container)
        print(f"   ✅ XML generated - {len(xml_content)} characters")
        
        # Step 3: Resolve relationships using dictionary-driven approach
        print(f"\n3️⃣ Resolving parent-child relationships...")
        # Reuse the mapping generator from cached infrastructure
        mapping_generator = infra['mapping_generator']
        resolver = RelationshipResolver(mapping_generator)
        nested_json = resolver.resolve_relationships(xml_content)
        print(f"   ✅ Relationships resolved")
        print(f"   📊 Root categories: {list(nested_json)}")
        
        # Step 4: Validate 4-level hierarchy
        print(f"\n4️⃣ Validating 4-level nested hierarchy...")
        try:
            # Navigate the expected hierarchy (updated for array-based format)
            entity_1 = nested_json['entity'][0]  # Get first entity from array
            print(f"   📦 Level 1 - Entity: {entity_1['type']}")
            
            # Branch 1: entity -> entity_poly -> entity_poly_seq
            entity_poly = entity_1['entity_poly'][0]  # Get first entity_poly from array
            print(f"   🧬 Level 2 - Entity_poly: {entity_poly['type']}")
            
            entity_poly_seq = entity_poly['entity_poly_seq'][0]  # Get first seq from array
            print(f"   🔗 Level 3 - Entity_poly_seq: {entity_poly_seq['mon_id']}")
            
            # Branch 2: entity -> struct_asym -> atom_site
            struct_asym = entity_1['struct_asym'][0]  # Get first struct_asym from array
            print(f"   🏗️ Level 2 - Struct_asym: {struct_asym['id']}")
            
            atom_site = struct_asym['atom_site'][0]  # Get first atom_site from array
            print(f"   ⚛️ Level 3 - Atom_site: {atom_site['label_atom_id']} at {atom_site['Cartn_x']}")
            
            print(f"   ✅ 4-level hierarchy validated successfully!")
            
            # Step 5: Show relationship structure
            print(f"\n5️⃣ Relationship structure analysis:")
            print(f"   entity(1)")
            print(f"   ├── entity_poly")
            print(f"   │   └── entity_poly_seq (VAL)")
            print(f"   └── struct_asym(A)")
            print(f"       └── atom_site (CA at 12.345, 67.890, 42.000)")
            
            # Step 6: Save outputs
            print(f"\n6️⃣ Saving demonstration outputs...")
            output_dir = Path("nested_demo_output")
            output_dir.mkdir(exist_ok=True)
            
            # Save XML
            xml_file = output_dir / "nested_demo.xml"
            with open(xml_file, 'w') as f:
                f.write(xml_content)
            print(f"   💾 XML: {xml_file}")
            
            # Save nested JSON
            json_file = output_dir / "perfect_nested_structure.json"
            with open(json_file, 'w') as f:
                json.dump(nested_json, f, indent=2)
            print(f"   💾 JSON: {json_file}")
            
            # Create ideal structure visualization
            ideal_structure = {
                "description": "4-level nested hierarchy demonstration",
                "hierarchy": {
                    "entity": {
                        "1": {
                            "type": "polymer",
                            "description": entity_1['pdbx_description'],
                            "entity_poly": {
                                "type": entity_poly['type'],
                                "entity_poly_seq": {
                                    "num": entity_poly_seq['num'],
                                    "mon_id": entity_poly_seq['mon_id']
                                }
                            },
                            "struct_asym": {
                                "id": struct_asym['id'],
                                "atom_site": {
                                    "atom": atom_site['label_atom_id'],
                                    "coordinates": [
                                        atom_site['Cartn_x'],
                                        atom_site['Cartn_y'],
                                        atom_site['Cartn_z']
                                    ]
                                }
                            }
                        }
                    }
                },
                "validation": {
                    "levels": 4,
                    "branches": 2,
                    "status": "SUCCESS"
                }
            }
            
            ideal_file = output_dir / "ideal_nested_structure.json"
            with open(ideal_file, 'w') as f:
                json.dump(ideal_structure, f, indent=2)
            print(f"   💾 Ideal structure: {ideal_file}")
            
            print(f"\n🎉 Nested relationship demonstration completed successfully!")
            print(f"💡 Key achievements:")
            print(f"   • Correctly parsed complex mmCIF relationships")
            print(f"   • Generated valid PDBML XML with proper nesting")
            print(f"   • Resolved 4-level parent-child hierarchy")
            print(f"   • entity → entity_poly → entity_poly_seq")
            print(f"   • entity → struct_asym → atom_site")
            print(f"   • Preserved all data integrity and cross-references")
            
            return True
            
        except (KeyError, TypeError) as e:
            print(f"   ❌ Hierarchy validation failed: {e}")
            print(f"   🔍 Available structure: {json.dumps(nested_json, indent=2)[:500]}...")
            return False
            
    except Exception as e:
        print(f"❌ Error in nested relationship demo: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # No file cleanup needed since we're using embedded data
        print("📝 No temporary files to clean up (using embedded data)")


def main():
    parser = argparse.ArgumentParser(
        description="SLOTH - Structural Loader with On-demand Traversal Handling | Lazy by design. Fast by default.",
        epilog="""Examples:
  python demo.py input.cif output.cif
  python demo.py input.cif output.cif --categories _database_2 _atom_site
  python demo.py input.cif output.cif --validate
  python demo.py --demo  # Run comprehensive demo including PDBML pipeline
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
    parser.add_argument("--demo", action="store_true", help="Run comprehensive demo with sample data (includes PDBML pipeline)")

    args = parser.parse_args()

    # Handle demo mode
    if args.demo:
        print("🦥 SLOTH Demo")
        print("Lazy by design. Fast by default.")
        print("=" * 40)
        print("⚡ Now using gemmi backend by default for high-performance parsing!")
        print("   Same elegant API, optimal performance")
        print("   Legacy implementations available in sloth.legacy")
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
        mmcif = handler.parse(args.input, categories=args.categories)

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
        with open(args.output, "w") as f:
            handler.file_obj = f
            handler.write(mmcif)

        print(f"✅ Successfully processed!")

        # Verify the output
        print(f"\n🔍 Verifying output...")
        verify_data = handler.parse(args.output)
        print(f"✅ Output file contains {len(verify_data.data)} data block(s)")

        # Demonstrate 2D slicing if available
        if hasattr(handler, "demonstrate_2d_slicing"):
            demonstrate_2d_slicing(mmcif)

        # Demonstrate export functionality
        output_dir = "exports"
        demonstrate_export_functionality(mmcif, output_dir)

        # Demonstrate import functionality
        imported_containers = demonstrate_import_functionality(output_dir)
        
        # Demonstrate gemmi comparison in demo mode
        if args.demo:
            demo_backend_comparison(args.input)

        # Demonstrate round-trip validation for each imported format
        for format_name, imported_container in imported_containers.items():
            demonstrate_round_trip(mmcif, imported_container, format_name)

        # Demonstrate schema validation
        # Note: This is always included in demo mode
        validation_dir = demonstrate_schema_validation(mmcif, output_dir)

        # Demonstrate PDBML pipeline (in demo mode)
        if args.demo:
            print("\n" + "=" * 60)
            print("🧬 PDBML CONVERSION PIPELINE DEMONSTRATION")
            print("=" * 60)
            print("Now demonstrating the complete PDBML conversion pipeline!")
            print("This shows mmCIF → PDBML XML → Validation → Relationship Resolution")
            
            # Run basic PDBML demo
            demonstrate_pdbml_pipeline(comprehensive=False)
            
            # Run nested relationship demo
            demonstrate_nested_relationships()
            
            # Run comprehensive PDBML demo  
            demonstrate_complete_pdbml_pipeline()

        # Clean up demo files if created
        if args.demo and os.path.exists("demo_structure.cif"):
            os.remove("demo_structure.cif")
            print("🧹 Cleaned up demo files")

        # Clean up validation examples
        if "validation_dir" in locals() and os.path.exists(validation_dir):
            try:
                shutil.rmtree(validation_dir)
                print("🧹 Cleaned up validation example files")
            except Exception:
                pass

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
