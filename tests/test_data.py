#!/usr/bin/env python3
"""
Shared test data and constants.

This module provides centralized test data that matches exactly what demo.py uses,
ensuring consistency across all tests.
"""

# Import the comprehensive demo data from demo.py to ensure consistency
import sys
import os
from pathlib import Path

# Add the parent directory to sys.path to import from demo.py
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from demo import COMPREHENSIVE_DEMO_MMCIF, SIMPLE_DEMO_MMCIF
except ImportError:
    # Fallback if import fails - define the data directly
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
"""
    
    SIMPLE_DEMO_MMCIF = """data_1ABC
#
_entry.id                      1ABC
#
_citation.id                   primary
_citation.title                'Test structure for mmCIF validation'
_citation.journal_abbrev       'J. Test'
_citation.year                 2023
#
_entity.id                     1
_entity.type                   polymer
_entity.src_method             man
_entity.pdbx_description       'Test protein'
#
_atom_site.group_PDB           ATOM
_atom_site.id                  1
_atom_site.type_symbol         N
_atom_site.label_atom_id       N
_atom_site.label_comp_id       ALA
_atom_site.label_asym_id       A
_atom_site.label_entity_id     1
_atom_site.label_seq_id        1
_atom_site.Cartn_x             1.0
_atom_site.Cartn_y             2.0
_atom_site.Cartn_z             3.0
_atom_site.occupancy           1.0
_atom_site.B_iso_or_equiv      20.0
_atom_site.pdbx_PDB_model_num  1
#
"""


def get_comprehensive_demo_data():
    """Get the comprehensive demo mmCIF data as a string."""
    return COMPREHENSIVE_DEMO_MMCIF


def get_simple_demo_data():
    """Get the simple demo mmCIF data as a string."""
    return SIMPLE_DEMO_MMCIF


def create_demo_json_data():
    """Create JSON test data based on the demo mmCIF structure."""
    return {
        "data_DEMO": {
            "_entity": {
                "id": "1",
                "type": "polymer",
                "src_method": "man",
                "pdbx_description": "Catalytic domain of model transferase"
            },
            "_citation": [
                {
                    "id": "primary",
                    "title": "High-resolution crystal structure of a model protein complex",
                    "journal_abbrev": "Nat. Struct. Mol. Biol.",
                    "year": "2021"
                },
                {
                    "id": "2",
                    "title": "Structural insights into protein folding mechanisms",
                    "journal_abbrev": "Science",
                    "year": "2021"
                }
            ],
            "_atom_site": [
                {
                    "group_PDB": "ATOM",
                    "id": "1",
                    "type_symbol": "N",
                    "label_atom_id": "N",
                    "label_comp_id": "MET",
                    "label_asym_id": "A",
                    "label_entity_id": "1",
                    "label_seq_id": "1",
                    "Cartn_x": "20.154",
                    "Cartn_y": "6.718",
                    "Cartn_z": "46.973",
                    "occupancy": "1.00",
                    "B_iso_or_equiv": "25.00"
                }
            ]
        }
    }

