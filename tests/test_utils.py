#!/usr/bin/env python3
"""
Shared test utilities and resources.

This module provides shared resources like caching mechanisms to improve
test performance by reusing expensive-to-create objects across test cases.
"""

import os
import tempfile
from pathlib import Path
from functools import lru_cache
from typing import Dict, Any

from sloth.mmcif.serializer import (
    MappingGenerator, DictionaryParser, 
    CacheManager, get_cache_manager, RelationshipResolver
)
from sloth.mmcif import JSONExporter

# Global exporter cache
_GLOBAL_EXPORTERS: Dict[str, JSONExporter] = {}

# Global paths for schemas
DICT_PATH = Path(__file__).parent.parent / "sloth" / "mmcif" / "schemas" / "mmcif_pdbx_v50.dic"

# Global cache directory
CACHE_DIR = os.path.join(tempfile.gettempdir(), "sloth_test_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Global caching instance
GLOBAL_CACHE = get_cache_manager(CACHE_DIR)


@lru_cache(maxsize=2)
def get_shared_exporter(quiet: bool = True) -> JSONExporter:
    """
    Get a shared JSON exporter instance.
    
    This function caches and reuses exporter instances across all tests,
    which dramatically improves test performance.
    
    Args:
        quiet: Whether the exporter should suppress output messages.
        
    Returns:
        A cached JSONExporter instance.
    """
    cache_key = f"exporter_{quiet}"
    
    if cache_key in _GLOBAL_EXPORTERS:
        return _GLOBAL_EXPORTERS[cache_key]
        
    # Create exporter with caching
    exporter = JSONExporter(cache_dir=CACHE_DIR, quiet=quiet)
    
    # Cache exporter for future use
    _GLOBAL_EXPORTERS[cache_key] = exporter
    
    return exporter


@lru_cache(maxsize=1)
def get_shared_mapping_generator() -> MappingGenerator:
    """
    Get a shared mapping generator instance.
    
    This function caches and reuses the mapping generator across all tests
    for improved performance.
    
    Returns:
        A cached MappingGenerator instance.
    """
    dict_parser = DictionaryParser(GLOBAL_CACHE, quiet=True)
    dict_parser.source = DICT_PATH
    
    mapping_generator = MappingGenerator(dict_parser, GLOBAL_CACHE, quiet=True)
    
    return mapping_generator


@lru_cache(maxsize=1)
def get_shared_relationship_resolver() -> RelationshipResolver:
    """
    Get a shared relationship resolver instance.
    
    This function caches and reuses the resolver across all tests
    for improved performance.
    
    Returns:
        A cached RelationshipResolver instance.
    """
    mapping_gen = get_shared_mapping_generator()
    resolver = RelationshipResolver(mapping_gen)
    
    return resolver


def get_test_cache_manager(subdir: str = None) -> CacheManager:
    """
    Get a cache manager for test purposes.
    
    Args:
        subdir: Optional subdirectory within the test cache directory.
        
    Returns:
        A CacheManager instance for testing.
    """
    if subdir:
        cache_path = os.path.join(CACHE_DIR, subdir)
    else:
        cache_path = CACHE_DIR
    
    os.makedirs(cache_path, exist_ok=True)
    return get_cache_manager(cache_path)


# Test data helpers
def create_simple_mmcif() -> str:
    """
    Create a simple mmCIF test structure.
    
    Returns:
        A string containing simple mmCIF data.
    """
    return """data_TEST
#
_entry.id TEST
#
_entity.id 1
_entity.type polymer
#
_atom_site.group_PDB ATOM
_atom_site.id 1
_atom_site.type_symbol C
_atom_site.label_entity_id 1
_atom_site.Cartn_x 10.0
_atom_site.Cartn_y 20.0
_atom_site.Cartn_z 30.0
#"""


def create_complex_mmcif_with_relationships() -> str:
    """
    Create a complex mmCIF structure with multiple entities and relationships.
    
    Returns:
        A string containing complex mmCIF data with relationships.
    """
    return """data_COMPLEX
#
_entry.id COMPLEX
#
loop_
_entity.id
_entity.type
_entity.pdbx_description
1 polymer 'Protein chain A'
2 water 'Water molecules'
#
loop_
_entity_poly.entity_id
_entity_poly.type
_entity_poly.nstd_chirality
1 'polypeptide(L)' no
#
loop_
_entity_poly_seq.entity_id
_entity_poly_seq.num
_entity_poly_seq.mon_id
1 1 VAL
1 2 ALA
#
loop_
_struct_asym.id
_struct_asym.entity_id
A 1
B 2
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
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
_atom_site.occupancy
_atom_site.B_iso_or_equiv
ATOM 1 C CA VAL A 1 1 10.0 11.0 12.0 1.00 20.0
ATOM 2 N N ALA A 1 2 13.0 14.0 15.0 1.00 25.0
ATOM 3 O O HOH B 2 . 16.0 17.0 18.0 1.00 30.0
ATOM 4 O O HOH B 2 . 19.0 20.0 21.0 1.00 35.0
#"""


if __name__ == '__main__':
    # Test that utilities work correctly
    print("Testing shared utilities...")
    
    exporter = get_shared_exporter()
    print(f"✓ Created shared exporter: {exporter}")
    
    mapping_gen = get_shared_mapping_generator()
    print(f"✓ Created shared mapping generator: {mapping_gen}")
    
    resolver = get_shared_relationship_resolver()
    print(f"✓ Created shared resolver: {resolver}")
    
    print("\n✓ All utilities work correctly!")

