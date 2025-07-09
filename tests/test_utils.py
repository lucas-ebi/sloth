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

from sloth.serializers import (
    PDBMLConverter, MappingGenerator, DictionaryParser, 
    XSDParser, HybridCache
)

# Global converter cache
_GLOBAL_CONVERTERS: Dict[str, PDBMLConverter] = {}

# Global paths for schemas
DICT_PATH = Path(__file__).parent.parent / "sloth" / "schemas" / "mmcif_pdbx_v50.dic"
XSD_PATH = Path(__file__).parent.parent / "sloth" / "schemas" / "pdbx-v50.xsd"

# Global cache directory
CACHE_DIR = os.path.join(tempfile.gettempdir(), "sloth_test_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Global caching instance
GLOBAL_CACHE = HybridCache(CACHE_DIR)


@lru_cache(maxsize=2)
def get_shared_converter(permissive: bool = False) -> PDBMLConverter:
    """
    Get a shared converter instance with the specified permissive setting.
    
    This function caches and reuses converter instances across all tests,
    which dramatically improves test performance.
    
    Args:
        permissive: Whether the converter should be in permissive mode.
        
    Returns:
        A cached PDBMLConverter instance.
    """
    cache_key = f"converter_{permissive}"
    
    if cache_key in _GLOBAL_CONVERTERS:
        return _GLOBAL_CONVERTERS[cache_key]
        
    # Set up parsers with default paths
    dict_parser = DictionaryParser(GLOBAL_CACHE, quiet=True)
    xsd_parser = XSDParser(GLOBAL_CACHE, quiet=True)
    dict_parser.source = DICT_PATH
    xsd_parser.source = XSD_PATH
    
    # Set up mapping generator
    mapping_generator = MappingGenerator(dict_parser, xsd_parser, GLOBAL_CACHE, quiet=True)
    
    # Create converter
    converter = PDBMLConverter(mapping_generator, permissive=permissive, quiet=True)
    
    # Cache converter for future use
    _GLOBAL_CONVERTERS[cache_key] = converter
    
    return converter
