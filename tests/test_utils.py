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
    PDBMLConverter, MappingGenerator, DictionaryParser, 
    XSDParser, CacheManager, get_cache_manager
)

# Global converter cache
_GLOBAL_CONVERTERS: Dict[str, PDBMLConverter] = {}

# Global paths for schemas
DICT_PATH = Path(__file__).parent.parent / "sloth" / "mmcif" / "schemas" / "mmcif_pdbx_v50.dic"
XSD_PATH = Path(__file__).parent.parent / "sloth" / "mmcif" / "schemas" / "pdbx-v50.xsd"

# Global cache directory
CACHE_DIR = os.path.join(tempfile.gettempdir(), "sloth_test_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Global caching instance
GLOBAL_CACHE = get_cache_manager(CACHE_DIR)


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


# Shared schema validator instances for improved performance
_GLOBAL_VALIDATORS: Dict[str, Any] = {}


@lru_cache(maxsize=4)
def get_shared_schema_validator(format_type: str):
    """
    Get a shared schema validator instance for the specified format.
    
    This function caches and reuses validator instances across all tests,
    which improves test performance by avoiding repeated schema loading.
    Uses the official PDBML schemas instead of custom mmCIF schemas.
    
    Args:
        format_type: The format type ('XML', 'JSON').
        
    Returns:
        A cached schema validator instance.
    """
    from sloth.mmcif.validator import XMLSchemaValidator, JSONSchemaValidator
    
    cache_key = f"validator_{format_type}"
    
    if cache_key in _GLOBAL_VALIDATORS:
        return _GLOBAL_VALIDATORS[cache_key]
    
    # Create validator directly using official schemas
    if format_type == 'XML':
        # Use the official PDBML XSD schema
        validator = XMLSchemaValidator(str(XSD_PATH))
    elif format_type == 'JSON':
        # Create a basic JSON schema for mmCIF data
        basic_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "patternProperties": {
                "^data_[A-Z0-9_]+$": {
                    "type": "object",
                    "patternProperties": {
                        "^_[a-zA-Z0-9_]+$": {
                            "anyOf": [
                                {"type": "object"},
                                {"type": "array"},
                                {"type": "string"},
                                {"type": "number"}
                            ]
                        }
                    },
                    "additionalProperties": False
                }
            },
            "additionalProperties": False,
            "minProperties": 1
        }
        validator = JSONSchemaValidator(basic_schema)
    elif format_type == 'XML_SIMPLE':
        # Create a simple XML validator for testing purposes
        # This bypasses the complex PDBML schema for basic functionality testing
        class SimpleXMLValidator:
            def __init__(self):
                pass
            
            def validate(self, xml_string):
                """Simple validation - just check if it's well-formed XML."""
                try:
                    import xml.etree.ElementTree as ET
                    ET.fromstring(xml_string)
                    
                    # Basic structure checks
                    if 'datablockName' not in xml_string:
                        raise ValueError("Missing required datablockName attribute")
                    if 'xmlns="http://pdbml.pdb.org/schema/pdbx-v50.xsd"' not in xml_string:
                        raise ValueError("Missing or incorrect namespace")
                    if xml_string.strip().endswith('></datablock>'):
                        raise ValueError("Empty datablock")
                        
                    return {"valid": True, "errors": []}
                except Exception as e:
                    from sloth.mmcif.validator import ValidationError
                    raise ValidationError(str(e))
            
            def is_valid(self, xml_string):
                """Check if XML is valid without raising exceptions."""
                try:
                    self.validate(xml_string)
                    return True
                except:
                    return False
        
        validator = SimpleXMLValidator()
    else:
        raise ValueError(f"Unsupported format type: {format_type}. Supported formats: XML, JSON, XML_SIMPLE")
    
    # Cache validator for future use
    _GLOBAL_VALIDATORS[cache_key] = validator
    
    return validator


def get_schema_paths():
    """
    Get the paths to the official PDBML schema files.
    
    Returns:
        A dictionary with paths to dictionary and XSD schema files.
    """
    return {
        'dict_path': DICT_PATH,
        'xsd_path': XSD_PATH
    }


def cleanup_test_cache():
    """Clean up test cache to free memory if needed."""
    global _GLOBAL_CONVERTERS, _GLOBAL_VALIDATORS
    _GLOBAL_CONVERTERS.clear()
    _GLOBAL_VALIDATORS.clear()
    if hasattr(GLOBAL_CACHE, 'clear_global_caches'):
        GLOBAL_CACHE.clear_global_caches()
