"""
PDBML Enums - Enum classes for legitimate constants in pdbml_converter.py

This module provides Enum classes for genuinely constant values that don't
depend on schema or dictionary content. All schema-driven logic should use
the XMLMappingGenerator instead of hardcoded enums.
"""

from enum import Enum
from typing import Set


class XMLLocation(Enum):
    """Enum for XML element location types"""
    ATTRIBUTE = "attribute"
    ELEMENT_CONTENT = "element_content"
    ELEMENT = "element"


class NullValue(Enum):
    """Enum for null value representations in mmCIF"""
    QUESTION_MARK = "?"
    DOT = "."
    EMPTY_STRING = ""
    QUOTED_EMPTY = "''"
    DOUBLE_QUOTED_EMPTY = '""'

    @classmethod
    def is_null(cls, value: str) -> bool:
        """Check if a value represents null"""
        return value in [item.value for item in cls]


class NumericDataType(Enum):
    """Enum for mmCIF dictionary numeric data types.

    These are the standard type names used in mmCIF dictionaries
    to identify numeric fields.
    """
    INT = "int"
    FLOAT = "float"
    REAL = "real"
    NUMBER = "number"
    NUMB = "numb"
    POSITIVE_INT = "positive_int"

    @classmethod
    def get_type_names(cls) -> Set[str]:
        """Get all numeric type names as a set for matching."""
        return {item.value for item in cls}


class MMCIFDataType(Enum):
    """Enum for standard mmCIF data types as defined in dictionaries.

    These are the canonical data type names used in mmCIF dictionary
    definitions for type checking and validation.
    """
    # Text types
    CHAR = "char"
    TEXT = "text"
    CODE = "code"

    # Numeric types (reference to NumericDataType for actual values)
    INT = "int"
    FLOAT = "float"
    REAL = "real"

    # Date/time types
    DATE = "date"
    DATETIME = "datetime"

    # Special types
    BOOLEAN = "boolean"
    BINARY = "binary"

    @classmethod
    def is_numeric_type(cls, type_name: str) -> bool:
        """Check if a type name represents a numeric data type."""
        return type_name.lower() in NumericDataType.get_type_names()

    @classmethod
    def is_text_type(cls, type_name: str) -> bool:
        """Check if a type name represents a text data type."""
        text_types = {cls.CHAR.value, cls.TEXT.value, cls.CODE.value}
        return type_name.lower() in text_types


class PDBMLNamespace(Enum):
    """Enum for PDBML XML namespace constants.

    These are the official namespace URIs used in PDBML/PDBX XML files.
    """
    PDBX_V50 = "http://pdbml.pdb.org/schema/pdbx-v50.xsd"
    PDBX_V40 = "http://pdbml.pdb.org/schema/pdbx-v40.xsd"
    XSI = "http://www.w3.org/2001/XMLSchema-instance"

    @classmethod
    def get_default_namespace(cls) -> str:
        """Get the default namespace (latest version)."""
        return cls.PDBX_V50.value

    @classmethod
    def get_schema_location(cls, namespace: str = None) -> str:
        """Get schema location string for XML."""
        ns = namespace or cls.get_default_namespace()
        schema_file = ns.split('/')[-1]  # Extract schema filename
        return f"{ns} {schema_file}"


class XMLElementType(Enum):
    """Enum for XML element structural types"""
    ROOT_CHILD_ELEMENT = "root_child_element"
    SIMPLE_ELEMENT = "simple_element"
    COMPOSITE_ELEMENT = "composite_element"


class XMLGroupingType(Enum):
    """Enum for XML element grouping strategies"""
    BY_SINGLE_KEY = "by_single_key"
    BY_COMPOSITE_KEY = "by_composite_key"


class XMLContainerType(Enum):
    """Enum for XML container types"""
    ENTRY = "entry"
    CATEGORY = "category"


class PDBMLElement(Enum):
    """Enum for standard PDBML XML element names"""
    DATABLOCK = "datablock"
    ENTRY = "entry"


class PDBMLAttribute(Enum):
    """Enum for standard PDBML XML attribute names"""
    DATABLOCK_NAME = "datablockName"
    ID = "id"
    SCHEMA_LOCATION = "schemaLocation"


class DebugFile(Enum):
    """Enum for debug file names used in development/troubleshooting"""
    RAW_XML = "debug_raw_xml.xml"
    PROBLEM_XML = "debug_problem_xml.xml"
    DATABASE_XML = "debug_database.xml"


def get_numeric_fields(mapping_generator=None) -> Set[str]:
    """Get set of numeric field names using schema-driven detection.

    Args:
        mapping_generator: Optional XMLMappingGenerator instance for
                          dictionary-based detection

    Returns:
        Set of field names that should be treated as numeric

    This function provides intelligent numeric field detection:
    1. If mapping_generator is provided, uses dictionary type information
    2. Otherwise returns empty set - relies on schema-driven approach
    """
    # Schema-driven approach: Use dictionary type information if available
    if (mapping_generator and hasattr(mapping_generator, '_items') and
            mapping_generator._items):
        numeric_fields = set()
        numeric_types = NumericDataType.get_type_names()

        for item_name, item_info in mapping_generator._items.items():
            item_type = item_info.get('type', '').lower()
            if any(num_type in item_type for num_type in numeric_types):
                # Extract just the field name (after the dot)
                if '.' in item_name:
                    field_name = item_name.split('.')[-1]
                    numeric_fields.add(field_name)

        return numeric_fields

    # No fallback - encourage proper schema-driven usage
    # If no mapping generator is provided, we can't reliably detect
    # numeric fields. This forces callers to provide dictionary information
    # for accurate detection
    return set()


def is_null_value(value: str) -> bool:
    """Check if value represents null"""
    return NullValue.is_null(value)


# REMOVED CLASSES/FUNCTIONS - These now use schema-driven logic instead:
# - StandardRelationship class -> Use DictionaryParser relationship extraction
# - Hardcoded numeric field lists -> Use dictionary type information only
# - get_essential_keys() -> Use XMLMappingGenerator.get_mapping_rules()
# - get_element_only_items() -> Use XMLMappingGenerator.get_mapping_rules()
# - get_required_attributes() -> Use XMLMappingGenerator.get_mapping_rules()
# - get_atom_site_defaults() -> Use XMLMappingGenerator.get_mapping_rules()
# - get_anisotropic_defaults() -> Use XMLMappingGenerator.get_mapping_rules()
# - get_problematic_field_replacement() -> Use proper validation logic
#
# ADDED PROPER ENUMS - Legitimate constants now have proper Enum classes:
# + NumericDataType enum -> Standard mmCIF dictionary numeric type names
# + MMCIFDataType enum -> Complete mmCIF data type classification system
# + PDBMLNamespace enum -> Official XML namespace URIs and schema locations
#
# EVOLUTION SUMMARY:
# Phase 1: Removed hardcoded enums, replaced with pattern-based detection
# Phase 2: Enhanced with schema-driven detection using dictionary type info
# Phase 3: Eliminated hardcoded fallbacks - pure schema-driven approach only
# Phase 4: Created proper Enum classes for all legitimate constant values
