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


class NumericField(Enum):
    """Enum for fields that should be treated as numeric values (based on known mmCIF patterns)"""
    YEAR = "year"
    JOURNAL_VOLUME = "journal_volume"
    PAGE_FIRST = "page_first"
    PAGE_LAST = "page_last"
    ORDINAL = "ordinal"


# Helper functions for backwards compatibility
def get_numeric_fields() -> Set[str]:
    """Get set of numeric field names"""
    return {field.value for field in NumericField}


def is_null_value(value: str) -> bool:
    """Check if value represents null"""
    return NullValue.is_null(value)


# REMOVED FUNCTIONS - These should use schema-driven logic instead:
# - get_essential_keys() -> Use XMLMappingGenerator.get_mapping_rules()
# - get_element_only_items() -> Use XMLMappingGenerator.get_mapping_rules()
# - get_required_attributes() -> Use XMLMappingGenerator.get_mapping_rules()
# - get_atom_site_defaults() -> Use XMLMappingGenerator.get_mapping_rules()
# - get_anisotropic_defaults() -> Use XMLMappingGenerator.get_mapping_rules()
# - get_problematic_field_replacement() -> Use proper validation logic
