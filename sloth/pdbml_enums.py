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


class StandardRelationship(Enum):
    """Standard mmCIF relationship patterns based on official specifications.
    
    These are fallback patterns when dictionary relationships are not available.
    Format: (child_category, parent_category, link_field)
    """
    # Core structural relationships
    ENTITY_POLY = ("entity_poly", "entity", "entity_id")
    ENTITY_POLY_SEQ = ("entity_poly_seq", "entity_poly", "entity_id") 
    STRUCT_ASYM = ("struct_asym", "entity", "entity_id")
    ATOM_SITE = ("atom_site", "struct_asym", "label_asym_id")
    
    # Additional atom_site relationships (multi-parent)
    ATOM_SITE_ENTITY = ("atom_site", "entity", "label_entity_id")
    ATOM_SITE_SEQ = ("atom_site", "entity_poly_seq", "label_seq_id")
    
    # Citation relationships
    CITATION_AUTHOR = ("citation_author", "citation", "citation_id")
    CITATION_EDITOR = ("citation_editor", "citation", "citation_id")
    
    # Chemical component relationships
    CHEM_COMP_ATOM = ("chem_comp_atom", "chem_comp", "comp_id")
    CHEM_COMP_BOND = ("chem_comp_bond", "chem_comp", "comp_id")
    CHEM_COMP_ANGLE = ("chem_comp_angle", "chem_comp", "comp_id")
    
    # Database relationships
    DATABASE_2 = ("database_2", "entry", "entry_id")
    PDBX_DATABASE_STATUS = ("pdbx_database_status", "entry", "entry_id")
    
    @classmethod
    def get_relationships_dict(cls) -> dict:
        """Get relationships as a dictionary mapping child->list of parent info"""
        relationships = {}
        for rel in cls:
            child, parent, link_field = rel.value
            if child not in relationships:
                relationships[child] = []
            relationships[child].append((parent, link_field))
        return relationships


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
