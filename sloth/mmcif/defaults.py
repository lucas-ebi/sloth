"""
PDBML Enums - Enum classes for legitimate constants in pdbml_converter.py

This module provides Enum classes for genuinely constant values that don't
depend on schema or dictionary content. All schema-driven logic should use
the XMLMappingGenerator instead of hardcoded enums.
"""

from enum import Enum
from typing import Set


class StructureFormat(Enum):
    """Enum for data structure format options"""
    NESTED = "nested"
    FLAT = "flat"


class ExportFormat(Enum):
    """Enum for export/import format options"""
    JSON = "json"
    XML = "xml"


class XMLLocation(Enum):
    """Enum for XML element location types"""
    ATTRIBUTE = "attribute"
    ELEMENT_CONTENT = "element_content"
    ELEMENT = "element"


class DataValue(Enum):
    """Enum for data value representations in mmCIF"""
    # Null values
    QUESTION_MARK = "?"
    DOT = "."
    EMPTY_STRING = ""
    QUOTED_EMPTY = "''"
    DOUBLE_QUOTED_EMPTY = '""'
    
    # Default values for missing data
    DEFAULT_INTEGER = '1'
    DEFAULT_DECIMAL = '0.0'
    DEFAULT_ID = 'id'

    @classmethod
    def is_null(cls, value: str) -> bool:
        """Check if a value represents null"""
        null_values = {cls.QUESTION_MARK.value, cls.DOT.value, cls.EMPTY_STRING.value, 
                      cls.QUOTED_EMPTY.value, cls.DOUBLE_QUOTED_EMPTY.value}
        return value in null_values


class DataType(Enum):
    """Unified enum for all data types across mmCIF, XSD, and PDBML"""
    # Text types
    CHAR = "char"
    TEXT = "text"
    CODE = "code"
    STRING = "xs:string"

    # Numeric types
    INT = "int"
    FLOAT = "float"
    REAL = "real"
    NUMBER = "number"
    NUMB = "numb"
    POSITIVE_INT = "positive_int"
    INTEGER = "xsd:integer"
    XSD_INT = "xsd:int"
    DECIMAL = "xsd:decimal"
    DOUBLE = "xsd:double"
    XSD_FLOAT = "xsd:float"

    # Date/time types
    DATE = "date"
    DATETIME = "datetime"
    XSD_DATE = "xsd:date"
    XSD_DATETIME = "xsd:dateTime"

    # Special types
    BOOLEAN = "boolean"
    XSD_BOOLEAN = "xsd:boolean"
    BINARY = "binary"

    @classmethod
    def get_numeric_types(cls) -> Set[str]:
        """Get all numeric type names as a set for matching."""
        numeric = {cls.INT.value, cls.FLOAT.value, cls.REAL.value, cls.NUMBER.value, 
                  cls.NUMB.value, cls.POSITIVE_INT.value, cls.INTEGER.value, 
                  cls.XSD_INT.value, cls.DECIMAL.value, cls.DOUBLE.value, cls.XSD_FLOAT.value}
        return numeric

    @classmethod
    def is_numeric_type(cls, type_name: str) -> bool:
        """Check if a type name represents a numeric data type."""
        return type_name.lower() in cls.get_numeric_types()

    @classmethod
    def is_text_type(cls, type_name: str) -> bool:
        """Check if a type name represents a text data type."""
        text_types = {cls.CHAR.value, cls.TEXT.value, cls.CODE.value, cls.STRING.value}
        return type_name.lower() in text_types


class XMLConstant(Enum):
    """Unified enum for XML and PDBML constants"""
    # Namespaces
    PDBX_V50 = "http://pdbml.pdb.org/schema/pdbx-v50.xsd"
    PDBX_V40 = "http://pdbml.pdb.org/schema/pdbx-v40.xsd"
    XSI_URI = "http://www.w3.org/2001/XMLSchema-instance"
    XS_URI = 'http://www.w3.org/2001/XMLSchema'
    
    # XML namespace prefixes and attributes
    XMLNS = 'xmlns'
    XS_PREFIX = 'xs'
    SCHEMA_LOCATION = 'schemaLocation'
    
    # Elements
    DATABLOCK = "datablock"
    ENTRY = "entry"
    COMPLEX_TYPE = 'xs:complexType'
    ELEMENT = 'xs:element'
    ATTRIBUTE = 'xs:attribute'
    SEQUENCE = 'xs:sequence'
    RESTRICTION = 'xs:restriction'
    
    # Attributes
    DATABLOCK_NAME = "datablockName"
    ID = "id"
    NAME = 'name'
    TYPE = 'type'
    BASE = 'base'
    
    # Schema files
    PDBX_V50_XSD = 'pdbx-v50.xsd'
    
    # XML declaration
    XML_VERSION = '<?xml version="1.0" encoding="UTF-8"?>\n'
    ENCODING = 'utf-8'

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
        numeric_types = DataType.get_numeric_types()

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
    return DataValue.is_null(value)

# ====================== Enums for Constants and Magic Strings ======================

class CacheType(Enum):
    """Cache type identifiers"""
    DICTIONARY = 'dictionary'
    XSD = 'xsd'
    MAPPING_RULES = 'mapping_rules'
    XSD_TREES = 'xsd_trees'

class DictDataType(Enum):
    """Dictionary data structure keys"""
    CATEGORIES = 'categories'
    ITEMS = 'items'
    RELATIONSHIPS = 'relationships'
    ENUMERATIONS = 'enumerations'
    ITEM_TYPES = 'item_types'
    PRIMARY_KEYS = 'primary_keys'

class SchemaDataType(Enum):
    """XSD schema data structure keys"""
    ELEMENTS = 'elements'
    ATTRIBUTES = 'attributes'
    REQUIRED_ELEMENTS = 'required_elements'
    DEFAULT_VALUES = 'default_values'
    COMPLEX_TYPES = 'complex_types'

class LoopDataKey(Enum):
    """Loop data structure keys"""
    LOOP_DATA = '_loop_data'
    NEXT_INDEX = '_next_index'
    HEADERS = 'headers'
    ITEMS = 'items'

class FrameMarker(Enum):
    """Save frame markers and delimiters"""
    SAVE_END = 'save_'
    LOOP_START = 'loop_'
    MULTILINE_DELIMITER = ';'
    UNDERSCORE = '_'
    HASH = '#'

class DictItemKey(Enum):
    """Dictionary item field keys"""
    CATEGORY_ID = 'category.id'
    ITEM_NAME = 'item.name'
    ITEM_DESCRIPTION = 'item_description.description'
    ITEM_TYPE_CODE = 'item_type.code'
    ITEM_ENUMERATION_VALUE = 'item_enumeration.value'
    CATEGORY_KEY_NAME = 'category_key.name'
    CATEGORY_DESCRIPTION = 'category.description'

class RelationshipKey(Enum):
    """Relationship field keys"""
    ITEM_LINKED_CHILD_NAME = 'item_linked.child_name'
    ITEM_LINKED_PARENT_NAME = 'item_linked.parent_name'
    CHILD_CATEGORY = 'child_category'
    CHILD_NAME = 'child_name'
    PARENT_CATEGORY = 'parent_category'
    PARENT_NAME = 'parent_name'
    PDBX_CHILD_CATEGORY_ID = 'pdbx_item_linked_group_list.child_category_id'
    PDBX_CHILD_NAME = 'pdbx_item_linked_group_list.child_name'
    PDBX_PARENT_NAME = 'pdbx_item_linked_group_list.parent_name'
    PDBX_PARENT_CATEGORY_ID = 'pdbx_item_linked_group_list.parent_category_id'

class TabularDataCategory(Enum):
    """Tabular data category names"""
    ITEM_TYPE_LIST = 'item_type_list'
    PDBX_ITEM_LINKED_GROUP_LIST = 'pdbx_item_linked_group_list'

class TabularDataField(Enum):
    """Tabular data field names"""
    CODE = 'code'
    CHILD_CATEGORY_ID = 'child_category_id'
    CHILD_NAME = 'child_name'
    PARENT_NAME = 'parent_name'
    PARENT_CATEGORY_ID = 'parent_category_id'

class TypeSuffix(Enum):
    """Type naming suffixes"""
    TYPE = 'Type'
    CATEGORY = 'Category'

class MappingDataKey(Enum):
    """Mapping data structure keys"""
    CATEGORY_MAPPING = 'category_mapping'
    ITEM_MAPPING = 'item_mapping'
    FK_MAP = 'fk_map'
    FIELDS = 'fields'
    ENUM = 'enum'
    DESCRIPTION = 'description'

class SemanticPattern(Enum):
    """Unified enum for semantic pattern matching in field names and relationships"""
    # Attribute field patterns
    ID = 'id'
    NAME = 'name'
    TYPE = 'type'
    VALUE = 'value'
    CODE = 'code'
    ID_SUFFIX = '_id'
    NO_SUFFIX = '_no'
    INDEX_SUFFIX = '_index'
    
    # Lookup/reference table patterns
    CLASS = 'class'
    METHOD = 'method'
    STATUS = 'status'
    SYMBOL = 'symbol'
    ENUM = 'enum'
    DICT = 'dict'
    LIST = 'list'
    TABLE = 'table'
    REF = 'ref'
    
    # Reference field patterns
    TYPE_SYMBOL = 'type_symbol'
    CATEGORY = 'category'
    KIND = 'kind'
    
    # Ownership field patterns
    ASYM_ID = 'asym_id'
    ENTITY_ID = 'entity_id'
    STRUCT_ID = 'struct_id'
    
    # Structural patterns
    SITE_SUFFIX = '_site'
    ASYM_SUFFIX = '_asym'
    ATOM_SUFFIX = '_atom'
    RESIDUE_SUFFIX = '_residue'
    AUTHOR = 'author'
    LABEL = 'label'


class RelationshipTerm(Enum):
    """Unified enum for relationship analysis terms"""
    # Ownership terms
    BELONGS_TO = 'belongs to'
    OWNED_BY = 'owned by'
    PART_OF = 'part of'
    CONTAINED_IN = 'contained in'
    MEMBER_OF = 'member of'
    IDENTIFIER = 'identifier'
    KEY = 'key'
    BELONGS = 'belongs'
    MEMBER = 'member'
    PART = 'part'
    
    # Reference terms
    REFERS_TO = 'refers to'
    REFERENCES = 'references'
    LOOKUP = 'lookup'
    TYPE_OF = 'type of'
    CODE_FOR = 'code for'
    
    # Hierarchy terms
    DETAIL = 'detail'
    SPECIFIC = 'specific'
    GENERAL = 'general'
    SUMMARY = 'summary'

class FileOperation(Enum):
    """File operation constants"""
    # File access modes
    READ = 'r'
    WRITE = 'w'
    READ_BINARY = 'rb'
    WRITE_BINARY = 'wb'
    
    # File extensions
    PICKLE_EXT = '.pkl'
    
    # Quote characters for parsing
    DOUBLE_QUOTE = '"'
    SINGLE_QUOTE = "'"