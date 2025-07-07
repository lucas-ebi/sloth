"""
PDBML Enums - Enum classes to replace hardcoded dictionaries in pdbml_converter.py

This module provides Enum classes for commonly used constants and mappings
to improve code clarity, maintainability, and reusability.
"""

from enum import Enum
from typing import Dict, List, Set


class XMLLocation(Enum):
    """Enum for XML element location types"""
    ATTRIBUTE = "attribute"
    ELEMENT_CONTENT = "element_content"
    ELEMENT = "element"


class NumericField(Enum):
    """Enum for fields that should be treated as numeric values"""
    YEAR = "year"
    JOURNAL_VOLUME = "journal_volume"
    PAGE_FIRST = "page_first"
    PAGE_LAST = "page_last"
    ORDINAL = "ordinal"


class RequiredAttribute(Enum):
    """Enum for categories and their required attributes"""
    EXPTL = "exptl"
    PDBX_DATABASE_STATUS = "pdbx_database_status"
    DATABASE_2 = "database_2"
    
    @classmethod
    def get_required_attrs(cls, category: str) -> List[str]:
        """Get required attributes for a category"""
        mapping = {
            cls.EXPTL.value: ["method", "entry_id"],
            cls.PDBX_DATABASE_STATUS.value: ["entry_id"],
            cls.DATABASE_2.value: ["database_id"]
        }
        return mapping.get(category, [])


class EssentialKey(Enum):
    """Enum for essential keys used as fallbacks"""
    ENTRY = "_entry"
    CITATION = "_citation"
    ATOM_SITE = "_atom_site"
    ENTITY = "_entity"
    ATOM_TYPE = "_atom_type"
    CHEM_COMP = "_chem_comp"
    STRUCT = "_struct"
    STRUCT_ASYM = "_struct_asym"
    
    @classmethod
    def get_keys(cls, category: str) -> List[str]:
        """Get essential keys for a category"""
        mapping = {
            cls.ENTRY.value: ["id"],
            cls.CITATION.value: ["id"],
            cls.ATOM_SITE.value: ["id"],
            cls.ENTITY.value: ["id"],
            cls.ATOM_TYPE.value: ["symbol"],
            cls.CHEM_COMP.value: ["id"],
            cls.STRUCT.value: ["entry_id"],
            cls.STRUCT_ASYM.value: ["id"]
        }
        return mapping.get(category, [])


class ElementOnlyItem(Enum):
    """Enum for items that must be elements (not attributes)"""
    ATOM_SITE = "atom_site"
    PDBX_DATABASE_STATUS = "pdbx_database_status"
    
    @classmethod
    def get_element_only_items(cls, category: str) -> List[str]:
        """Get items that must be elements for a category"""
        mapping = {
            cls.ATOM_SITE.value: [
                'type_symbol', 'label_atom_id', 'label_comp_id', 'comp_id',
                'B_equiv_geom_mean', 'B_iso_or_equiv', 'Cartn_x', 'Cartn_y', 'Cartn_z',
                'calc_flag', 'footnote_id', 'adp_type', 'label_asym_id', 'label_entity_id',
                'label_seq_id', 'occupancy', 'U_iso_or_equiv'
            ],
            cls.PDBX_DATABASE_STATUS.value: ['entry_id', 'deposit_site', 'process_site']
        }
        return mapping.get(category, [])


class DefaultValue(Enum):
    """Enum for common default values"""
    ZERO_FLOAT = "0.0"
    ZERO_INT = "0"
    ONE_FLOAT = "1.0"
    ONE_INT = "1"
    EMPTY_STRING = ""
    DOT = "."
    CALC = "calc"
    BISO = "Biso"
    PDB = "PDB"
    MET = "MET"
    NITROGEN = "N"
    CHAIN_A = "A"
    NITROGEN_ATOM = "N1"
    WYCKOFF_A = "a"


class AtomSiteDefault(Enum):
    """Enum for atom_site category default values"""
    ADP_TYPE = ("adp_type", DefaultValue.BISO.value)
    B_ISO_OR_EQUIV = ("B_iso_or_equiv", DefaultValue.ZERO_FLOAT.value)
    B_ISO_OR_EQUIV_ESD = ("B_iso_or_equiv_esd", DefaultValue.ZERO_FLOAT.value)
    CARTN_X_ESD = ("Cartn_x_esd", DefaultValue.ZERO_FLOAT.value)
    CARTN_Y_ESD = ("Cartn_y_esd", DefaultValue.ZERO_FLOAT.value)
    CARTN_Z_ESD = ("Cartn_z_esd", DefaultValue.ZERO_FLOAT.value)
    U_ISO_OR_EQUIV = ("U_iso_or_equiv", DefaultValue.ZERO_FLOAT.value)
    U_ISO_OR_EQUIV_ESD = ("U_iso_or_equiv_esd", DefaultValue.ZERO_FLOAT.value)
    B_EQUIV_GEOM_MEAN = ("B_equiv_geom_mean", DefaultValue.ZERO_FLOAT.value)
    B_EQUIV_GEOM_MEAN_ESD = ("B_equiv_geom_mean_esd", DefaultValue.ZERO_FLOAT.value)
    U_EQUIV_GEOM_MEAN = ("U_equiv_geom_mean", DefaultValue.ZERO_FLOAT.value)
    U_EQUIV_GEOM_MEAN_ESD = ("U_equiv_geom_mean_esd", DefaultValue.ZERO_FLOAT.value)
    WYCKOFF_SYMBOL = ("Wyckoff_symbol", DefaultValue.WYCKOFF_A.value)
    LABEL_ENTITY_ID = ("label_entity_id", DefaultValue.ONE_INT.value)
    LABEL_SEQ_ID = ("label_seq_id", DefaultValue.ONE_INT.value)
    OCCUPANCY = ("occupancy", DefaultValue.ONE_FLOAT.value)
    OCCUPANCY_ESD = ("occupancy_esd", DefaultValue.ZERO_FLOAT.value)
    PDBX_PDB_ATOM_NAME = ("pdbx_PDB_atom_name", DefaultValue.NITROGEN.value)
    PDBX_PDB_INS_CODE = ("pdbx_PDB_ins_code", DefaultValue.DOT.value)
    PDBX_PDB_MODEL_NUM = ("pdbx_PDB_model_num", DefaultValue.ONE_INT.value)
    PDBX_PDB_RESIDUE_NAME = ("pdbx_PDB_residue_name", DefaultValue.MET.value)
    PDBX_PDB_RESIDUE_NO = ("pdbx_PDB_residue_no", DefaultValue.ONE_INT.value)
    PDBX_PDB_STRAND_ID = ("pdbx_PDB_strand_id", DefaultValue.CHAIN_A.value)
    CALC_FLAG = ("calc_flag", DefaultValue.CALC.value)
    FOOTNOTE_ID = ("footnote_id", DefaultValue.ONE_INT.value)
    
    @classmethod
    def get_defaults_dict(cls) -> Dict[str, str]:
        """Get all atom_site defaults as a dictionary"""
        return {item.value[0]: item.value[1] for item in cls}


class AnisotropicParam(Enum):
    """Enum for anisotropic thermal parameters"""
    ANISO_B11 = ("aniso_B11", DefaultValue.ZERO_FLOAT.value)
    ANISO_B11_ESD = ("aniso_B11_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_B12 = ("aniso_B12", DefaultValue.ZERO_FLOAT.value)
    ANISO_B12_ESD = ("aniso_B12_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_B13 = ("aniso_B13", DefaultValue.ZERO_FLOAT.value)
    ANISO_B13_ESD = ("aniso_B13_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_B22 = ("aniso_B22", DefaultValue.ZERO_FLOAT.value)
    ANISO_B22_ESD = ("aniso_B22_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_B23 = ("aniso_B23", DefaultValue.ZERO_FLOAT.value)
    ANISO_B23_ESD = ("aniso_B23_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_B33 = ("aniso_B33", DefaultValue.ZERO_FLOAT.value)
    ANISO_B33_ESD = ("aniso_B33_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_U11 = ("aniso_U11", DefaultValue.ZERO_FLOAT.value)
    ANISO_U11_ESD = ("aniso_U11_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_U12 = ("aniso_U12", DefaultValue.ZERO_FLOAT.value)
    ANISO_U12_ESD = ("aniso_U12_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_U13 = ("aniso_U13", DefaultValue.ZERO_FLOAT.value)
    ANISO_U13_ESD = ("aniso_U13_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_U22 = ("aniso_U22", DefaultValue.ZERO_FLOAT.value)
    ANISO_U22_ESD = ("aniso_U22_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_U23 = ("aniso_U23", DefaultValue.ZERO_FLOAT.value)
    ANISO_U23_ESD = ("aniso_U23_esd", DefaultValue.ZERO_FLOAT.value)
    ANISO_U33 = ("aniso_U33", DefaultValue.ZERO_FLOAT.value)
    ANISO_U33_ESD = ("aniso_U33_esd", DefaultValue.ZERO_FLOAT.value)
    
    @classmethod
    def get_b_factor_params(cls) -> List[str]:
        """Get list of B-factor parameter names"""
        return [item.value[0] for item in cls if "aniso_B" in item.value[0]]
    
    @classmethod
    def get_defaults_dict(cls) -> Dict[str, str]:
        """Get all anisotropic parameters as a dictionary"""
        return {item.value[0]: item.value[1] for item in cls}


class ProblematicField(Enum):
    """Enum for fields with special handling requirements"""
    STATUS_CODE_SF = ("status_code_sf", "?", None)  # Skip if value is ?
    STATUS_CODE_MR = ("status_code_mr", "?", None)  # Skip if value is ?
    LABEL_SEQ_ID = ("label_seq_id", "?", "1")      # Use 1 as default
    FOOTNOTE_ID = ("footnote_id", ".", "1")        # Use 1 as default
    PDBX_FORMAL_CHARGE = ("pdbx_formal_charge", ["?", ".", "", "''", '""'], "0")
    CALC_FLAG = ("calc_flag", ["?", ".", "", "''", '""'], "calc")
    
    @classmethod
    def get_replacement(cls, field_name: str, value: str) -> str:
        """Get replacement value for problematic field"""
        for item in cls:
            if item.value[0] == field_name:
                bad_values = item.value[1]
                replacement = item.value[2]
                
                if isinstance(bad_values, list):
                    if value in bad_values:
                        return replacement if replacement is not None else ""
                else:
                    if value == bad_values:
                        return replacement if replacement is not None else ""
        return value


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


class SpecialAttribute(Enum):
    """Enum for special attributes that need specific handling"""
    DATABASE_ID_PDB = ("database_2", "database_id", "PDB")
    EXPTL_METHOD = ("exptl", "method", None)  # Method must be attribute, value from data
    
    @classmethod
    def get_special_attrs(cls, category: str) -> Dict[str, str]:
        """Get special attributes for a category"""
        attrs = {}
        for item in cls:
            if item.value[0] == category and item.value[2] is not None:
                attrs[item.value[1]] = item.value[2]
        return attrs


class ValidationRule(Enum):
    """Enum for validation rules"""
    REQUIRED_ANISO_PARAMS = "required_aniso_params"
    REQUIRED_ELEMENTS = "required_elements"
    REQUIRED_ATTRIBUTES = "required_attributes"
    
    @classmethod
    def get_atom_site_required_elements(cls) -> Dict[str, str]:
        """Get minimal required elements for atom_site category"""
        return {
            "adp_type": DefaultValue.BISO.value,
            "B_iso_or_equiv": DefaultValue.ZERO_FLOAT.value,
            "label_entity_id": DefaultValue.ONE_INT.value,
            "label_seq_id": DefaultValue.ONE_INT.value,
            "occupancy": DefaultValue.ONE_FLOAT.value,
            "occupancy_esd": DefaultValue.ZERO_FLOAT.value,
            "pdbx_PDB_atom_name": DefaultValue.NITROGEN.value,
            "pdbx_PDB_ins_code": DefaultValue.DOT.value,
            "pdbx_PDB_model_num": DefaultValue.ONE_INT.value,
            "pdbx_PDB_residue_name": DefaultValue.MET.value,
            "pdbx_PDB_residue_no": DefaultValue.ONE_INT.value,
            "pdbx_PDB_strand_id": DefaultValue.CHAIN_A.value
        }


# Helper functions for backwards compatibility
def get_numeric_fields() -> Set[str]:
    """Get set of numeric field names"""
    return {field.value for field in NumericField}


def get_essential_keys(category: str) -> List[str]:
    """Get essential keys for a category"""
    return EssentialKey.get_keys(category)


def get_element_only_items(category: str) -> List[str]:
    """Get element-only items for a category"""
    return ElementOnlyItem.get_element_only_items(category)


def get_required_attributes(category: str) -> List[str]:
    """Get required attributes for a category"""
    return RequiredAttribute.get_required_attrs(category)


def get_atom_site_defaults() -> Dict[str, str]:
    """Get atom_site default values"""
    return AtomSiteDefault.get_defaults_dict()


def get_anisotropic_defaults() -> Dict[str, str]:
    """Get anisotropic parameter default values"""
    return AnisotropicParam.get_defaults_dict()


def get_problematic_field_replacement(field_name: str, value: str) -> str:
    """Get replacement value for problematic field"""
    return ProblematicField.get_replacement(field_name, value)


def is_null_value(value: str) -> bool:
    """Check if value represents null"""
    return NullValue.is_null(value)
