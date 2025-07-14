#!/usr/bin/env python3
"""
Importer for SLOTH - JSON and XML (PDBML) import capabilities.

This module provides functionality to import nested JSON data and PDBML XML
back into mmCIF format, enabling round-trip conversions.
Supports validation through intermediate PDBML XML when permissive=False.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
from xml.etree import ElementTree as ET
import jsonschema
from .models import MMCIFDataContainer, DataBlock, Category
from .parser import MMCIFParser
from .common import BaseImporter
from .defaults import StructureFormat
from .serializer import (
    PDBMLConverter, 
    RelationshipResolver,
    DictionaryParser,
    XSDParser,
    MappingGenerator,
    get_cache_manager
)
from .validator import XMLSchemaValidator, ValidationError


class XMLImporter(BaseImporter):
    """Import PDBML XML data back to mmCIF format with optional validation."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        """
        Initialize the XML importer.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            xsd_path: Path to PDBML XSD schema file
            cache_dir: Directory for caching
            permissive: If False, validates XML against XSD schema
            quiet: Suppress output messages
        """
        super().__init__(dict_path, xsd_path, cache_dir, permissive, quiet)
        
        # Remove custom XML schema paths - we only use PDBML XSD validation
        # The nested parameter in import_data is now only used for processing hints,
        # not for schema selection
    
    def import_data(
        self, 
        data: Union[str, Dict[str, Any], Path], 
        nested: bool = True,
        permissive: bool = None
    ) -> MMCIFDataContainer:
        """
        Import PDBML XML data back to mmCIF format.
        
        Args:
            data: PDBML XML data as string, dict, or file path
            nested: Processing hint (not used for validation - PDBML XSD handles both)
            permissive: Override schema validation permissiveness (uses self.permissive if None)
            
        Returns:
            MMCIFDataContainer with imported data
            
        Raises:
            ValidationError: If validation fails and permissive=False
        """
        # Parse XML input
        if isinstance(data, (str, Path)):
            # Determine if it's a file path or XML string
            # A file path should be much shorter and not contain XML structure
            if len(str(data)) < 512 and not str(data).strip().startswith('<') and Path(data).exists():
                # It's a file path
                with open(data, 'r') as f:
                    xml_data = f.read()
            else:
                # It's an XML string
                xml_data = data
        else:
            raise ValidationError("Invalid data type for XML import")
        
        # Determine validation mode  
        validate = not (self.permissive if permissive is None else permissive)
        
        # Validate PDBML content if required (PDBML XSD validation handles both flat and nested)
        if validate and self.converter and self.validator:
            self._validate_pdbml_content(xml_data)
        
        # Convert XML to mmCIF container
        container = self._convert_xml_to_mmcif(xml_data)
        
        return container
    
    # Remove _validate_xml_structure method entirely - not needed anymore
    
    # Keep the existing _convert_xml_to_mmcif method as is
    def _convert_xml_to_mmcif(self, xml_data: str) -> MMCIFDataContainer:
        """
        Convert PDBML XML data to mmCIF format.
        
        Args:
            xml_data: PDBML XML data as string
            
        Returns:
            MMCIFDataContainer with converted data
            
        Raises:
            ValidationError: If conversion fails
        """
        try:
            # Parse XML
            root = ET.fromstring(xml_data)
            
            # Create a simple data block
            block_name = root.get('datablockName', 'IMPORTED_XML')
            categories = {}
            
            # Extract categories from XML
            from .models import Category
            
            # Iterate through all child elements that end with "Category"
            for element in root:
                if element.tag.endswith('Category'):
                    # Extract category name (remove "Category" suffix and namespace)
                    category_name = element.tag.split('}')[-1]  # Remove namespace
                    if category_name.endswith('Category'):
                        category_name = category_name[:-8]  # Remove "Category" suffix
                    
                    # Add underscore prefix for mmCIF convention
                    if not category_name.startswith('_'):
                        category_name = f'_{category_name}'
                    
                    # Create category object
                    category = Category(name=category_name, validator_factory=None)
                    
                    # Extract items from category
                    for item_element in element:
                        # Add values for each attribute
                        for attr_name, attr_value in item_element.attrib.items():
                            category._add_item_value(attr_name, attr_value)
                        
                        # Also extract data from child elements (for multi-row categories)
                        for child in item_element:
                            child_name = child.tag.split('}')[-1]  # Remove namespace
                            child_text = child.text or ""
                            category._add_item_value(child_name, child_text)
                    
                    # Commit batches to make data available
                    category._commit_all_batches()
                    categories[category_name] = category
            
            # Create data block with extracted categories
            from .models import DataBlock
            data_block = DataBlock(block_name, categories)
            return MMCIFDataContainer({block_name: data_block})
        except Exception as e:
            raise ValidationError(f"XML to mmCIF conversion error: {str(e)}")

class JSONImporter(BaseImporter):
    """Import JSON data back to mmCIF format using PDBML as intermediate step."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        """
        Initialize the JSON importer.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            xsd_path: Path to PDBML XSD schema file
            cache_dir: Directory for caching
            permissive: If False, validates through PDBML XML against XSD schema
            quiet: Suppress output messages
        """
        super().__init__(dict_path, xsd_path, cache_dir, permissive, quiet)
        
        # Set up JSON-specific components (same as JSONExporter)
        if not self.permissive:
            from .serializer import RelationshipResolver, MappingGenerator, DictionaryParser, XSDParser, get_cache_manager
            
            cache_manager = get_cache_manager(
                self.cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
            )
            
            # Set up metadata parsers
            dict_parser = DictionaryParser(cache_manager, self.quiet)
            xsd_parser = XSDParser(cache_manager, self.quiet)
            dict_parser.source = self.dict_path
            xsd_parser.source = self.xsd_path
            
            # Set up mapping generator and relationship resolver
            mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache_manager, self.quiet)
            self.resolver = RelationshipResolver(mapping_generator)
        else:
            self.resolver = None
    
    def import_data(
        self, 
        data: Union[str, Dict[str, Any], Path], 
        nested: bool = True,
        permissive: bool = None
    ) -> MMCIFDataContainer:
        """
        Import JSON data back to mmCIF format.
        
        Args:
            data: JSON data as string, dict, or file path
            nested: Whether the JSON has nested structure
            permissive: Override schema validation permissiveness
            
        Returns:
            MMCIFDataContainer with imported data
            
        Raises:
            ValidationError: If validation fails and permissive=False
        """
        # Parse JSON input
        json_data = self._parse_json_input(data)
        
        if nested:
            return self._import_nested_json(json_data, permissive)
        else:
            return self._import_flat_json(json_data, permissive)
    
    def _parse_json_input(self, data: Union[str, Dict[str, Any], Path]) -> Dict[str, Any]:
        """Parse JSON input from various formats."""
        if isinstance(data, dict):
            return data
        elif isinstance(data, (str, Path)):
            # Determine if it's a file path or JSON string
            data_str = str(data)
            if len(data_str) < 512 and not data_str.strip().startswith('{') and Path(data).exists():
                # It's a file path
                with open(data, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # It's a JSON string
                return json.loads(data_str)
        else:
            raise ValidationError("Invalid data type for JSON import")
    
    def _import_nested_json(
        self, 
        json_data: Dict[str, Any], 
        permissive: bool = None
    ) -> MMCIFDataContainer:
        """
        Import nested JSON back to mmCIF format using PDBML as intermediate step.
        
        This mirrors the JSONExporter._to_nested_json() process in reverse:
        1. Flatten nested JSON to flat format
        2. Use flat JSON import logic (with validation if needed)
        """
        # Convert nested JSON to flat format first
        flat_structure = self._flatten_nested_json(json_data)
        
        # Reuse flat JSON import logic
        return self._import_flat_json(flat_structure, permissive)
    
    def _import_flat_json(
        self, 
        json_data: Dict[str, Any], 
        permissive: bool = None
    ) -> MMCIFDataContainer:
        """
        Import flat JSON back to mmCIF format.
        
        For flat JSON, we can convert directly to mmCIF or optionally
        validate through PDBML conversion if not permissive.
        """
        validate = not (self.permissive if permissive is None else permissive)
        
        # Convert flat JSON to mmCIF container first
        container = self._convert_flat_json_to_mmcif(json_data)
        
        # If validation requested, convert container to PDBML and validate
        if validate and self.converter and self.validator:
            # Convert mmCIF container to PDBML for validation
            pdbml_xml = self.converter.convert_to_pdbml(container)
            self._validate_pdbml_content(pdbml_xml)
        
        # Return the already-created container (no need to reconvert)
        return container

    def _flatten_nested_json(self, nested_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten nested JSON structure back to flat format.
        
        This reverses the nesting done by RelationshipResolver.
        Note: Only flattens categories that contain other categories,
        not individual item dictionaries within categories.
        """
        flat_data = {}
        
        for block_name, block_data in nested_data.items():
            flat_block = {}
            
            def flatten_category(category_data, category_name):
                """Recursively flatten nested category data."""
                if isinstance(category_data, dict):
                    # Check if this dict represents item data or nested categories
                    if self._is_item_dict(category_data):
                        # This is item data - keep as is
                        flat_block[category_name] = category_data
                    else:
                        # This contains nested categories - flatten them
                        for key, value in category_data.items():
                            if isinstance(value, (dict, list)):
                                flatten_category(value, key)
                            else:
                                # Individual item - shouldn't happen at this level
                                flat_block[key] = value
                elif isinstance(category_data, list):
                    # Multi-row category - keep as is
                    flat_block[category_name] = category_data
                else:
                    # Simple value - keep as is
                    flat_block[category_name] = category_data
            
            for category_name, category_data in block_data.items():
                flatten_category(category_data, category_name)
            
            flat_data[block_name] = flat_block
        
        return flat_data
    
    def _is_item_dict(self, data: Dict[str, Any]) -> bool:
        """Check if a dictionary represents item data vs nested categories."""
        # Item dictionaries typically have string/primitive values
        # Category dictionaries have dict/list values
        return all(not isinstance(v, (dict, list)) or 
                  (isinstance(v, list) and all(not isinstance(item, dict) for item in v))
                  for v in data.values())
    
    def _convert_flat_json_to_mmcif(self, json_data: Dict[str, Any]) -> MMCIFDataContainer:
        """
        Convert flat JSON structure to mmCIF format.
        
        This mirrors the flat JSON structure used in JSONExporter._to_flat_json().
        """
        from .models import DataBlock, Category
        
        blocks = {}
        
        for block_name, block_data in json_data.items():
            # Remove data_ prefix if present for internal storage
            internal_block_name = block_name[5:] if block_name.startswith("data_") else block_name
            
            categories = {}
            
            for category_name, category_data in block_data.items():
                # Create category (name should already have underscore prefix)
                category = Category(name=category_name, validator_factory=None)
                
                if isinstance(category_data, list):
                    # Multi-row category
                    for row in category_data:
                        if isinstance(row, dict):
                            for item_name, item_value in row.items():
                                category._add_item_value(item_name, str(item_value))
                elif isinstance(category_data, dict):
                    # Single-row category
                    for item_name, item_value in category_data.items():
                        category._add_item_value(item_name, str(item_value))
                
                # Commit batches to make data available
                category._commit_all_batches()
                categories[category_name] = category
            
            blocks[internal_block_name] = DataBlock(internal_block_name, categories)
        
        return MMCIFDataContainer(blocks)
