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


class JSONImporter(BaseImporter):
    """Import nested JSON data back to mmCIF format with optional validation."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        json_schema_nested_path: Optional[Union[str, Path]] = None,
        json_schema_flat_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        """
        Initialize the JSON importer.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            xsd_path: Path to PDBML XSD schema file
            json_schema_nested_path: Path to JSON schema file for nested structure validation
            json_schema_flat_path: Path to JSON schema file for flat structure validation
            cache_dir: Directory for caching
            permissive: If False, validates through PDBML XML against XSD schema
            quiet: Suppress output messages
        """
        super().__init__(dict_path, xsd_path, cache_dir, permissive, quiet)
        
        # Set JSON schema paths (structure-specific)
        if json_schema_nested_path is None:
            json_schema_nested_path = Path(__file__).parent / "schemas" / "mmcif_json_nested_schema.json"
        if json_schema_flat_path is None:
            json_schema_flat_path = Path(__file__).parent / "schemas" / "mmcif_json_flat_schema.json"
            
        self.json_schema_nested_path = json_schema_nested_path
        self.json_schema_flat_path = json_schema_flat_path
        
        # Load JSON schemas for structure validation (always loaded)
        with open(json_schema_nested_path, 'r') as f:
            self.json_schema_nested = json.load(f)
        with open(json_schema_flat_path, 'r') as f:
            self.json_schema_flat = json.load(f)
    
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
            nested: Whether to expect nested structure (True) or flat (False)
            permissive: Override schema validation permissiveness (uses self.permissive if None)
            
        Returns:
            MMCIFDataContainer with imported data
            
        Raises:
            ValidationError: If validation fails and permissive=False
        """
        # Parse JSON input
        if isinstance(data, (str, Path)):
            if Path(data).exists():
                # It's a file path
                with open(data, 'r') as f:
                    json_data = json.load(f)
            else:
                # It's a JSON string
                json_data = json.loads(data)
        else:
            # It's already a dict
            json_data = data
        
        # Validate JSON structure for both nested and flat formats
        self._validate_json_structure(json_data, nested)
        
        # Convert to flat mmCIF structure
        if nested:
            flat_data = self._flatten_nested_json(json_data)
        else:
            flat_data = json_data
        
        # Create mmCIF container
        container = self._create_mmcif_container(flat_data)
        
        # Validate content if required (PDBML XSD validation)
        should_validate = not (self.permissive if permissive is None else permissive)
        if should_validate and self.converter and self.validator:
            self._validate_content_via_pdbml(container)
        
        return container
    
    def _validate_json_structure(self, data: Dict[str, Any], nested: bool = True) -> None:
        """
        Validate JSON data structure against the appropriate schema.
        
        This validation is always performed regardless of permissive mode,
        as correct structure is essential for downstream processing.
        
        Args:
            data: JSON data to validate
            nested: Whether to validate against nested or flat schema
            
        Raises:
            ValidationError: If JSON structure validation fails
        """
        try:
            # Choose appropriate schema based on structure type
            schema = self.json_schema_nested if nested else self.json_schema_flat
            schema_type = "nested" if nested else "flat"
            
            jsonschema.validate(instance=data, schema=schema)
            if not self.quiet:
                print(f"✅ JSON {schema_type} structure validation passed")
        except jsonschema.ValidationError as e:
            schema_type = "nested" if nested else "flat"
            error_msg = f"JSON {schema_type} structure validation failed: {e.message}"
            if hasattr(e, 'absolute_path') and e.absolute_path:
                error_msg += f" at path: {'.'.join(str(p) for p in e.absolute_path)}"
            raise ValidationError(error_msg)
        except jsonschema.SchemaError as e:
            schema_type = "nested" if nested else "flat"
            raise ValidationError(f"JSON {schema_type} schema error: {e.message}")

    def _flatten_nested_json(self, nested_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Convert nested JSON structure back to flat mmCIF-style data.
        
        This reverses the nesting process performed by the RelationshipResolver.
        """
        flat_data = {}
        
        def extract_category(category_name: str, items: Union[List, Dict], parent_refs: Dict = None):
            """Recursively extract categories and their relationships."""
            if not items:
                return
                
            # Normalize to list format
            if isinstance(items, dict):
                items = [items]
            elif not isinstance(items, list):
                return
            
            # Initialize category in flat_data if not exists
            if category_name not in flat_data:
                flat_data[category_name] = []
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                # Create flat item with parent references
                flat_item = {}
                
                # Add parent references if provided
                if parent_refs:
                    flat_item.update(parent_refs)
                
                # Process item attributes and nested categories
                for key, value in item.items():
                    if isinstance(value, (list, dict)) and self._is_nested_category(key):
                        # This is a nested category - extract it recursively
                        child_refs = self._get_child_references(category_name, key, item)
                        extract_category(key, value, child_refs)
                    else:
                        # This is a regular attribute
                        flat_item[key] = value
                
                # Add the flat item to the category
                if flat_item:  # Only add if it has content
                    flat_data[category_name].append(flat_item)
        
        # Start extraction from top-level categories
        for category_name, category_data in nested_data.items():
            extract_category(category_name, category_data)
        
        return flat_data
    
    def _is_nested_category(self, key: str) -> bool:
        """Check if a key represents a nested category."""
        # Common nested category patterns
        nested_patterns = [
            'entity_poly', 'entity_poly_seq', 'struct_asym', 'atom_site',
            'citation_author', 'struct_conf', 'struct_sheet', 'database_2'
        ]
        return key in nested_patterns or '_' in key
    
    def _get_child_references(self, parent_category: str, child_category: str, parent_item: Dict) -> Dict[str, Any]:
        """Generate reference fields for child categories based on relationships."""
        refs = {}
        
        # Common relationship patterns
        if parent_category == 'entity':
            if child_category in ['entity_poly', 'entity_poly_seq', 'struct_asym']:
                refs['entity_id'] = parent_item.get('id')
        
        elif parent_category == 'struct_asym':
            if child_category == 'atom_site':
                refs['label_asym_id'] = parent_item.get('id')
                refs['label_entity_id'] = parent_item.get('entity_id')
        
        elif parent_category == 'citation':
            if child_category == 'citation_author':
                refs['citation_id'] = parent_item.get('id')
        
        return refs
    
    def _create_mmcif_container(self, flat_data: Dict[str, Any]) -> MMCIFDataContainer:
        """Create MMCIFDataContainer from flat data structure."""
        containers = {}
        
        # Check if this is already in the expected format (categories as top-level keys)
        # or if it's in flat export format (data blocks as top-level keys)
        if any(isinstance(v, dict) and not isinstance(list(v.values())[0] if v else None, list) 
               for v in flat_data.values()):
            # This looks like flat export format: {"block_name": {"_category": {...}}}
            for block_name, block_data in flat_data.items():
                categories = {}
                for category_name, items in block_data.items():
                    if items:  # Only create categories with data
                        category = Category(category_name)
                        
                        if isinstance(items, list):
                            # Multi-row category
                            all_fields = set()
                            for item in items:
                                if isinstance(item, dict):
                                    all_fields.update(item.keys())
                            
                            # Initialize category fields
                            for field in all_fields:
                                category[field] = []
                            
                            # Populate with data
                            for item in items:
                                for field in all_fields:
                                    value = item.get(field, '?') if isinstance(item, dict) else '?'
                                    category[field].append(str(value))
                        else:
                            # Single-row category
                            if isinstance(items, dict):
                                for field, value in items.items():
                                    category[field] = [str(value)]
                        
                        categories[category_name] = category
                
                data_block = DataBlock(block_name, categories)
                containers[block_name] = data_block
        else:
            # This is in the expected format: {"_category": [...]}
            block_name = self._get_block_name(flat_data)
            categories = {}
            
            for category_name, items in flat_data.items():
                if items:  # Only create categories with data
                    category = Category(category_name)
                    
                    # Get all field names from all items
                    all_fields = set()
                    for item in items:
                        if isinstance(item, dict):
                            all_fields.update(item.keys())
                    
                    # Initialize category fields
                    for field in all_fields:
                        category[field] = []
                    
                    # Populate with data
                    for item in items:
                        for field in all_fields:
                            value = item.get(field, '?') if isinstance(item, dict) else '?'
                            category[field].append(str(value))
                    
                    categories[category_name] = category
            
            data_block = DataBlock(block_name, categories)
            containers[block_name] = data_block
        
        return MMCIFDataContainer(containers)
    
    def _get_block_name(self, flat_data: Dict[str, Any]) -> str:
        """Extract data block name from the data."""
        # If the keys don't start with '_', they're probably block names
        non_category_keys = [k for k in flat_data.keys() if not k.startswith('_')]
        if non_category_keys:
            return non_category_keys[0]
        
        # Try to get from entry category
        if '_entry' in flat_data:
            entry_data = flat_data['_entry']
            if isinstance(entry_data, list) and entry_data:
                entry_id = entry_data[0].get('id')
                if entry_id:
                    return str(entry_id)
            elif isinstance(entry_data, dict):
                entry_id = entry_data.get('id')
                if entry_id:
                    return str(entry_id)
        
        # Default name
        return 'IMPORTED_DATA'


class XMLImporter(BaseImporter):
    """Import PDBML XML data back to mmCIF format with optional validation."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        xml_schema_nested_path: Optional[Union[str, Path]] = None,
        xml_schema_flat_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        """
        Initialize the XML importer.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            xsd_path: Path to PDBML XSD schema file
            xml_schema_nested_path: Path to XML schema file for nested structure validation
            xml_schema_flat_path: Path to XML schema file for flat structure validation
            cache_dir: Directory for caching
            permissive: If False, validates XML against XSD schema
            quiet: Suppress output messages
        """
        super().__init__(dict_path, xsd_path, cache_dir, permissive, quiet)
        
        # Set XML schema paths (structure-specific, for XML structure validation)
        if xml_schema_nested_path is None:
            xml_schema_nested_path = Path(__file__).parent / "schemas" / "mmcif_xml_nested_schema.xsd"
        if xml_schema_flat_path is None:
            xml_schema_flat_path = Path(__file__).parent / "schemas" / "mmcif_xml_flat_schema.xsd"
            
        self.xml_schema_nested_path = xml_schema_nested_path
        self.xml_schema_flat_path = xml_schema_flat_path
    
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
            nested: Whether the XML has nested or flat structure
            permissive: Override schema validation permissiveness (uses self.permissive if None)
            
        Returns:
            MMCIFDataContainer with imported data
            
        Raises:
            ValidationError: If validation fails and permissive=False
        """
        # Parse XML input
        if isinstance(data, (str, Path)):
            if Path(data).exists():
                # It's a file path
                with open(data, 'r') as f:
                    xml_data = f.read()
            else:
                # It's an XML string
                xml_data = data
        else:
            raise ValidationError("Invalid data type for XML import")
        
        # Validate XML structure
        self._validate_xml_structure(xml_data, nested)
        
        # Convert XML to mmCIF container
        container = self._convert_xml_to_mmcif(xml_data)
        
        # Validate content if required (PDBML XSD validation)
        should_validate = not (self.permissive if permissive is None else permissive)
        if should_validate and self.converter and self.validator:
            self._validate_content_via_pdbml(container)
        
        return container
    
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
            # For now, use the simple conversion method
            # TODO: Implement proper PDBML to mmCIF conversion in PDBMLConverter
            return self._simple_xml_to_mmcif(xml_data)
        except Exception as e:
            raise ValidationError(f"XML to mmCIF conversion error: {str(e)}")
    
    def _simple_xml_to_mmcif(self, xml_data: str) -> MMCIFDataContainer:
        """Simple XML to mmCIF conversion without full validation."""
        # Parse XML
        root = ET.fromstring(xml_data)
        
        # Create a simple data block
        block_name = root.get('datablockName', 'IMPORTED_XML')
        categories = {}
        
        # This is a very basic implementation
        # In practice, the PDBMLConverter handles this properly
        from .models import DataBlock
        data_block = DataBlock(block_name, categories)
        return MMCIFDataContainer({block_name: data_block})
    
    def _validate_xml_structure(self, xml_data: str, nested: bool = True) -> None:
        """
        Validate XML data structure against the appropriate schema.
        
        This validation checks XML structure/topology, separate from PDBML content validation.
        
        Args:
            xml_data: XML data string to validate
            nested: Whether to validate against nested or flat schema
            
        Raises:
            ValidationError: If XML structure validation fails
        """
        try:
            from lxml import etree
            
            # Choose appropriate schema based on structure type
            schema_path = self.xml_schema_nested_path if nested else self.xml_schema_flat_path
            schema_type = "nested" if nested else "flat"
            
            # Load and parse schema
            with open(schema_path, 'r') as f:
                schema_doc = etree.parse(f)
            schema = etree.XMLSchema(schema_doc)
            
            # Parse and validate XML
            xml_doc = etree.fromstring(xml_data.encode('utf-8'))
            schema.assertValid(xml_doc)
            
            if not self.quiet:
                print(f"✅ XML {schema_type} structure validation passed")
                
        except etree.XMLSyntaxError as e:
            raise ValidationError(f"XML syntax error: {str(e)}")
        except etree.DocumentInvalid as e:
            schema_type = "nested" if nested else "flat"
            raise ValidationError(f"XML {schema_type} structure validation failed: {str(e)}")
        except Exception as e:
            schema_type = "nested" if nested else "flat"
            raise ValidationError(f"XML {schema_type} structure validation error: {str(e)}")
