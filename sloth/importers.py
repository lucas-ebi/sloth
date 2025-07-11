#!/usr/bin/env python3
"""
JSON Importer for SLOTH - Focused on nested JSON format.

This module provides functionality to import nested JSON data (as produced by 
the exporter) back into mmCIF format, enabling round-trip conversions.
Supports validation through intermediate PDBML XML when permissive=False.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
import jsonschema
from .models import MMCIFDataContainer, DataBlock, Category
from .parser import MMCIFParser
from .serializers import (
    PDBMLConverter, 
    RelationshipResolver,
    DictionaryParser,
    XSDParser,
    MappingGenerator,
    get_cache_manager
)
from .validators import XMLSchemaValidator, ValidationError


class JSONImporter:
    """Import nested JSON data back to mmCIF format with optional validation."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        json_schema_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        """
        Initialize the JSON importer.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            xsd_path: Path to PDBML XSD schema file
            json_schema_path: Path to JSON schema file for structure validation
            cache_dir: Directory for caching
            permissive: If False, validates through PDBML XML against XSD schema
            quiet: Suppress output messages
        """
        self.permissive = permissive
        self.quiet = quiet
        
        # Set default schema paths
        if dict_path is None:
            dict_path = Path(__file__).parent / "schemas" / "mmcif_pdbx_v50.dic"
        if xsd_path is None:
            xsd_path = Path(__file__).parent / "schemas" / "pdbx-v50.xsd"
        if json_schema_path is None:
            json_schema_path = Path(__file__).parent / "schemas" / "mmcif_json_schema.json"
            
        self.dict_path = dict_path
        self.xsd_path = xsd_path
        self.json_schema_path = json_schema_path
        
        # Load JSON schema for structure validation (always loaded)
        with open(json_schema_path, 'r') as f:
            self.json_schema = json.load(f)
        
        # Set up validation components if not permissive
        if not permissive:
            cache_manager = get_cache_manager(
                cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
            )
            
            # Set up metadata parsers
            dict_parser = DictionaryParser(cache_manager, quiet)
            xsd_parser = XSDParser(cache_manager, quiet)
            dict_parser.source = dict_path
            xsd_parser.source = xsd_path
            
            # Set up components for validation
            mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache_manager, quiet)
            self.converter = PDBMLConverter(mapping_generator, permissive=False, quiet=quiet)
            self.validator = XMLSchemaValidator(xsd_path)
        else:
            self.converter = None
            self.validator = None
    
    def import_from_json(
        self, 
        json_data: Union[str, Dict[str, Any], Path], 
        validate: bool = None
    ) -> MMCIFDataContainer:
        """
        Import nested JSON data back to mmCIF format.
        
        Args:
            json_data: JSON data as string, dict, or file path
            validate: Override validation setting (uses self.permissive if None)
            
        Returns:
            MMCIFDataContainer with imported data
            
        Raises:
            ValidationError: If validation fails and permissive=False
        """
        # Parse JSON input
        if isinstance(json_data, (str, Path)):
            if Path(json_data).exists():
                # It's a file path
                with open(json_data, 'r') as f:
                    data = json.load(f)
            else:
                # It's a JSON string
                data = json.loads(json_data)
        else:
            # It's already a dict
            data = json_data
        
        # ALWAYS validate JSON structure first (essential for downstream processes)
        self._validate_json_structure(data)
        
        # Convert nested JSON back to flat mmCIF structure
        flat_data = self._flatten_nested_json(data)
        
        # Create mmCIF container
        container = self._create_mmcif_container(flat_data)
        
        # Validate content if required (PDBML XSD validation)
        should_validate = not self.permissive if validate is None else validate
        if should_validate and self.converter and self.validator:
            self._validate_content(container)
        
        return container
    
    def _validate_json_structure(self, data: Dict[str, Any]) -> None:
        """
        Validate JSON data structure against JSON schema.
        
        This validation is always performed regardless of permissive mode,
        as correct structure is essential for downstream processing.
        
        Args:
            data: JSON data to validate
            
        Raises:
            ValidationError: If JSON structure validation fails
        """
        try:
            jsonschema.validate(instance=data, schema=self.json_schema)
            if not self.quiet:
                print("✅ JSON structure validation passed")
        except jsonschema.ValidationError as e:
            error_msg = f"JSON structure validation failed: {e.message}"
            if hasattr(e, 'absolute_path') and e.absolute_path:
                error_msg += f" at path: {'.'.join(str(p) for p in e.absolute_path)}"
            raise ValidationError(error_msg)
        except jsonschema.SchemaError as e:
            raise ValidationError(f"JSON schema error: {e.message}")

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
    
    def _create_mmcif_container(self, flat_data: Dict[str, List[Dict[str, Any]]]) -> MMCIFDataContainer:
        """Create MMCIFDataContainer from flat data structure."""
        # Create a data block (use first entry's ID or default name)
        block_name = self._get_block_name(flat_data)
        
        # Create categories
        categories = {}
        for category_name, items in flat_data.items():
            if items:  # Only create categories with data
                category = Category(category_name)
                
                # Get all field names from all items
                all_fields = set()
                for item in items:
                    all_fields.update(item.keys())
                
                # Initialize category fields
                for field in all_fields:
                    category[field] = []
                
                # Populate with data
                for item in items:
                    for field in all_fields:
                        value = item.get(field, '?')  # Use '?' for missing values
                        category[field].append(str(value))
                
                categories[category_name] = category
        
        # Create data block and container
        data_block = DataBlock(block_name, categories)
        return MMCIFDataContainer({block_name: data_block})
    
    def _get_block_name(self, flat_data: Dict[str, List[Dict[str, Any]]]) -> str:
        """Extract data block name from the data."""
        # Try to get from entry category
        if 'entry' in flat_data and flat_data['entry']:
            entry_id = flat_data['entry'][0].get('id')
            if entry_id:
                return str(entry_id)
        
        # Default name
        return 'IMPORTED_DATA'
    
    def _validate_content(self, container: MMCIFDataContainer) -> None:
        """Validate container content by converting to PDBML and checking against XSD."""
        try:
            if not self.quiet:
                print("🔍 Starting PDBML XSD content validation...")
            
            # Convert to PDBML XML
            pdbml_xml = self.converter.convert_to_pdbml(container)
            
            # Validate against XSD
            validation_result = self.validator.validate(pdbml_xml)
            
            if isinstance(validation_result, dict):
                if not validation_result.get('valid', False):
                    errors = validation_result.get('errors', [])
                    error_msg = '; '.join(errors) if errors else 'Unknown validation error'
                    raise ValidationError(f"PDBML XSD content validation failed: {error_msg}")
                else:
                    if not self.quiet:
                        print("✅ PDBML XSD content validation passed")
            
        except Exception as e:
            if isinstance(e, ValidationError):
                raise
            else:
                raise ValidationError(f"Content validation process failed: {str(e)}")


# Convenience functions
def import_json_file(
    json_file: Union[str, Path], 
    permissive: bool = False,
    validate: bool = None
) -> MMCIFDataContainer:
    """
    Convenience function to import a JSON file.
    
    Args:
        json_file: Path to JSON file
        permissive: Whether to skip validation
        validate: Override validation setting
        
    Returns:
        MMCIFDataContainer with imported data
    """
    importer = JSONImporter(permissive=permissive)
    return importer.import_from_json(json_file, validate=validate)


def import_json_string(
    json_string: str, 
    permissive: bool = False,
    validate: bool = None
) -> MMCIFDataContainer:
    """
    Convenience function to import a JSON string.
    
    Args:
        json_string: JSON data as string
        permissive: Whether to skip validation
        validate: Override validation setting
        
    Returns:
        MMCIFDataContainer with imported data
    """
    importer = JSONImporter(permissive=permissive)
    return importer.import_from_json(json_string, validate=validate)


def import_json_dict(
    json_dict: Dict[str, Any], 
    permissive: bool = False,
    validate: bool = None
) -> MMCIFDataContainer:
    """
    Convenience function to import a JSON dictionary.
    
    Args:
        json_dict: JSON data as dictionary
        permissive: Whether to skip validation
        validate: Override validation setting
        
    Returns:
        MMCIFDataContainer with imported data
    """
    importer = JSONImporter(permissive=permissive)
    return importer.import_from_json(json_dict, validate=validate)
