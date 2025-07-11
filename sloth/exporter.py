#!/usr/bin/env python3
"""
JSON Exporter for SLOTH - Focused on nested JSON format using current serializers.

This module provides functionality to export mmCIF data to nested JSON format
using the RelationshipResolver from serializers.py. Supports validation 
through intermediate PDBML XML when permissive=False.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union
from .models import MMCIFDataContainer
from .serializers import (
    PDBMLConverter, 
    RelationshipResolver,
    DictionaryParser,
    XSDParser,
    MappingGenerator,
    get_cache_manager
)
from .validators import XMLSchemaValidator, ValidationError


class JSONExporter:
    """Export mmCIF data to nested JSON format with optional validation."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        """
        Initialize the JSON exporter.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            xsd_path: Path to PDBML XSD schema file  
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
            
        self.dict_path = dict_path
        self.xsd_path = xsd_path
        
        # Set up components
        cache_manager = get_cache_manager(
            cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
        )
        
        # Set up metadata parsers
        dict_parser = DictionaryParser(cache_manager, quiet)
        xsd_parser = XSDParser(cache_manager, quiet)
        dict_parser.source = dict_path
        xsd_parser.source = xsd_path
        
        # Set up mapping generator and components
        mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache_manager, quiet)
        self.converter = PDBMLConverter(mapping_generator, permissive, quiet)
        self.resolver = RelationshipResolver(mapping_generator)
        
        # Set up validator if not permissive
        if not permissive:
            self.validator = XMLSchemaValidator(xsd_path)
        else:
            self.validator = None
    
    def export_to_nested_json(
        self, 
        mmcif_data: MMCIFDataContainer,
        validate: bool = None
    ) -> Dict[str, Any]:
        """
        Export mmCIF data to nested JSON format using relationship resolution.
        
        Args:
            mmcif_data: The mmCIF data container to export
            validate: Whether to validate (defaults to not self.permissive)
            
        Returns:
            Nested JSON dictionary with resolved relationships
            
        Raises:
            ValidationError: If validation fails and not in permissive mode
        """
        if validate is None:
            validate = not self.permissive
            
        # Convert mmCIF to PDBML XML first
        pdbml_xml = self.converter.convert_to_pdbml(mmcif_data)
        
        # Validate if requested
        if validate and self.validator:
            validation_result = self.validator.validate(pdbml_xml)
            if not validation_result.get("valid", False):
                errors = validation_result.get("errors", [])
                error_msg = f"PDBML validation failed: {'; '.join(errors)}"
                if not self.permissive:
                    raise ValidationError(error_msg)
                elif not self.quiet:
                    print(f"Warning: {error_msg}")
        
        # Resolve relationships to create nested JSON
        nested_json = self.resolver.resolve_relationships(pdbml_xml)
        
        return nested_json
    
    def to_json(
        self, 
        mmcif_data: MMCIFDataContainer,
        file_path: Optional[Union[str, Path]] = None, 
        indent: int = 2,
        validate: bool = None
    ) -> Optional[str]:
        """
        Export mmCIF data to JSON format.

        Args:
            mmcif_data: The mmCIF data container to export
            file_path: Path to save the JSON file (optional)
            indent: Number of spaces for indentation
            validate: Whether to validate (defaults to not self.permissive)
            
        Returns:
            JSON string if no file_path provided, otherwise None
            
        Raises:
            ValidationError: If validation fails and not in permissive mode
        """
        # Get nested JSON using relationship resolution
        nested_data = self.export_to_nested_json(mmcif_data, validate)
        
        # Convert to JSON string
        json_str = json.dumps(nested_data, indent=indent, ensure_ascii=False)
        
        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(json_str)
            if not self.quiet:
                print(f"Exported nested JSON to: {file_path}")
            return None
        else:
            return json_str
    
    def to_file(
        self, 
        mmcif_data: MMCIFDataContainer,
        file_path: Union[str, Path], 
        indent: int = 2,
        validate: bool = None
    ) -> None:
        """
        Export mmCIF data to a JSON file.
        
        Args:
            mmcif_data: The mmCIF data container to export
            file_path: Path to save the JSON file
            indent: Number of spaces for indentation
            validate: Whether to validate (defaults to not self.permissive)
            
        Raises:
            ValidationError: If validation fails and not in permissive mode
        """
        self.to_json(mmcif_data, file_path, indent, validate)


# Convenience function for direct export
def export_mmcif_to_json(
    mmcif_data: MMCIFDataContainer,
    file_path: Optional[Union[str, Path]] = None,
    dict_path: Optional[Union[str, Path]] = None,
    xsd_path: Optional[Union[str, Path]] = None,
    permissive: bool = False,
    validate: bool = None,
    indent: int = 2,
    quiet: bool = False
) -> Optional[str]:
    """
    Convenience function to export mmCIF data to nested JSON format.
    
    Args:
        mmcif_data: The mmCIF data container to export
        file_path: Path to save the JSON file (optional)
        dict_path: Path to mmCIF dictionary file (optional)
        xsd_path: Path to PDBML XSD schema file (optional)
        permissive: If False, validates through PDBML XML against XSD schema
        validate: Whether to validate (defaults to not permissive)
        indent: Number of spaces for indentation
        quiet: Suppress output messages
        
    Returns:
        JSON string if no file_path provided, otherwise None
        
    Raises:
        ValidationError: If validation fails and not in permissive mode
    """
    exporter = JSONExporter(
        dict_path=dict_path,
        xsd_path=xsd_path,
        permissive=permissive,
        quiet=quiet
    )
    
    return exporter.to_json(mmcif_data, file_path, indent, validate)
