#!/usr/bin/env python3
"""
JSON Exporter for SLOTH - Focused on nested JSON format using current serializers.

This module provides functionality to export mmCIF data to nested JSON format
using the RelationshipResolver from serializer.py. Supports validation 
through intermediate PDBML XML when permissive=False.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union
from .models import MMCIFDataContainer
from .common import BaseExporter
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


class JSONExporter(BaseExporter):
    """Export mmCIF data to nested JSON format with optional validation."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False,
        precomputed: bool = True  # Add this parameter
    ):
        """
        Initialize the JSON exporter.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            xsd_path: Path to PDBML XSD schema file  
            cache_dir: Directory for caching
            permissive: If False, validates through PDBML XML against XSD schema
            quiet: Suppress output messages
            precomputed: Use precomputed DBML relationship mappings for faster performance
        """
        super().__init__(dict_path, xsd_path, cache_dir, permissive, quiet)
        
        # Set up JSON-specific components with precomputation support
        from .serializer import (
            RelationshipResolver, PrecomputedMappingGenerator, 
            DictionaryParser, XSDParser, get_cache_manager
        )
        
        cache_manager = get_cache_manager(
            self.cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
        )
        
        # Set up metadata parsers
        dict_parser = DictionaryParser(cache_manager, self.quiet)
        xsd_parser = XSDParser(cache_manager, self.quiet)
        dict_parser.source = self.dict_path
        xsd_parser.source = self.xsd_path
        
        # Set up mapping generator with precomputation support
        mapping_generator = PrecomputedMappingGenerator(
            dict_parser, xsd_parser, cache_manager, self.quiet, precomputed=precomputed
        )
        self.resolver = RelationshipResolver(mapping_generator, precomputed=precomputed)
    
    def export_data(
        self, 
        mmcif_data: MMCIFDataContainer,
        file_path: Optional[Union[str, Path]] = None,
        permissive: bool = None,
        indent: int = 2
    ) -> Optional[str]:
        """
        Export mmCIF data to JSON format (always nested).
        
        Args:
            mmcif_data: The mmCIF data container to export
            file_path: Path to save the file (optional)
            permissive: Override permissive mode setting for schema validation
            indent: Number of spaces for indentation
            
        Returns:
            JSON string if no file_path provided, otherwise None
        """
        # Get nested JSON using relationship resolution
        nested_data = self._to_nested_json(mmcif_data, permissive)
        
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

    def _to_nested_json(
        self, 
        mmcif_data: MMCIFDataContainer,
        permissive: bool = None
    ) -> Dict[str, Any]:
        """
        Export mmCIF data to nested JSON format using relationship resolution.
        
        Args:
            mmcif_data: The mmCIF data container to export
            permissive: Override permissive mode setting for schema validation
            
        Returns:
            Nested JSON dictionary with resolved relationships and block structure
            
        Raises:
            ValidationError: If validation fails and not in permissive mode
        """
        if permissive is None:
            validate = not self.permissive
        else:
            validate = not permissive
        
        # Handle multiple blocks by processing each separately to preserve block boundaries
        result = {}
        
        for block in mmcif_data:
            # Create a temporary container with just this block
            from .models import MMCIFDataContainer
            single_block_container = MMCIFDataContainer()
            single_block_container.data[block.name] = block
            
            # Convert this single block to PDBML XML using base class method
            pdbml_xml = self._convert_to_pdbml(single_block_container)
            
            # Validate if requested using base class method
            if validate and self.converter and self.validator:
                self._validate_pdbml_content(pdbml_xml)
            
            # Resolve relationships for this block only
            nested_categories = self.resolver.resolve_relationships(pdbml_xml)
            
            # Add underscore prefix to category names for consistency with flat format and external API
            prefixed_categories = {}
            for category_name, category_data in nested_categories.items():
                if not category_name.startswith("_"):
                    prefixed_name = f"_{category_name}"
                else:
                    prefixed_name = category_name
                prefixed_categories[prefixed_name] = category_data
            
            # Use block name directly from the block object
            # Make sure to include the data_ prefix for external API consistency
            block_name = block.name
            if not block_name.startswith("data_"):
                block_name = f"data_{block_name}"
            
            result[block_name] = prefixed_categories
        
        return result

class XMLExporter(BaseExporter):
    """Export mmCIF data to PDBML XML format with optional validation."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        """
        Initialize the XML exporter.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            xsd_path: Path to PDBML XSD schema file  
            cache_dir: Directory for caching
            permissive: If False, validates through PDBML XML against XSD schema
            quiet: Suppress output messages
        """
        super().__init__(dict_path, xsd_path, cache_dir, permissive, quiet)
    
    def export_data(
        self, 
        mmcif_data: MMCIFDataContainer,
        file_path: Optional[Union[str, Path]] = None,
        permissive: bool = None,
        pretty_print: bool = True
    ) -> Optional[str]:
        """
        Export mmCIF data to XML format.
        
        Args:
            mmcif_data: The mmCIF data container to export
            file_path: Path to save the file (optional)
            nested: Whether to use nested structure (ignored for XML)
            permissive: Override permissive mode setting for schema validation
            pretty_print: Whether to format XML with indentation
            
        Returns:
            XML string if no file_path provided, otherwise None
        """
        if permissive is None:
            validate = not self.permissive
        else:
            validate = not permissive
            
        # Convert mmCIF to PDBML XML using base class method
        pdbml_xml = self._convert_to_pdbml(mmcif_data)
        
        # Validate if requested using base class method
        if validate and self.converter and self.validator:
            self._validate_pdbml_content(pdbml_xml)
        
        # Pretty print if requested
        if pretty_print:
            try:
                from xml.dom import minidom
                dom = minidom.parseString(pdbml_xml)
                xml_str = dom.toprettyxml(indent="  ")
            except:
                # Fallback to raw XML if pretty printing fails
                xml_str = pdbml_xml
        else:
            xml_str = pdbml_xml
        
        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(xml_str)
            if not self.quiet:
                print(f"Exported PDBML XML to: {file_path}")
            return None
        else:
            return xml_str
