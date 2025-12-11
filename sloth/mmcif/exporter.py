#!/usr/bin/env python3
"""
JSON Exporter for SLOTH - Export mmCIF data to nested JSON format.

This module provides functionality to export mmCIF data to nested JSON format
using the RelationshipResolver from serializer.py, working directly from mmCIF dictionary.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union
from .models import MMCIFDataContainer
from .common import BaseExporter
from .serializer import (
    RelationshipResolver,
    DictionaryParser,
    MappingGenerator,
    get_cache_manager
)


class JSONExporter(BaseExporter):
    """Export mmCIF data to nested JSON format."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        quiet: bool = False,
        denormalize: bool = False
    ):
        """Initialize the JSON exporter.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            cache_dir: Directory for caching
            quiet: Suppress output messages
            denormalize: If True, embed reference/lookup data for full denormalization
        """
        super().__init__(dict_path, None, cache_dir, quiet)
        
        # Set up JSON-specific components
        cache_manager = get_cache_manager(
            self.cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
        )
        
        # Set up dictionary parser
        dict_parser = DictionaryParser(cache_manager, self.quiet)
        dict_parser.source = self.dict_path
        
        # Set up mapping generator
        mapping_generator = MappingGenerator(dict_parser, cache_manager, self.quiet)
        
        self.resolver = RelationshipResolver(mapping_generator)
        self.resolver.set_denormalize(denormalize)
    
    def export_data(
        self, 
        mmcif_data: MMCIFDataContainer,
        file_path: Optional[Union[str, Path]] = None,
        indent: int = 2
    ) -> Optional[str]:
        """
        Export mmCIF data to JSON format (always nested).
        
        Args:
            mmcif_data: The mmCIF data container to export
            file_path: Path to save the file (optional)
            indent: Number of spaces for indentation
            
        Returns:
            JSON string if no file_path provided, otherwise None
        """
        # Get nested JSON using relationship resolution
        nested_data = self._to_nested_json(mmcif_data)
        
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
        mmcif_data: MMCIFDataContainer
    ) -> Dict[str, Any]:
        """
        Export mmCIF data to nested JSON format using relationship resolution.
        
        Args:
            mmcif_data: The mmCIF data container to export
            
        Returns:
            Nested JSON dictionary with resolved relationships and block structure
        """
        # Handle multiple blocks by processing each separately to preserve block boundaries
        result = {}
        
        for block in mmcif_data:
            # Create a temporary container with just this block
            from .models import MMCIFDataContainer
            single_block_container = MMCIFDataContainer()
            single_block_container.data[block.name] = block
            
            # Resolve relationships directly from mmCIF
            nested_categories = self.resolver.resolve_relationships(single_block_container)
            
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
