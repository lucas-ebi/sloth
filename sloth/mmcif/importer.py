#!/usr/bin/env python3
"""
Importer for SLOTH - JSON import capabilities.

This module provides functionality to import nested JSON data
back into mmCIF format, enabling round-trip conversions.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
import jsonschema
from .models import MMCIFDataContainer, DataBlock, Category
from .parser import MMCIFParser
from .common import BaseImporter
# StructureFormat removed - JSON is always nested
from .serializer import (
    RelationshipResolver,
    DictionaryParser,
    MappingGenerator,
    get_cache_manager
)
from .validator import ValidationError


class JSONImporter(BaseImporter):
    """Import JSON data back to mmCIF format."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        quiet: bool = False
    ):
        """Initialize the JSON importer.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            cache_dir: Directory for caching
            quiet: Suppress output messages
        """
        super().__init__(dict_path, cache_dir, quiet)
        
        # Always skip validation
        self.resolver = None
    
    def import_data(
        self, 
        data: Union[str, Dict[str, Any], Path]
    ) -> MMCIFDataContainer:
        """Import JSON data back to mmCIF format.
        
        JSON import always expects nested structure since that's our default export format.
        
        Args:
            data: JSON data as string, dict, or file path
            
        Returns:
            MMCIFDataContainer with imported data
        """
        # Parse JSON input
        json_data = self._parse_json_input(data)
        
        # Always use nested JSON import since that's our default format
        return self._import_nested_json(json_data)
    
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
        json_data: Dict[str, Any]
    ) -> MMCIFDataContainer:
        """Import nested JSON back to mmCIF format.
        
        This mirrors the JSONExporter._to_nested_json() process in reverse:
        1. Flatten nested JSON to flat format
        2. Convert flat JSON to mmCIF container
        """
        # Convert nested JSON to flat format first
        flat_structure = self._flatten_nested_json(json_data)

        # Convert flat JSON to mmCIF container
        container = self._convert_flat_json_to_mmcif(flat_structure)

        # Return the container
        return container

    def _flatten_nested_json(self, nested_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten nested JSON structure back to flat format.
        
        This reverses the nesting done by RelationshipResolver.
        Extracts child categories nested within parent rows and creates
        separate top-level categories for each. Handles recursive nesting
        (e.g., atom_site nested in struct_asym nested in entity).
        
        Complexity: O(n) where n is total number of items across all categories,
        with each item processed once regardless of nesting depth.
        """
        flat_data = {}
        
        for block_name, block_data in nested_data.items():
            flat_block = {}
            
            # Process each top-level category
            for category_name, category_data in block_data.items():
                nested_categories = {}
                flattened_data = self._extract_and_flatten(category_data, nested_categories)
                
                # Add flattened parent category
                flat_block[category_name] = flattened_data
                
                # Add all extracted nested categories
                flat_block.update(nested_categories)
            
            flat_data[block_name] = flat_block
        
        return flat_data
    
    def _extract_and_flatten(self, data: Any, accumulated_categories: Dict[str, List]) -> Any:
        """
        Extract nested categories and return flattened data.
        
        Single-pass algorithm that processes each row once, extracting
        nested categories while building the flattened parent.
        
        Args:
            data: Category data (list of rows, single dict, or primitive)
            accumulated_categories: Dict accumulating extracted nested categories
            
        Returns:
            Flattened data with nested categories removed
        """
        if not isinstance(data, (list, dict)):
            return data
        
        if isinstance(data, list):
            return self._flatten_list_category(data, accumulated_categories)
        else:
            return self._flatten_dict_category(data, accumulated_categories)
    
    def _flatten_list_category(self, rows: List[Dict], accumulated_categories: Dict[str, List]) -> List[Dict]:
        """
        Flatten a multi-row category, extracting nested categories from each row.
        
        Args:
            rows: List of row dictionaries
            accumulated_categories: Dict accumulating extracted nested categories
            
        Returns:
            List of flattened rows without nested categories
        """
        flattened_rows = []
        
        for row in rows:
            if not isinstance(row, dict):
                flattened_rows.append(row)
                continue
            
            flattened_row = self._extract_nested_from_row(row, accumulated_categories)
            flattened_rows.append(flattened_row)
        
        return flattened_rows
    
    def _flatten_dict_category(self, row: Dict[str, Any], accumulated_categories: Dict[str, List]) -> Dict[str, Any]:
        """
        Flatten a single-row category, extracting nested categories.
        
        Args:
            row: Single row dictionary
            accumulated_categories: Dict accumulating extracted nested categories
            
        Returns:
            Flattened row without nested categories
        """
        return self._extract_nested_from_row(row, accumulated_categories)
    
    def _extract_nested_from_row(self, row: Dict[str, Any], accumulated_categories: Dict[str, List]) -> Dict[str, Any]:
        """
        Extract nested categories from a single row.
        
        Separates regular items from nested categories. Nested categories are
        identified as dict/list values (except for 'id' which is always regular).
        
        Args:
            row: Row dictionary containing both regular items and nested categories
            accumulated_categories: Dict accumulating extracted nested categories
            
        Returns:
            Row dictionary with only regular items
        """
        regular_items = {}
        
        for key, value in row.items():
            if self._is_regular_item(key, value):
                regular_items[key] = value
            else:
                self._extract_nested_category(key, value, accumulated_categories)
        
        return regular_items
    
    def _is_regular_item(self, key: str, value: Any) -> bool:
        """
        Determine if a row item is a regular data item vs nested category.
        
        Args:
            key: Item key/name
            value: Item value
            
        Returns:
            True if regular item, False if nested category
        """
        # 'id' is always a regular item even if it's a list/dict
        if key == 'id':
            return True
        
        # Non-list/dict values are regular items
        return not isinstance(value, (list, dict))
    
    def _extract_nested_category(self, key: str, value: Any, accumulated_categories: Dict[str, List]) -> None:
        """
        Extract a nested category and add it to accumulated categories.
        
        Args:
            key: Category key (without underscore prefix)
            value: Nested category data
            accumulated_categories: Dict accumulating extracted nested categories
        """
        # Add underscore prefix for category name
        cat_name = f"_{key}" if not key.startswith('_') else key
        
        # Initialize category list if first occurrence
        if cat_name not in accumulated_categories:
            accumulated_categories[cat_name] = []
        
        # Recursively flatten nested data
        nested_flattened = self._extract_and_flatten(value, accumulated_categories)
        
        # Add to accumulated categories
        if isinstance(nested_flattened, list):
            accumulated_categories[cat_name].extend(nested_flattened)
        else:
            accumulated_categories[cat_name].append(nested_flattened)
    
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
                category = Category(name=category_name)
                
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
