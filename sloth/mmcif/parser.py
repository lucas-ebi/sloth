"""
SLOTH mmCIF Parser - High-Performance Gemmi Backend

This module provides the main MMCIFParser class that uses gemmi as the backend
for optimal performance while maintaining the elegant SLOTH API.
"""

from typing import Optional, List, Union
from pathlib import Path
from .models import MMCIFDataContainer, DataBlock, Category, LazyGemmiColumn
from .common import BaseParser


class MMCIFParser(BaseParser):
    """
    High-performance mmCIF parser using gemmi backend with SLOTH's elegant API.
    
    This parser uses gemmi's optimized C++ backend for fast parsing while
    maintaining the exact same API as the original SLOTH parser.
    """
    
    def __init__(
        self,
        categories: Optional[List[str]] = None,
    ):
        """
        Initialize the MMCIFParser with gemmi backend.
        
        :param categories: Optional list of categories to parse (for performance)
        """
        super().__init__(categories=categories)
        
    def parse(self, file_path: Union[str, Path]) -> MMCIFDataContainer:
        """
        Parse mmCIF file using gemmi backend but return SLOTH data structures
        with the same elegant API.
        
        :param file_path: Path to mmCIF file
        :type file_path: Union[str, Path]
        :return: MMCIFDataContainer with SLOTH's elegant API
        :rtype: MMCIFDataContainer
        """
        # Use categories from instance initialization
        parse_categories = self.categories
        
        try:
            import gemmi
        except ImportError:
            raise ImportError(
                "gemmi is required for MMCIFParser. Install with: pip install gemmi"
            )
        
        # Convert Path to string if needed
        file_path_str = str(file_path)
        
        # Use gemmi to parse the file
        doc = gemmi.cif.read_file(file_path_str)
        
        # Convert gemmi structure to SLOTH format
        container = MMCIFDataContainer()
        
        for block in doc:
            sloth_block = self._convert_gemmi_block_to_sloth(block, parse_categories)
            container[block.name] = sloth_block
            
        return container
    
    def _convert_gemmi_block_to_sloth(self, gemmi_block, categories: Optional[List[str]] = None) -> DataBlock:
        """Convert gemmi block to SLOTH DataBlock with same API"""
        sloth_block = DataBlock(gemmi_block.name)
        
        # Collect all category names and their items
        category_items = {}
        
        for item in gemmi_block:
            if item.pair:
                # This is a single item (non-loop)
                tag, value = item.pair
                category_name = self._extract_category_name(tag)
                
                # Apply category filtering if specified
                if categories and category_name not in categories:
                    continue
                
                if category_name not in category_items:
                    category_items[category_name] = {}
                
                field_name = self._extract_field_name(tag)
                category_items[category_name][field_name] = [str(value)]
                
            elif item.loop:
                # This is a loop/table
                loop = item.loop
                tags = loop.tags
                
                if not tags:
                    continue
                    
                # Get category name from first tag
                category_name = self._extract_category_name(tags[0])
                
                # Apply category filtering if specified
                if categories and category_name not in categories:
                    continue
                    
                if category_name not in category_items:
                    category_items[category_name] = {}
                
                # Store lazy column wrappers instead of eagerly loading data
                for i, tag in enumerate(tags):
                    field_name = self._extract_field_name(tag)
                    # Create lazy column that will load data only when accessed
                    category_items[category_name][field_name] = LazyGemmiColumn(loop, i)
        
        # Create SLOTH categories
        for category_name, items in category_items.items():
            sloth_category = Category(category_name)
            
            # Add all items to the category
            for field_name, values in items.items():
                sloth_category[field_name] = values
            
            sloth_block[category_name] = sloth_category
        
        return sloth_block
    
    def _extract_category_name(self, tag: str) -> str:
        """Extract category name from mmCIF tag (e.g., '_atom_site.id' -> '_atom_site')"""
        if '.' in tag:
            return tag.split('.')[0]
        return tag
    
    def _extract_field_name(self, tag: str) -> str:
        """Extract field name from mmCIF tag (e.g., '_atom_site.id' -> 'id')"""
        if '.' in tag:
            return tag.split('.', 1)[1]
        return tag
