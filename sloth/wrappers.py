"""
SLOTH wrappers for external libraries - same elegant API, enhanced performance

This module provides wrappers that maintain the exact same API as SLOTH's core handlers
but use external libraries for parsing and writing behind the scenes for maximum performance.
"""

from typing import Optional, List, Union, Any, Dict
from pathlib import Path
from .models import MMCIFDataContainer, DataBlock, Category
from .common import BaseParser, BaseWriter


class GemmiParser(BaseParser):
    """
    SLOTH parser using gemmi backend - maintains the exact same API as MMCIFParser
    but uses gemmi for parsing behind the scenes for maximum performance.
    
    This class is typically not used directly - instead, use MMCIFHandler
    with use_gemmi=True parameter.
    """
    
    def __init__(self, validator_factory=None, categories=None):
        """
        Initialize the GemmiParser with the same interface as MMCIFParser.
        
        :param validator_factory: Optional validator factory for data validation
        :param categories: Optional list of categories to parse (for performance)
        """
        super().__init__(validator_factory, categories)
        
    def parse_file(self, file_path: Union[str, Path]) -> MMCIFDataContainer:
        """
        Parse mmCIF file using gemmi backend but return SLOTH data structures
        with the same elegant API.
        
        :param file_path: Path to mmCIF file
        :type file_path: Union[str, Path]
        :return: MMCIFDataContainer with same API as regular SLOTH
        :rtype: MMCIFDataContainer
        """
        # Use categories from instance initialization
        parse_categories = self.categories
        
        try:
            import gemmi
        except ImportError:
            raise ImportError(
                "gemmi is required for GemmiParser. Install with: pip install gemmi"
            )
        
        # Use gemmi to parse the file
        doc = gemmi.cif.read_file(str(file_path))
        
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
                
                # Process loop data
                for i, tag in enumerate(tags):
                    field_name = self._extract_field_name(tag)
                    column_data = []
                    
                    # Extract column data
                    for row_idx in range(loop.length()):
                        value = loop[row_idx, i]
                        column_data.append(str(value))
                    
                    category_items[category_name][field_name] = column_data
        
        # Create SLOTH categories
        for category_name, items in category_items.items():
            sloth_category = Category(category_name, self.validator_factory)
            
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


class GemmiWriter(BaseWriter):
    """
    SLOTH writer using gemmi backend - maintains the exact same API as MMCIFWriter
    but uses gemmi for writing behind the scenes for maximum performance.
    
    This class is typically not used directly - instead, use MMCIFHandler
    with use_gemmi=True parameter.
    """
    
    def __init__(self):
        """Initialize the GemmiWriter."""
        pass
        
    def write(self, file_obj, mmcif):
        """
        Write SLOTH data structure to file using gemmi backend
        
        :param file_obj: The file object to write to
        :type file_obj: IO
        :param mmcif: SLOTH MMCIFDataContainer
        :type mmcif: MMCIFDataContainer
        :return: None
        """
        try:
            import gemmi
        except ImportError:
            raise ImportError(
                "gemmi is required for GemmiWriter. Install with: pip install gemmi"
            )
        
        # Convert SLOTH structure back to gemmi format
        doc = gemmi.cif.Document()
        
        for sloth_block in mmcif:
            gemmi_block = self._convert_sloth_block_to_gemmi(sloth_block)
            gemmi_block.name = sloth_block.name
            doc.add_copied_block(gemmi_block)
        
        # Write to file object
        content = doc.as_string()
        file_obj.write(content)
    
    def _convert_sloth_block_to_gemmi(self, sloth_block: DataBlock):
        """Convert SLOTH DataBlock back to gemmi format"""
        try:
            import gemmi
        except ImportError:
            raise ImportError(
                "gemmi is required for GemmiWriter. Install with: pip install gemmi"
            )
        
        gemmi_block = gemmi.cif.Block(sloth_block.name)
        
        # Iterate through categories properly
        for category_name in sloth_block.categories:
            sloth_category = sloth_block[category_name]
            
            # Skip if category has no items
            if not hasattr(sloth_category, 'items') or not sloth_category.items:
                continue
                
            # Get all values and determine max length
            item_values = []
            field_names = list(sloth_category.items)
            
            for field_name in field_names:
                values = sloth_category[field_name]  # This uses __getitem__ which returns the list
                item_values.append(values)
            
            if not item_values:
                continue
                
            max_length = max(len(values) for values in item_values)
            
            if max_length > 1:
                # Create a loop
                loop = gemmi_block.init_loop(category_name, field_names)
                
                # Add rows
                for i in range(max_length):
                    row = []
                    for values in item_values:
                        if i < len(values):
                            row.append(str(values[i]))
                        else:
                            row.append('.')
                    loop.add_row(row)
            else:
                # Add as single items
                for field_name, values in zip(field_names, item_values):
                    tag = f"{category_name}.{field_name}"
                    value = str(values[0]) if values else '.'
                    gemmi_block.set_pair(tag, value)
        
        return gemmi_block
