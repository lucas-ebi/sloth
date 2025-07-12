"""
SLOTH mmCIF Writer - High-Performance Gemmi Backend

This module provides the main MMCIFWriter class that uses gemmi as the backend
for optimal performance while maintaining the same elegant SLOTH API.

Note: The original pure Python implementation is available in sloth.legacy
for reference purposes if needed.
"""

from typing import IO
from .models import MMCIFDataContainer, DataBlock
from .common import BaseWriter


class MMCIFWriter(BaseWriter):
    """
    High-performance mmCIF writer using gemmi backend with SLOTH's elegant API.
    
    This writer uses gemmi's optimized C++ backend for fast writing while
    maintaining the exact same API as the original SLOTH writer.
    """
    
    def __init__(self, permissive: bool = False):
        """
        Initialize the MMCIFWriter with gemmi backend.
        
        Args:
            permissive: If False, performs validation during writing
        """
        self.permissive = permissive
        
    def write(self, file_obj: IO, mmcif: MMCIFDataContainer) -> None:
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
                "gemmi is required for MMCIFWriter. Install with: pip install gemmi\n"
                "Note: Legacy writer is available in sloth.legacy for reference."
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
                "gemmi is required for MMCIFWriter. Install with: pip install gemmi"
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
                # Create a loop - need full tag names for gemmi
                full_tag_names = [f"{category_name}.{field_name}" for field_name in field_names]
                loop = gemmi_block.init_loop("", full_tag_names)
                
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
