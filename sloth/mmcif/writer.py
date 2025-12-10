"""
SLOTH mmCIF Writer - High-Performance Gemmi Backend

This module provides the main MMCIFWriter class that uses gemmi as the backend
for optimal performance while maintaining the elegant SLOTH API.
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
    
    def __init__(self):
        """Initialize the MMCIFWriter with gemmi backend."""
        pass
        
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
                "gemmi is required for MMCIFWriter. Install with: pip install gemmi"
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
    
    @staticmethod
    def _cif_quote_value(value: str) -> str:
        """
        Quote a CIF value if necessary according to CIF specification.
        
        Values need quoting if they:
        - Contain whitespace
        - Start with underscore, hash, dollar, quote, or semicolon
        - Are reserved words (data_, loop_, stop_, global_)
        """
        value_str = str(value)
        
        # Check if quoting is needed
        needs_quoting = (
            ' ' in value_str or 
            '\t' in value_str or
            '\n' in value_str or
            value_str.startswith(('_', '#', '$', "'", '"', ';')) or
            value_str.lower() in ('data_', 'loop_', 'stop_', 'global_') or
            value_str in ('.', '?')
        )
        
        if not needs_quoting:
            return value_str
        
        # Use single quotes if no single quotes in value
        if "'" not in value_str:
            return f"'{value_str}'"
        
        # Use double quotes if no double quotes in value
        if '"' not in value_str:
            return f'"{value_str}"'
        
        # Use semicolon-delimited text field for complex cases
        return f'\n;{value_str}\n;'
    
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
                            row.append(self._cif_quote_value(values[i]))
                        else:
                            row.append('.')
                    loop.add_row(row)
            else:
                # Add as single items
                for field_name, values in zip(field_names, item_values):
                    tag = f"{category_name}.{field_name}"
                    value = self._cif_quote_value(values[0]) if values else '.'
                    gemmi_block.set_pair(tag, value)
        
        return gemmi_block

