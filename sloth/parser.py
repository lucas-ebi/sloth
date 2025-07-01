import mmap
import shlex
import os
from typing import Optional, List
from .models import Category, DataBlock, MMCIFDataContainer, DataSourceFormat
from .validator import ValidatorFactory

def fast_mmcif_split(line: str) -> List[str]:
    """Optimized mmCIF line splitting - avoid shlex overhead when possible."""
    # Fast path: if no quotes, use simple split (90%+ of cases)
    if '"' not in line and "'" not in line:
        return line.split()
    
    # Medium path: try simple approach with basic quote handling
    parts = line.split()
    needs_shlex = False
    for part in parts:
        if (part.startswith('"') and not part.endswith('"')) or \
           (part.startswith("'") and not part.endswith("'")):
            needs_shlex = True
            break
    
    if not needs_shlex:
        # Simple quotes - remove them
        return [p.strip("\"'") if p.startswith(('"', "'")) and p.endswith(('"', "'")) else p for p in parts]
    
    # Complex path: use shlex only when absolutely needed
    try:
        return shlex.split(line)
    except ValueError:
        # Fallback to simple split if shlex fails
        return line.split()

class MMCIFParser:
    """Memory-mapped mmCIF parser with lazy loading for optimal performance."""

    def __init__(self, validator_factory: Optional[ValidatorFactory], categories: Optional[List[str]] = None):
        self.validator_factory = validator_factory
        self.categories = categories
        self._data_blocks = {}
        self._current_block = None
        self._current_category = None
        self._current_data = None
        self._loop_items = []
        self._in_loop = False
        self._multi_line_value = False
        self._multi_line_item_name = ""
        self._multi_line_value_buffer = []
        self._current_row_values = []
        self._value_counter = 0
        self._mmap_obj: Optional[mmap.mmap] = None
        self._file_path: Optional[str] = None

    def parse_file(self, file_path: str) -> MMCIFDataContainer:
        """Parse a file using memory mapping with lazy loading."""
        self._file_path = file_path
        return self._parse_with_mmap(file_path)

    def _parse_with_mmap(self, file_path: str) -> MMCIFDataContainer:
        """Parse using memory mapping with true lazy loading for large files, regular I/O for small files."""
        # Check file size first
        file_size = os.path.getsize(file_path)
        
        # Handle empty files - can't memory map empty files
        if file_size == 0:
            return MMCIFDataContainer({})
        
        # Small-to-medium files: use regular file I/O (faster than mmap overhead)
        if file_size < 10 * 1024 * 1024:  # < 10MB (even higher threshold)
            return self._parse_regular_file(file_path)
        
        # Large files: use memory mapping for efficient lazy loading
        with open(file_path, 'rb') as f:
            # Create memory map
            self._mmap_obj = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            
            # Parse directly from memory map
            return self._parse_from_mmap()
    
    def _parse_regular_file(self, file_path: str) -> MMCIFDataContainer:
        """Parse small files using regular file I/O for better performance."""
        # Read entire file at once (faster than line-by-line for medium files)
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split into lines once (faster than readlines())
        lines = content.split('\n')
        
        # Process lines without memory mapping overhead
        line_count = len(lines)
        for i in range(line_count):
            line = lines[i].rstrip()
            if line:  # Skip empty lines early
                self._process_line_simple(line)
        
        # Commit any remaining batched values
        self._commit_all_category_batches()
        
        return MMCIFDataContainer(self._data_blocks, source_format=DataSourceFormat.MMCIF)
    
    def _commit_all_category_batches(self) -> None:
        """Commit all batched values across all categories."""
        for block in self._data_blocks.values():
            for category in block._categories.values():
                if hasattr(category, '_commit_all_batches'):
                    category._commit_all_batches()
    
    def _process_line_simple(self, line: str) -> None:
        """Process a line for small files without memory mapping overhead."""
        if not line:  # Early exit for empty lines
            return
        
        first_char = line[0]
        # Ultra-fast character-based dispatch
        if first_char == '#':
            return
        elif first_char == 'd':  # data_
            if line.startswith('data_'):
                self._handle_data_block(line)
        elif first_char == 'l':  # loop_
            if line.startswith('loop_'):
                self._start_loop()
        elif first_char == '_':
            self._handle_item_line_simple(line)
        elif self._in_loop and first_char not in ['#', 'd', 'l', '_']:
            self._handle_loop_value_line_simple(line)
        elif self._multi_line_value:
            self._handle_non_loop_multiline(line)
    
    def _parse_from_mmap(self) -> MMCIFDataContainer:
        """Parse directly from memory map using byte offsets for true lazy loading."""
        # Convert mmap to string for line-by-line processing
        # We still need to process line by line to understand the structure
        content = self._mmap_obj[:].decode('utf-8')
        lines = content.split('\n')  # Faster than splitlines() for our use case
        
        # Track byte positions for lazy loading
        current_pos = 0
        
        # Override the _ensure_current_data to create memory-mapped categories
        original_ensure = self._ensure_current_data
        def mmap_ensure_current_data(category: str):
            if self._current_category != category:
                self._current_category = category
                if category not in self._data_blocks[self._current_block]._categories:
                    self._data_blocks[self._current_block]._categories[category] = Category(
                        category, self.validator_factory, self._mmap_obj)
                self._current_data = self._data_blocks[self._current_block]._categories[category]
        
        self._ensure_current_data = mmap_ensure_current_data
        
        try:
            for line in lines:
                # Calculate start and end positions for this line in the mmap
                line_start = current_pos
                line_end = current_pos + len(line.encode('utf-8'))
                current_pos = line_end + 1  # +1 for newline character
                
                # Process the line, potentially storing byte offsets for lazy loading
                self._process_line_with_offsets(line.rstrip(), line_start, line_end)
                
            return MMCIFDataContainer(self._data_blocks, source_format=DataSourceFormat.MMCIF)
        finally:
            # Restore original method
            self._ensure_current_data = original_ensure

    def _process_line_with_offsets(self, line: str, line_start: int, line_end: int) -> None:
        """Process a line and store byte offsets for true lazy loading."""
        if line.startswith('#'):
            return
        elif line.startswith('data_'):
            self._handle_data_block(line)
        elif line.startswith('loop_'):
            self._start_loop()
        elif line.startswith('_'):
            self._handle_item_line_with_offsets(line, line_start, line_end)
        elif self._in_loop:
            self._handle_loop_value_line_with_offsets(line, line_start, line_end)
        elif self._multi_line_value:
            self._handle_non_loop_multiline(line)

    def _handle_item_line_with_offsets(self, line: str, line_start: int, line_end: int) -> None:
        """Handle item lines with byte offset tracking for lazy loading."""
        parts = fast_mmcif_split(line)
        if len(parts) == 2:
            self._handle_simple_item_with_offsets(parts[0], parts[1], line, line_start, line_end)
        else:
            self._handle_loop_item(parts[0], line[len(parts[0]):].strip())

    def _handle_simple_item_with_offsets(self, item_full: str, value: str, full_line: str, line_start: int, line_end: int) -> None:
        """Handle simple items with precise byte offset calculation for lazy loading."""
        category, item = item_full.split('.', 1)
        if not self._should_include_category(category):
            return
        self._ensure_current_data(category)
        
        if value.startswith(';'):
            # Multi-line values need special handling
            self._multi_line_value = True
            self._multi_line_item_name = item
            self._multi_line_value_buffer = []
        else:
            # Parse the line manually to find the exact byte position of the value
            parts = fast_mmcif_split(full_line)
            if len(parts) >= 2:
                # Find the value part in the original line
                # We need to account for quotes and spacing
                item_part = parts[0]
                value_part = parts[1]
                
                # Find where the value starts in the line
                item_end = full_line.find(item_part) + len(item_part)
                # Skip whitespace after item name
                value_start_in_line = item_end
                while value_start_in_line < len(full_line) and full_line[value_start_in_line].isspace():
                    value_start_in_line += 1
                
                # Calculate byte offsets
                value_start_offset = line_start + value_start_in_line
                
                # Handle quoted values
                if value_start_in_line < len(full_line) and full_line[value_start_in_line] in ['"', "'"]:
                    # Find the end of the quoted value
                    quote_char = full_line[value_start_in_line]
                    value_end_in_line = value_start_in_line + 1
                    while value_end_in_line < len(full_line) and full_line[value_end_in_line] != quote_char:
                        if full_line[value_end_in_line] == '\\':
                            value_end_in_line += 2  # Skip escaped character
                        else:
                            value_end_in_line += 1
                    if value_end_in_line < len(full_line):
                        value_end_in_line += 1  # Include closing quote
                else:
                    # Unquoted value - find the end
                    value_end_in_line = value_start_in_line
                    while value_end_in_line < len(full_line) and not full_line[value_end_in_line].isspace():
                        value_end_in_line += 1
                
                value_end_offset = line_start + value_end_in_line
                
                # Store with byte offsets for true lazy loading
                self._current_data._add_item_value(item, value.strip(), value_start_offset, value_end_offset)
            else:
                # Fallback if parsing fails
                self._current_data._add_item_value(item, value.strip())

    def _handle_loop_value_line_with_offsets(self, line: str, line_start: int, line_end: int) -> None:
        """Handle loop value lines with precise byte offset tracking."""
        item_names = [item.split('.', 1)[1] for item in self._loop_items]
        if not self._multi_line_value:
            # Use shlex to properly parse quoted values, but track positions manually
            original_line = line
            values = []
            positions = []
            
            # Manual tokenization to track byte positions
            i = 0
            while i < len(line):
                # Skip whitespace
                while i < len(line) and line[i].isspace():
                    i += 1
                if i >= len(line):
                    break
                    
                start_pos = i
                if line[i] in ['"', "'"]:
                    # Quoted string
                    quote_char = line[i]
                    i += 1
                    while i < len(line) and line[i] != quote_char:
                        if line[i] == '\\':  # Handle escaped characters
                            i += 2
                        else:
                            i += 1
                    if i < len(line):
                        i += 1  # Include closing quote
                    value = line[start_pos:i]
                    # Remove quotes for the actual value
                    actual_value = value[1:-1] if len(value) >= 2 else value
                else:
                    # Unquoted string
                    while i < len(line) and not line[i].isspace():
                        i += 1
                    value = line[start_pos:i]
                    actual_value = value
                
                if value:
                    values.append(actual_value)
                    value_start_offset = line_start + start_pos
                    value_end_offset = line_start + i
                    positions.append((value_start_offset, value_end_offset))
            
            # Process the parsed values with their positions
            value_index = 0
            while len(self._current_row_values) < len(self._loop_items) and value_index < len(values):
                value = values[value_index]
                start_offset, end_offset = positions[value_index]
                
                if value.startswith(';'):
                    self._multi_line_value = True
                    self._multi_line_item_name = item_names[len(self._current_row_values)]
                    self._multi_line_value_buffer.append(value[1:])
                    self._current_row_values.append(None)
                    break
                else:
                    self._current_row_values.append((value, start_offset, end_offset))
                    self._value_counter += 1
                    value_index += 1
                    
            self._maybe_commit_loop_row_with_offsets()
        else:
            if line == ';':
                self._multi_line_value = False
                full_value = "\n".join(self._multi_line_value_buffer)
                self._current_row_values[-1] = (full_value, None, None)
                self._multi_line_value_buffer = []
                self._value_counter += 1
                self._maybe_commit_loop_row_with_offsets()
            else:
                self._multi_line_value_buffer.append(line)

    def _maybe_commit_loop_row_with_offsets(self):
        """Commit loop row with byte offsets for lazy loading."""
        if self._value_counter == len(self._loop_items):
            for i, value_data in enumerate(self._current_row_values):
                item_name = self._loop_items[i].split('.', 1)[1]
                if isinstance(value_data, tuple) and len(value_data) == 3:
                    value, start_offset, end_offset = value_data
                    self._current_data._add_item_value(item_name, value, start_offset, end_offset)
                else:
                    # Fallback for non-tuple values
                    self._current_data._add_item_value(item_name, str(value_data))
            self._current_row_values = []
            self._value_counter = 0

    def close(self) -> None:
        """Close the memory-mapped file."""
        if self._mmap_obj:
            self._mmap_obj.close()
            self._mmap_obj = None

    def _process_line(self, line: str) -> None:
        if line.startswith('#'):
            return
        elif line.startswith('data_'):
            self._handle_data_block(line)
        elif line.startswith('loop_'):
            self._start_loop()
        elif line.startswith('_'):
            self._handle_item_line(line)
        elif self._in_loop:
            self._handle_loop_value_line(line)
        elif self._multi_line_value:
            self._handle_non_loop_multiline(line)

    def _handle_data_block(self, line: str):
        self._current_block = line.split('_', 1)[1]
        self._data_blocks[self._current_block] = DataBlock(self._current_block, {})
        self._current_category = None
        self._in_loop = False

    def _start_loop(self):
        self._in_loop = True
        self._loop_items = []

    def _handle_item_line(self, line: str):
        parts = shlex.split(line)
        if len(parts) == 2:
            self._handle_simple_item(*parts)
        else:
            self._handle_loop_item(parts[0], line[len(parts[0]):].strip())

    def _handle_simple_item(self, item_full: str, value: str):
        category, item = item_full.split('.', 1)
        if not self._should_include_category(category):
            return
        self._ensure_current_data(category)
        if value.startswith(';'):
            self._multi_line_value = True
            self._multi_line_item_name = item
            self._multi_line_value_buffer = []
        else:
            self._current_data._add_item_value(item, value.strip())

    def _handle_loop_item(self, item_full: str, value: str):
        # Handle malformed item names gracefully
        if '.' not in item_full:
            return  # Skip malformed items
        category, item = item_full.split('.', 1)
        if not self._should_include_category(category):
            return
        if self._in_loop:
            self._loop_items.append(item_full)
            self._ensure_current_data(category)
        else:
            self._ensure_current_data(category)
            self._current_data._add_item_value(item, value)

    def _handle_loop_value_line(self, line: str):
        item_names = [item.split('.', 1)[1] for item in self._loop_items]
        if not self._multi_line_value:
            values = shlex.split(line)
            while len(self._current_row_values) < len(self._loop_items) and values:
                value = values.pop(0)
                if value.startswith(';'):
                    self._multi_line_value = True
                    self._multi_line_item_name = item_names[len(self._current_row_values)]
                    self._multi_line_value_buffer.append(value[1:])
                    self._current_row_values.append(None)
                    break
                else:
                    self._current_row_values.append(value)
                    self._value_counter += 1
            self._maybe_commit_loop_row()
        else:
            if line == ';':
                self._multi_line_value = False
                full_value = "\n".join(self._multi_line_value_buffer)
                self._current_row_values[-1] = full_value
                self._multi_line_value_buffer = []
                self._value_counter += 1
                self._maybe_commit_loop_row()
            else:
                self._multi_line_value_buffer.append(line)

    def _maybe_commit_loop_row(self):
        """Commit loop row (fallback method for non-offset parsing)."""
        if self._value_counter == len(self._loop_items):
            for i, val in enumerate(self._current_row_values):
                item_name = self._loop_items[i].split('.', 1)[1]
                self._current_data._add_item_value(item_name, val)
            self._current_row_values = []
            self._value_counter = 0

    def _handle_non_loop_multiline(self, line: str):
        if line == ';':
            self._multi_line_value = False
            full_value = "\n".join(self._multi_line_value_buffer)
            self._current_data._add_item_value(self._multi_line_item_name, full_value)
            self._multi_line_value_buffer = []
        else:
            self._multi_line_value_buffer.append(line)

    def _should_include_category(self, category: str) -> bool:
        return not self.categories or category in self.categories

    def _ensure_current_data(self, category: str):
        if self._current_category != category:
            self._current_category = category
            if category not in self._data_blocks[self._current_block]._categories:
                self._data_blocks[self._current_block]._categories[category] = Category(
                    category, self.validator_factory)
            self._current_data = self._data_blocks[self._current_block]._categories[category]

    def _handle_item_line_simple(self, line: str) -> None:
        """Handle item lines for small files without offset tracking."""
        parts = fast_mmcif_split(line)
        if len(parts) == 2:
            self._handle_simple_item_simple(parts[0], parts[1])
        else:
            self._handle_loop_item(parts[0], line[len(parts[0]):].strip())

    def _handle_simple_item_simple(self, item_full: str, value: str) -> None:
        """Handle simple items for small files without memory mapping."""
        category, item = item_full.split('.', 1)
        if not self._should_include_category(category):
            return
        self._ensure_current_data_simple(category)
        
        if value.startswith(';'):
            self._multi_line_value = True
            self._multi_line_item_name = item
            self._multi_line_value_buffer = []
        else:
            # Direct value storage without offsets
            self._current_data._add_item_value_simple(item, value.strip())

    def _handle_loop_value_line_simple(self, line: str) -> None:
        """Optimized loop value line handling for small files."""
        if not self._multi_line_value:
            # Fast tokenization optimized for common cases
            values = self._fast_tokenize_loop_line(line)
            
            # Process values in batch
            num_items = len(self._loop_items)
            values_processed = 0
            
            for value in values:
                if len(self._current_row_values) >= num_items:
                    break
                    
                if value.startswith(';'):
                    # Handle multi-line value
                    item_idx = len(self._current_row_values)
                    self._multi_line_value = True
                    self._multi_line_item_name = self._loop_items[item_idx].split('.', 1)[1]
                    self._multi_line_value_buffer.append(value[1:])
                    self._current_row_values.append(None)
                    break
                else:
                    self._current_row_values.append(value)
                    values_processed += 1
                        
            self._value_counter += values_processed
            self._maybe_commit_loop_row_simple()
        else:
            if line == ';':
                self._multi_line_value = False
                full_value = "\n".join(self._multi_line_value_buffer)
                self._current_row_values[-1] = full_value
                self._multi_line_value_buffer = []
                self._value_counter += 1
                self._maybe_commit_loop_row_simple()
            else:
                self._multi_line_value_buffer.append(line)
    
    def _fast_tokenize_loop_line(self, line: str) -> List[str]:
        """Ultra-fast tokenization optimized for loop value lines."""
        # Fast path: no quotes means simple split works
        if '"' not in line and "'" not in line:
            return line.split()
        
        # Complex path: manual tokenization with quote handling
        tokens = []
        i = 0
        line_len = len(line)
        
        while i < line_len:
            # Skip whitespace
            while i < line_len and line[i].isspace():
                i += 1
            if i >= line_len:
                break
                
            start = i
            if line[i] in ['"', "'"]:
                # Quoted token
                quote = line[i]
                i += 1
                while i < line_len and line[i] != quote:
                    if line[i] == '\\':
                        i += 2  # Skip escaped char
                    else:
                        i += 1
                if i < line_len:
                    i += 1  # Skip closing quote
                # Extract without quotes
                if i > start + 1:
                    tokens.append(line[start + 1:i - 1])
            else:
                # Unquoted token
                while i < line_len and not line[i].isspace():
                    i += 1
                tokens.append(line[start:i])
        
        return tokens

    def _maybe_commit_loop_row_simple(self):
        """Optimized loop row commit for small files."""
        if self._value_counter == len(self._loop_items):
            # Batch process all values for this row at once
            item_names = [item.split('.', 1)[1] for item in self._loop_items]
            
            for i, value in enumerate(self._current_row_values):
                if value is not None:
                    self._current_data._add_item_value_simple(item_names[i], value)
            
            # Reset row state (reuse lists instead of creating new ones)
            self._current_row_values.clear()
            self._value_counter = 0

    def _ensure_current_data_simple(self, category: str):
        """Ensure current data for small files without memory mapping."""
        if self._current_category != category:
            self._current_category = category
            if category not in self._data_blocks[self._current_block]._categories:
                self._data_blocks[self._current_block]._categories[category] = Category(
                    category, self.validator_factory, None)  # No mmap object
            self._current_data = self._data_blocks[self._current_block]._categories[category]
