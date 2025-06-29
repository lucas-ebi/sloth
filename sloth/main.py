from typing import Callable, Dict, Tuple, List, Any, Union, Optional, IO, Iterator
import io
import shlex
import mmap
import os
from functools import cached_property


class ValidatorFactory:
    """A factory class for creating validators and cross-checkers."""
    def __init__(self):
        self.validators: Dict[str, Callable[[str], None]] = {}
        self.cross_checkers: Dict[Tuple[str, str], Callable[[str, str], None]] = {}

    def register_validator(self, category_name: str, validator_function: Callable[[str], None]) -> None:
        """
        Registers a validator function for a category.

        :param category_name: The name of the category.
        :type category_name: str
        :param validator_function: The validator function.
        :type validator_function: Callable[[str], None]
        :return: None
        """
        self.validators[category_name] = validator_function

    def register_cross_checker(self, category_pair: Tuple[str, str], cross_checker_function: Callable[[str, str], None]) -> None:
        """
        Registers a cross-checker function for a pair of categories.

        :param category_pair: The pair of category names.
        :type category_pair: Tuple[str, str]
        :param cross_checker_function: The cross-checker function.
        :type cross_checker_function: Callable[[str, str], None]
        :return: None
        """
        self.cross_checkers[category_pair] = cross_checker_function

    def get_validator(self, category_name: str) -> Optional[Callable[[str], None]]:
        """
        Retrieves a validator function for a category.

        :param category_name: The name of the category.
        :type category_name: str
        :return: The validator function.
        :rtype: Optional[Callable[[str], None]]
        """
        return self.validators.get(category_name)

    def get_cross_checker(self, category_pair: Tuple[str, str]) -> Optional[Callable[[str, str], None]]:
        """
        Retrieves a cross-checker function for a pair of categories.

        :param category_pair: The pair of category names.
        :type category_pair: Tuple[str, str]
        :return: The cross-checker function.
        :rtype: Optional[Callable[[str, str], None]]
        """
        return self.cross_checkers.get(category_pair)


class Item:
    """A lazy-loaded item that uses memory mapping for efficient access to large files."""
    
    def __init__(self, name: str, mmap_obj: Optional[mmap.mmap] = None, 
                 value_offsets: Optional[List[Tuple[int, int]]] = None,
                 eager_values: Optional[List[str]] = None):
        """
        Initialize an Item with either memory-mapped offsets or eager values.
        
        :param name: The name of the item
        :param mmap_obj: Memory-mapped file object
        :param value_offsets: List of (start, end) byte offsets for values in the mmap
        :param eager_values: Pre-loaded values (fallback for small datasets)
        """
        self._name = name
        self._mmap_obj = mmap_obj
        self._value_offsets = value_offsets or []
        self._eager_values = eager_values
        self._cached_values: Optional[List[str]] = None

    @property
    def name(self) -> str:
        """Read-only access to the item name."""
        return self._name

    @cached_property
    def values(self) -> List[str]:
        """Lazy-loaded values with caching."""
        if self._cached_values is not None:
            return self._cached_values
            
        if self._eager_values is not None:
            self._cached_values = self._eager_values
            return self._cached_values
            
        if self._mmap_obj is None or not self._value_offsets:
            self._cached_values = []
            return self._cached_values
            
        # Memory-mapped lazy loading
        self._cached_values = []
        for start_offset, end_offset in self._value_offsets:
            try:
                # Extract bytes from memory map
                value_bytes = self._mmap_obj[start_offset:end_offset]
                value = value_bytes.decode('utf-8').strip()
                self._cached_values.append(value)
            except (IndexError, UnicodeDecodeError) as e:
                # Fallback for malformed data
                self._cached_values.append("")
                
        return self._cached_values

    def add_offset(self, start: int, end: int) -> None:
        """Add a new value offset for memory-mapped access."""
        self._value_offsets.append((start, end))
        # Clear cache when new offsets are added
        self._cached_values = None

    def add_eager_value(self, value: str) -> None:
        """Add a value directly (for small datasets or immediate loading)."""
        if self._eager_values is None:
            self._eager_values = []
        self._eager_values.append(value)
        # Clear cache when new values are added
        self._cached_values = None

    def __iter__(self):
        """Iterate over values (triggers lazy loading)."""
        return iter(self.values)

    def __len__(self):
        """Get the number of values."""
        if self._eager_values is not None:
            return len(self._eager_values)
        return len(self._value_offsets)

    def __getitem__(self, index: Union[int, slice]) -> Union[str, List[str]]:
        """Get value(s) by index (triggers lazy loading)."""
        return self.values[index]

    def __repr__(self):
        return f"Item(name='{self.name}', length={len(self)}, loaded={self._cached_values is not None})"


class Category:
    """A class to represent a category in a data block."""
    def __init__(self, name: str, validator_factory: Optional[ValidatorFactory], 
                 mmap_obj: Optional[mmap.mmap] = None):
        self.name: str = name
        self._items: Dict[str, Union[List[str], Item]] = {}
        self._validator_factory: Optional[ValidatorFactory] = validator_factory
        self._mmap_obj = mmap_obj

    def __setattr__(self, name: str, value: Any) -> None:
        if name in ('name', '_items', '_validator_factory', '_mmap_obj'):
            super().__setattr__(name, value)
        else:
            self._items[name] = value

    def __getattr__(self, item_name: str) -> Union[List[str], Item]:
        if item_name in self._items:
            item = self._items[item_name]
            # Return values for Item objects, the Item itself for direct access
            if isinstance(item, Item):
                return item.values
            return item
        elif item_name == 'validate':
            return self._create_validator()
        else:
            raise AttributeError(f"'Category' object has no attribute '{item_name}'")

    def _create_validator(self):
        return self.Validator(self, self._validator_factory)

    def __getitem__(self, key: Union[str, int, slice]) -> Union[List[str], 'Row', List['Row']]:
        """
        Access values by item name or row index/slice.
        
        If key is a string, return all values for that item (column-wise access).
        If key is an integer or slice, return Row(s) (row-wise access).
        """
        if isinstance(key, str):
            # Existing behavior - column access by item name
            item = self._items[key]
            if isinstance(item, Item):
                return item.values
            return item
        elif isinstance(key, int):
            # New behavior - row access by index
            row_count = self.row_count
            
            if row_count == 0:
                raise IndexError("Cannot access rows in empty category")
                
            # Handle negative indices
            if key < 0:
                key = row_count + key
                
            if key < 0 or key >= row_count:
                raise IndexError(f"Row index {key} is out of range (0-{row_count-1})")
                
            return Row(self, key)
        elif isinstance(key, slice):
            # New behavior - multiple rows access by slice
            row_count = self.row_count
            
            if row_count == 0:
                return []
                
            # Get the indices from the slice
            indices = range(*key.indices(row_count))
            return [Row(self, i) for i in indices]
        else:
            raise TypeError(f"Category indices must be strings, integers or slices, not {type(key).__name__}")

    def __setitem__(self, item_name: str, value: Union[List[str], Item]) -> None:
        self._items[item_name] = value

    def __iter__(self):
        return iter(self._items.items())

    def __len__(self):
        return len(self._items)

    def __repr__(self):
        # Limit the output to avoid long print statements
        return f"Category(name={self.name}, items={list(self._items.keys())})"

    @property
    def items(self) -> List[str]:
        """Provides a list of item names."""
        return list(self._items.keys())

    @property
    def data(self) -> Dict[str, List[str]]:
        """Provides read-only access to the data (forces loading of lazy items)."""
        result = {}
        for name, item in self._items.items():
            if isinstance(item, Item):
                result[name] = item.values
            else:
                result[name] = item
        return result

    @property
    def row_count(self) -> int:
        """Returns the number of rows in this category."""
        if not self._items:
            return 0
        
        # Get the length of the first item to determine row count
        any_item = next(iter(self._items.values()))
        if isinstance(any_item, Item):
            return len(any_item)
        return len(any_item)
        
    @property
    def rows(self) -> List['Row']:
        """Returns all rows in this category."""
        return self[:] if self.row_count > 0 else []

    def get_item(self, item_name: str) -> Union[Item, List[str]]:
        """Get the raw item (Item object or list), without forcing lazy loading."""
        return self._items[item_name]

    def is_lazy_loaded(self, item_name: str) -> bool:
        """Check if an item is lazy-loaded."""
        return isinstance(self._items.get(item_name), Item)

    def _add_item_value(self, item_name: str, value: str, 
                       start_offset: Optional[int] = None, 
                       end_offset: Optional[int] = None) -> None:
        """Adds a value to the list of values for the given item name using memory mapping."""
        if item_name not in self._items:
            self._items[item_name] = Item(item_name, self._mmap_obj)
        
        if isinstance(self._items[item_name], Item):
            if self._mmap_obj is not None and start_offset is not None and end_offset is not None:
                # Memory-mapped lazy loading with byte offsets
                self._items[item_name].add_offset(start_offset, end_offset)
            else:
                # Fallback to eager loading if offsets not available
                self._items[item_name].add_eager_value(value)
        else:
            # Convert existing list to Item
            existing_values = self._items[item_name] if isinstance(self._items[item_name], list) else []
            item = Item(item_name, self._mmap_obj, eager_values=existing_values)
            if self._mmap_obj is not None and start_offset is not None and end_offset is not None:
                item.add_offset(start_offset, end_offset)
            else:
                item.add_eager_value(value)
            self._items[item_name] = item


    class Validator:
        """A class to validate a category."""
        def __init__(self, category: 'Category', factory: ValidatorFactory):
            self._category: 'Category' = category
            self._factory: ValidatorFactory = factory
            self._other_category: Optional['Category'] = None
        
        def __call__(self) -> 'Category.Validator':
            validator = self._factory.get_validator(self._category.name)
            if validator:
                validator(self._category.name)
            else:
                print(f"No validator registered for category '{self._category.name}'")
            return self
        
        def against(self, other_category: 'Category') -> 'Category.Validator':
            """
            Cross-checks the current category against another category.

            :param other_category: The other category to cross-check against.
            :type other_category: Category
            :return: The validator object.
            :rtype: Category.Validator
            """
            self._other_category = other_category
            cross_checker = self._factory.get_cross_checker((self._category.name, other_category.name))
            if cross_checker:
                cross_checker(self._category.name, other_category.name)
            else:
                print(f"No cross-checker registered for categories '{self._category.name}' and '{other_category.name}'")
            return self


class DataBlock:
    """A class to represent a data block in an mmCIF file."""
    def __init__(self, name: str, categories: Dict[str, Category]):
        self.name = name
        self._categories = categories

    def __getitem__(self, category_name: str) -> Category:
        return self._categories[category_name]

    def __setitem__(self, category_name: str, category: Category) -> None:
        self._categories[category_name] = category

    def __getattr__(self, category_name: str) -> Category:
        try:
            return self._categories[category_name]
        except KeyError:
            raise AttributeError(f"'DataBlock' object has no attribute '{category_name}'")

    def __iter__(self):
        return iter(self._categories.values())

    def __len__(self):
        return len(self._categories)

    def __repr__(self):
        return f"DataBlock(name={self.name}, categories={self._categories})"

    @property
    def categories(self) -> List[str]:
        """Provides a list of category names in the data block."""
        return list(self._categories.keys())

    @property
    def data(self) -> Dict[str, Category]:
        """Provides read-only access to the category objects."""
        return self._categories


class MMCIFDataContainer:
    """A class to represent an mmCIF data container."""
    def __init__(self, data_blocks: Dict[str, DataBlock]):
        self._data_blocks = data_blocks

    def __getitem__(self, block_name: str) -> DataBlock:
        return self._data_blocks[block_name]

    def __setitem__(self, block_name: str, block: DataBlock) -> None:
        self._data_blocks[block_name] = block

    def __getattr__(self, block_name: str) -> DataBlock:
        if block_name.startswith("data_"):
            block_name = block_name[5:]  # Remove the 'data_' prefix
        if block_name in self._data_blocks:
            return self._data_blocks[block_name]
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{block_name}'")

    def __iter__(self):
        return iter(self._data_blocks.values())

    def __len__(self):
        return len(self._data_blocks)

    def __repr__(self):
        return f"MMCIFDataContainer(data_blocks={self._data_blocks})"

    @property
    def data(self) ->  List[DataBlock]:
        """Provides read-only access to the data blocks."""
        return list(self._data_blocks.values())

    @property
    def blocks(self) -> List[str]:
        """Provides a list of data block names."""
        return list(self._data_blocks.keys())


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
        """Parse using memory mapping with true lazy loading."""
        # Check file size first
        file_size = os.path.getsize(file_path)
        
        # Handle empty files - can't memory map empty files
        if file_size == 0:
            return MMCIFDataContainer({})
        
        # Always use memory mapping for consistent behavior
        with open(file_path, 'rb') as f:
            # Create memory map
            self._mmap_obj = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            
            # Parse directly from memory map
            return self._parse_from_mmap()
    
    def _parse_from_mmap(self) -> MMCIFDataContainer:
        """Parse directly from memory map using byte offsets for true lazy loading."""
        # Convert mmap to string for line-by-line processing
        # We still need to process line by line to understand the structure
        content = self._mmap_obj[:].decode('utf-8')
        lines = content.split('\n')
        
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
                
            return MMCIFDataContainer(self._data_blocks)
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
        parts = shlex.split(line)
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
            parts = shlex.split(full_line)
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


class Row:
    """Represents a single row of data in a Category."""
    
    def __init__(self, category: 'Category', row_index: int):
        self._category = category
        self._row_index = row_index
        
    def __getattr__(self, item_name: str) -> str:
        """Allow dot notation access to item values in this row."""
        if item_name in self._category._items:
            values = self._category[item_name]
            if self._row_index < len(values):
                return values[self._row_index]
            raise IndexError(f"Row index {self._row_index} is out of range")
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item_name}'")
    
    def __getitem__(self, item_name: str) -> str:
        """Allow dictionary-style access to item values in this row."""
        if item_name in self._category._items:
            values = self._category[item_name]
            if self._row_index < len(values):
                return values[self._row_index]
            raise KeyError(f"Item '{item_name}' at index {self._row_index} not found")
        raise KeyError(item_name)
    
    def __repr__(self):
        return f"Row({self._row_index}, {self._category.name})"
    
    @property
    def data(self) -> Dict[str, str]:
        """Return all item values for this row as a dictionary."""
        result = {}
        for item_name in self._category.items:
            values = self._category[item_name]
            if self._row_index < len(values):
                result[item_name] = values[self._row_index]
        return result


class MMCIFWriter:
    """A class to write an mmCIF data container to a file."""
    def write(self, file_obj: IO, data_container: MMCIFDataContainer) -> None:
        try:
            for data_block in data_container:
                file_obj.write(f"data_{data_block.name}\n")
                file_obj.write("#\n")
                for category_name in data_block.categories:
                    category = data_block.data[category_name]
                    if isinstance(category, Category):
                        self._write_category(file_obj, category_name, category)
                        file_obj.write("#\n")
        except IOError as e:
            print(f"Error writing to file: {e}")

    def _write_category(self, file_obj: IO, category_name: str, category: Category) -> None:
        """
        Writes a category to a file.

        :param file_obj: The file object to write to.
        :type file_obj: IO
        :param category_name: The name of the category.
        :type category_name: str
        :param category: The category to write.
        :type category: Category
        :return: None
        """
        # Get all data (this will force loading of lazy items)
        items = category.data
        
        if any(len(values) > 1 for values in items.values()):
            file_obj.write("loop_\n")
            for item_name in items.keys():
                file_obj.write(f"{category_name}.{item_name}\n")
            for row in zip(*items.values()):
                formatted_row = [self._format_value(value) for value in row]
                file_obj.write(f"{''.join(formatted_row)}\n".replace('\n\n', '\n'))
        else:
            for item_name, values in items.items():
                for value in values:
                    formatted_value = self._format_value(value)
                    file_obj.write(f"{category_name}.{item_name} {formatted_value}\n")

    @staticmethod
    def _format_value(value: str) -> str:
        """
        Formats a value for writing to a file.

        :param value: The value to format.
        :type value: str
        :return: The formatted value.
        :rtype: str
        """
        if '\n' in value or value.startswith(' ') or value.startswith(';'):
            return f"\n;{value.strip()}\n;\n"
        if ' ' in value or value.startswith('_') or value.startswith("'") or value.startswith('"'):
            return f"'{value}' "
        return f"{value} "


class MMCIFHandler:
    """A class to handle reading and writing mmCIF files with efficient memory mapping and lazy loading."""
    
    def __init__(self, validator_factory: Optional[ValidatorFactory] = None):
        """
        Initialize the handler with memory mapping and lazy loading always enabled.
        
        :param validator_factory: Optional validator factory for data validation
        """
        self.validator_factory = validator_factory
        self._parser = None
        self._writer = None

    def parse(self, filename: str, categories: Optional[List[str]] = None) -> MMCIFDataContainer:
        """
        Parses an mmCIF file and returns a data container using memory mapping and lazy loading.

        :param filename: The name of the file to parse.
        :type filename: str
        :param categories: The categories to parse. If None, all categories are included.
        :type categories: Optional[List[str]]
        :return: The data container with lazy-loaded items.
        :rtype: MMCIFDataContainer
        """
        self._parser = MMCIFParser(self.validator_factory, categories)
        return self._parser.parse_file(filename)

    def write(self, data_container: MMCIFDataContainer) -> None:
        """
        Writes a data container to a file.

        :param data_container: The data container to write.
        :type data_container: MMCIFDataContainer
        :return: None
        """
        if hasattr(self, '_file_obj') and self._file_obj:
            self._writer = MMCIFWriter()
            self._writer.write(self._file_obj, data_container)
        else:
            raise IOError("File is not open for writing")

    @property
    def file_obj(self):
        """Provides access to the file object."""
        return self._file_obj

    @file_obj.setter
    def file_obj(self, file_obj):
        """Sets the file object."""
        self._file_obj = file_obj
