from typing import Callable, Dict, Tuple, List, Any, Union, Optional, IO, Iterator, Protocol, TypeVar, runtime_checkable, Type
from functools import cached_property
import os
import mmap
import shlex
import json
import pickle
from enum import Enum, auto
import xml.etree.ElementTree as ET
import glob
from abc import ABC, abstractmethod


# Forward reference for schema validation
SchemaValidator = Union[Any]  # Will be imported where needed


class DataSourceFormat(Enum):
    """Enum to track the format source of mmCIF data."""
    MMCIF = auto()     # Native mmCIF file
    JSON = auto()      # JSON file or string
    XML = auto()       # XML file or string
    PICKLE = auto()    # Pickle file
    YAML = auto()      # YAML file or string
    CSV = auto()       # CSV directory
    DICT = auto()      # Python dictionary
    UNKNOWN = auto()   # Unknown source


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


class DataNode(ABC):
    """Abstract base class for all data nodes in the hierarchy."""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Get the name of the node."""
        pass
    
    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name})"

class DataContainer(DataNode):
    """Abstract base class for containers that hold other nodes."""
    
    @abstractmethod
    def __getitem__(self, key: str):
        pass
    
    @abstractmethod
    def __iter__(self):
        pass
    
    @abstractmethod
    def __len__(self):
        pass


class DataValueLoader(ABC):
    """Abstract base for value loading strategies."""
    
    @abstractmethod
    def load_values(self) -> List[str]:
        pass

class MMapValueLoader(DataValueLoader):
    """Loads values from memory-mapped file."""
    
    def __init__(self, mmap_obj: mmap.mmap, value_offsets: List[Tuple[int, int]]):
        self._mmap_obj = mmap_obj
        self._value_offsets = value_offsets
    
    def load_values(self) -> List[str]:
        values = []
        for start_offset, end_offset in self._value_offsets:
            try:
                value_bytes = self._mmap_obj[start_offset:end_offset]
                values.append(value_bytes.decode('utf-8').strip())
            except (IndexError, UnicodeDecodeError):
                values.append("")
        return values

class EagerValueLoader(DataValueLoader):
    """Holds pre-loaded values."""
    
    def __init__(self, values: List[str]):
        self._values = values
    
    def load_values(self) -> List[str]:
        return self._values.copy()

class EmptyValueLoader(DataValueLoader):
    """Fallback for empty items."""
    
    def load_values(self) -> List[str]:
        return []


class Item(DataNode):
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


class Row(DataNode):
    """Represents a single row of data in a Category."""
    
    def __init__(self, category: 'Category', row_index: int):
        self._category = category
        self._row_index = row_index
        
    @property
    def name(self) -> str:
        """Return name from the first item value in the row if available, otherwise the row index."""
        if len(self._category.items) > 0:
            first_item = self._category.items[0]
            try:
                return self._category[first_item][self._row_index]
            except (IndexError, KeyError):
                pass
        return str(self._row_index)
    
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
    
    @property
    def data(self) -> Dict[str, str]:
        """Return all item values for this row as a dictionary."""
        result = {}
        for item_name in self._category.items:
            values = self._category[item_name]
            if self._row_index < len(values):
                result[item_name] = values[self._row_index]
        return result

    def __repr__(self):
        return f"Row({self._row_index}, {self._category.name})"


class Category(DataContainer):
    """A class to represent a category in a data block."""
    
    class Validator:
        """A class to validate a category."""
        def __init__(self, category: 'Category', factory: 'ValidatorFactory'):
            self._category = category
            self._factory = factory
            self._other_category: Optional['Category'] = None
        
        def __call__(self) -> 'Category.Validator':
            validator = self._factory.get_validator(self._category.name)
            if validator:
                validator(self._category.name)
            return self
        
        def against(self, other_category: 'Category') -> 'Category.Validator':
            self._other_category = other_category
            cross_checker = self._factory.get_cross_checker(
                (self._category.name, other_category.name))
            if cross_checker:
                cross_checker(self._category.name, other_category.name)
            return self
            
    def __init__(self, name: str, validator_factory: Optional['ValidatorFactory'] = None,
                 mmap_obj: Optional[mmap.mmap] = None):
        self._name = name
        self._items: Dict[str, Union[List[str], Item]] = {}
        self._validator_factory = validator_factory
        self._mmap_obj = mmap_obj

    @property
    def name(self) -> str:
        return self._name
        
    @property
    def validator_factory(self) -> Optional['ValidatorFactory']:
        return self._validator_factory

    @property
    def items(self) -> List[str]:
        """Get names of contained items."""
        return list(self._items.keys())

    def __getattr__(self, item_name: str) -> Union[List[str], Item]:
        if item_name in self._items:
            item = self._items[item_name]
            # Return values for Item objects, the Item itself for direct access
            if isinstance(item, Item):
                return item.values
            return item
        elif item_name == 'validate':
            return self.Validator(self, self._validator_factory)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item_name}'")

    def __getitem__(self, key: Union[str, int, slice]) -> Union[List[str], 'Row', List['Row']]:
        """
        Access values by item name or row index/slice.
        
        If key is a string, return all values for that item (column-wise access).
        If key is an integer or slice, return Row(s) (row-wise access).
        """
        if isinstance(key, str):
            # Column access by item name
            item = self._items[key]
            return item.values if isinstance(item, Item) else item
        elif isinstance(key, int):
            # Row access by index
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
            # Multiple rows access by slice
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
        return f"Category(name={self.name}, items={list(self._items.keys())})"

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


class DataBlock(DataContainer):
    """A class to represent a data block in an mmCIF file."""
    
    def __init__(self, name: str, categories: Dict[str, Category] = None):
        self._name = name
        self._categories = categories if categories is not None else {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def categories(self) -> List[str]:
        """Get names of contained categories."""
        return list(self._categories.keys())

    @property
    def data(self) -> Dict[str, Category]:
        """Provides read-only access to the category objects."""
        return self._categories

    def __getitem__(self, category_name: str) -> Category:
        return self._categories[category_name]

    def __setitem__(self, category_name: str, category: Category) -> None:
        self._categories[category_name] = category

    def __getattr__(self, category_name: str) -> Category:
        try:
            return self._categories[category_name]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{category_name}'")

    def __iter__(self):
        return iter(self._categories.values())

    def __len__(self):
        return len(self._categories)

    def __repr__(self):
        return f"DataBlock(name={self.name}, categories={list(self.categories)})"


class MMCIFDataContainer(DataContainer):
    """A class to represent an mmCIF data container."""
    
    def __init__(self, data_blocks: Dict[str, DataBlock] = None, source_format: DataSourceFormat = DataSourceFormat.MMCIF):
        self._data_blocks = data_blocks if data_blocks is not None else {}
        self.source_format = source_format

    @property
    def name(self) -> str:
        return f"MMCIFDataContainer({len(self)} blocks)"

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
        return f"MMCIFDataContainer({len(self)} blocks)"

    @property
    def blocks(self) -> List[str]:
        """Provides a list of data block names."""
        return list(self._data_blocks.keys())

    @property
    def data(self) -> List[DataBlock]:
        """Provides read-only access to the data blocks."""
        return list(self._data_blocks.values())


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


class MMCIFWriter:
    """A class to write an mmCIF data container to a file."""
    def write(self, file_obj: IO, mmcif_data_container: MMCIFDataContainer) -> None:
        try:
            for data_block in mmcif_data_container:
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


class MMCIFExporter:
    """A class to export mmCIF data to different formats like JSON, XML, Pickle, YAML, etc."""
    
    def __init__(self, mmcif_data_container: MMCIFDataContainer):
        """
        Initialize the exporter with an mmCIF data container.
        
        :param mmcif_data_container: The mmCIF data container to export
        :type mmcif_data_container: MMCIFDataContainer
        """
        self.mmcif_data_container = mmcif_data_container
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the mmCIF data container to a dictionary structure.
        
        :return: A dictionary representation of the mmCIF data
        :rtype: Dict[str, Any]
        """
        result = {}
        
        for block in self.mmcif_data_container:
            block_dict = {}
            
            for category_name in block.categories:
                category = block[category_name]
                category_dict = {}
                
                # Get all data (this will force loading of lazy items)
                items = category.data
                
                # Check if we have multiple rows
                if any(len(values) > 1 for values in items.values()):
                    # For multi-row categories, create a list of row objects
                    rows = []
                    for i in range(category.row_count):
                        row = {}
                        for item_name, values in items.items():
                            if i < len(values):
                                row[item_name] = values[i]
                        rows.append(row)
                    category_dict = rows
                else:
                    # For single-row categories, create a simple key-value object
                    for item_name, values in items.items():
                        if values:  # Check if there are any values
                            category_dict[item_name] = values[0]
                
                block_dict[category_name] = category_dict
            
            result[block.name] = block_dict
            
        return result
    
    def to_json(self, file_path: Optional[str] = None, indent: int = 2) -> Optional[str]:
        """
        Export mmCIF data to JSON format.
        
        :param file_path: Path to save the JSON file (optional)
        :type file_path: Optional[str]
        :param indent: Number of spaces for indentation
        :type indent: int
        :return: JSON string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        import json
        data_dict = self.to_dict()
        
        if file_path:
            with open(file_path, 'w') as f:
                json.dump(data_dict, f, indent=indent)
            return None
        else:
            return json.dumps(data_dict, indent=indent)
    
    def to_xml(self, file_path: Optional[str] = None, pretty_print: bool = True) -> Optional[str]:
        """
        Export mmCIF data to XML format.
        
        :param file_path: Path to save the XML file (optional)
        :type file_path: Optional[str]
        :param pretty_print: Whether to format XML with indentation
        :type pretty_print: bool
        :return: XML string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        from xml.dom import minidom
        from xml.etree import ElementTree as ET
        
        root = ET.Element("mmcif_data")
        
        for block in self.mmcif_data_container:
            block_elem = ET.SubElement(root, "data_block", name=block.name)
            
            for category_name in block.categories:
                category = block[category_name]
                category_elem = ET.SubElement(block_elem, "category", name=category_name)
                
                # Get all data (this will force loading of lazy items)
                items = category.data
                
                if any(len(values) > 1 for values in items.values()):
                    # For multi-row categories
                    for i in range(category.row_count):
                        row_elem = ET.SubElement(category_elem, "row", index=str(i))
                        for item_name, values in items.items():
                            if i < len(values):
                                item_elem = ET.SubElement(row_elem, "item", name=item_name)
                                item_elem.text = values[i]
                else:
                    # For single-row categories
                    for item_name, values in items.items():
                        if values:  # Check if there are any values
                            item_elem = ET.SubElement(category_elem, "item", name=item_name)
                            item_elem.text = values[0]
        
        # Convert to string
        rough_string = ET.tostring(root, 'utf-8')
        
        if pretty_print:
            reparsed = minidom.parseString(rough_string)
            xml_string = reparsed.toprettyxml(indent="  ")
        else:
            xml_string = rough_string.decode('utf-8')
        
        if file_path:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(xml_string)
            return None
        else:
            return xml_string
    
    def to_pickle(self, file_path: str) -> None:
        """
        Export mmCIF data to a Python pickle file.
        
        :param file_path: Path to save the pickle file
        :type file_path: str
        :return: None
        """
        import pickle
        data_dict = self.to_dict()
        
        with open(file_path, 'wb') as f:
            pickle.dump(data_dict, f)
    
    def to_yaml(self, file_path: Optional[str] = None) -> Optional[str]:
        """
        Export mmCIF data to YAML format.
        
        :param file_path: Path to save the YAML file (optional)
        :type file_path: Optional[str]
        :return: YAML string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        # Using PyYAML package
        try:
            import yaml
        except ImportError:
            raise ImportError("PyYAML package is required for YAML export. Install it using 'pip install pyyaml'.")
        
        data_dict = self.to_dict()
        
        if file_path:
            with open(file_path, 'w') as f:
                yaml.dump(data_dict, f, default_flow_style=False)
            return None
        else:
            return yaml.dump(data_dict, default_flow_style=False)
    
    def to_pandas(self) -> Dict[str, Dict[str, Any]]:
        """
        Export mmCIF data to pandas DataFrames, with one DataFrame per category.
        
        :return: Dictionary of DataFrames organized by data block and category
        :rtype: Dict[str, Dict[str, Any]]
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas package is required for DataFrame export. Install it using 'pip install pandas'.")
        
        result = {}
        
        for block in self.mmcif_data_container:
            block_dict = {}
            
            for category_name in block.categories:
                category = block[category_name]
                
                # Get all data (this will force loading of lazy items)
                items = category.data
                
                # Create DataFrame from items
                df = pd.DataFrame(items)
                block_dict[category_name] = df
            
            result[block.name] = block_dict
            
        return result
    
    def to_csv(self, directory_path: str, prefix: str = "") -> Dict[str, Dict[str, str]]:
        """
        Export mmCIF data to CSV files, with one file per category.
        
        :param directory_path: Directory to save the CSV files
        :type directory_path: str
        :param prefix: Prefix for CSV filenames
        :type prefix: str
        :return: Dictionary mapping block and category names to file paths
        :rtype: Dict[str, Dict[str, str]]
        """
        try:
            import pandas as pd
            import os
        except ImportError:
            raise ImportError("pandas package is required for CSV export. Install it using 'pip install pandas'.")
        
        # Create directory if it doesn't exist
        os.makedirs(directory_path, exist_ok=True)
        
        file_paths = {}
        
        for block in self.mmcif_data_container:
            block_dict = {}
            
            for category_name in block.categories:
                category = block[category_name]
                
                # Get all data (this will force loading of lazy items)
                items = category.data
                
                # Create DataFrame from items
                df = pd.DataFrame(items)
                
                # Create CSV filename
                filename = f"{prefix}{block.name}_{category_name}.csv"
                filepath = os.path.join(directory_path, filename)
                
                # Save to CSV
                df.to_csv(filepath, index=False)
                block_dict[category_name] = filepath
            
            file_paths[block.name] = block_dict
            
        return file_paths


class DictToMMCIFConverter:
    def __init__(self, validator_factory: Optional[ValidatorFactory] = None):
        self.validator_factory = validator_factory

    def convert(self, data_dict: Dict[str, Any]) -> MMCIFDataContainer:
        data_blocks = {
            block_name: DataBlock(
                block_name,
                self._convert_categories(block_data)
            )
            for block_name, block_data in data_dict.items()
        }
        return MMCIFDataContainer(data_blocks, source_format=DataSourceFormat.DICT)

    def _convert_categories(self, block_data: Dict[str, Any]) -> Dict[str, Category]:
        categories = {}
        for category_name, category_data in block_data.items():
            category = Category(category_name, self.validator_factory)
            if self._is_multi_row(category_data):
                self._populate_multiline_category(category, category_data)
            else:
                self._populate_singleline_category(category, category_data)
            categories[category_name] = category
        return categories

    def _is_multi_row(self, category_data: Any) -> bool:
        return isinstance(category_data, list) and category_data and isinstance(category_data[0], dict)

    def _populate_multiline_category(self, category: Category, rows: list):
        all_item_names = {k for row in rows for k in row.keys()}
        for item_name in all_item_names:
            category[item_name] = []
        for row in rows:
            for item_name in all_item_names:
                category[item_name].append(row.get(item_name, ""))

    def _populate_singleline_category(self, category: Category, data: Dict[str, Any]):
        for item_name, value in data.items():
            category[item_name] = value if isinstance(value, list) else [value]


class FormatLoader(ABC):
    def __init__(self, validator_factory: Optional[ValidatorFactory] = None, 
                 schema_validator: Optional['SchemaValidator'] = None):
        self.validator_factory = validator_factory
        self.schema_validator = schema_validator

    @abstractmethod
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        raise NotImplementedError
        
    def validate_schema(self, data: Any) -> None:
        """
        Validate data against schema if a schema validator is provided.
        
        Args:
            data: Data to validate
            
        Raises:
            ValidationError: If schema validation fails
        """
        if self.schema_validator and data:  # Only validate if data is not empty
            self.schema_validator.validate(data)


class JsonLoader(FormatLoader):
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        import json
        try:
            if isinstance(input_, str) and os.path.exists(input_):
                if os.path.getsize(input_) > 0:
                    try:
                        with open(input_, 'rb') as f:
                            mmap_obj = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                            data = json.loads(mmap_obj[:].decode('utf-8'))
                    except Exception:
                        with open(input_, 'r') as f:
                            data = json.load(f)
                else:
                    # Empty file - create an empty data dictionary
                    data = {}
            elif isinstance(input_, str):
                data = json.loads(input_)
            else:
                data = json.load(input_)
        except json.JSONDecodeError as e:
            # Raise a clear error instead of silently converting to empty dict
            raise ValueError(f"Invalid JSON input: {e}") from e
        
        # Validate data against schema if provided
        self.validate_schema(data)
            
        container = DictToMMCIFConverter(self.validator_factory).convert(data)
        container.source_format = DataSourceFormat.JSON
        return container


class XmlLoader(FormatLoader):
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        from xml.etree import ElementTree as ET
        # Parse XML
        xml_data = input_
        
        if isinstance(input_, str) and os.path.exists(input_):
            # It's a file path
            root = ET.parse(input_).getroot()
            # For schema validation, use the parsed root
            xml_data = root
        elif isinstance(input_, str):
            # It's an XML string
            try:
                root = ET.fromstring(input_)
                xml_data = root
            except Exception:
                # Try encoding it if parsing as a string failed
                root = ET.fromstring(input_.encode('utf-8'))
                xml_data = root
        else:
            # It's a file-like object
            root = ET.parse(input_).getroot()
            xml_data = root

        # Validate XML against schema if provided
        if self.schema_validator:
            self.validate_schema(xml_data)

        data_dict = {}
        for block_elem in root.findall(".//data_block"):
            block_name = block_elem.get("name")
            block_dict = {}
            for category_elem in block_elem.findall("category"):
                category_name = category_elem.get("name")
                rows = category_elem.findall("row")
                if rows:
                    category_list = []
                    for row_elem in rows:
                        row_dict = {item.get("name"): item.text or "" for item in row_elem.findall("item")}
                        category_list.append(row_dict)
                    block_dict[category_name] = category_list
                else:
                    block_dict[category_name] = {
                        item.get("name"): item.text or "" for item in category_elem.findall("item")
                    }
            data_dict[block_name] = block_dict
        container = DictToMMCIFConverter(self.validator_factory).convert(data_dict)
        container.source_format = DataSourceFormat.XML
        return container


class PickleLoader(FormatLoader):
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        import pickle
        if not isinstance(input_, str):
            raise TypeError("PickleLoader requires a file path string.")
        if os.path.getsize(input_) > 0:
            try:
                with open(input_, 'rb') as f:
                    mmap_obj = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                    data = pickle.loads(mmap_obj[:])
            except Exception:
                with open(input_, 'rb') as f:
                    data = pickle.load(f)
        else:
            with open(input_, 'rb') as f:
                data = pickle.load(f)
                
        # Validate data against schema if provided
        self.validate_schema(data)
                
        container = DictToMMCIFConverter(self.validator_factory).convert(data)
        container.source_format = DataSourceFormat.PICKLE
        return container


class YamlLoader(FormatLoader):
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        import yaml
        # Store original YAML for schema validation if string
        original_yaml = input_ if isinstance(input_, str) else None
        
        try:
            if isinstance(input_, str) and os.path.exists(input_):
                if os.path.getsize(input_) > 0:
                    try:
                        with open(input_, 'rb') as f:
                            mmap_obj = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                            data = yaml.safe_load(mmap_obj[:].decode('utf-8'))
                            # Store original YAML for schema validation
                            original_yaml = mmap_obj[:].decode('utf-8')
                    except Exception:
                        with open(input_, 'r') as f:
                            original_yaml = f.read()
                            data = yaml.safe_load(original_yaml)
                else:
                    # Empty file - create empty data dictionary
                    data = {}
            elif isinstance(input_, str):
                data = yaml.safe_load(input_)
                # Original YAML already stored
            else:
                original_yaml = None  # Can't get original from file handle
                data = yaml.safe_load(input_)
                
            # Handle None result from yaml.safe_load (empty file)
            if data is None:
                data = {}
                
        except Exception:
            # Handle any YAML parsing errors by returning an empty data dictionary
            data = {}
        
        # For schema validation, prefer the parsed data over the original string
        # as the YAMLSchemaValidator can handle both the string and parsed data
        validation_data = data if data else original_yaml
        self.validate_schema(validation_data)
            
        container = DictToMMCIFConverter(self.validator_factory).convert(data)
        container.source_format = DataSourceFormat.YAML
        return container


class CsvLoader(FormatLoader):
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        import pandas as pd
        import glob
        import re
        if not isinstance(input_, str):
            raise TypeError("CsvLoader requires a directory path string.")
        pattern = r'^(.+?)_(.+?)\.csv$'  # Non-greedy match for block and category names with underscores
        data_dict = {}
        csv_file_data = {}  # Store file data for validation
        
        for csv_file in glob.glob(os.path.join(input_, '*.csv')):
            match = re.match(pattern, os.path.basename(csv_file))
            if match:
                block_name, category_name = match.groups()
                if block_name not in data_dict:
                    data_dict[block_name] = {}
                    
                df = pd.read_csv(csv_file)
                
                # Store dataframe with filename for validation
                csv_file_data[os.path.basename(csv_file)] = {
                    'file': os.path.basename(csv_file),
                    'data': df
                }
                
                data_dict[block_name][category_name] = df.to_dict('records')
        
        # Validate CSV data if a schema validator is provided
        # Pass each CSV file's data to the validator
        if self.schema_validator:
            for filename, file_data in csv_file_data.items():
                self.validate_schema(file_data)
                
        container = DictToMMCIFConverter(self.validator_factory).convert(data_dict)
        container.source_format = DataSourceFormat.CSV
        return container


FORMAT_LOADERS = {
    DataSourceFormat.JSON: JsonLoader,
    DataSourceFormat.XML: XmlLoader,
    DataSourceFormat.YAML: YamlLoader,
    DataSourceFormat.PICKLE: PickleLoader,
    DataSourceFormat.CSV: CsvLoader,
}


class MMCIFImporter:
    @staticmethod
    def from_dict(data_dict: Dict[str, Any], validator_factory: Optional[ValidatorFactory] = None) -> MMCIFDataContainer:
        return DictToMMCIFConverter(validator_factory).convert(data_dict)

    @staticmethod
    def from_json(json_str_or_file: Union[str, IO], 
                  validator_factory: Optional[ValidatorFactory] = None,
                  schema_validator: Optional['SchemaValidator'] = None) -> MMCIFDataContainer:
        return JsonLoader(validator_factory, schema_validator).load(json_str_or_file)

    @staticmethod
    def from_xml(xml_str_or_file: Union[str, IO], 
                 validator_factory: Optional[ValidatorFactory] = None,
                 schema_validator: Optional['SchemaValidator'] = None) -> MMCIFDataContainer:
        return XmlLoader(validator_factory, schema_validator).load(xml_str_or_file)

    @staticmethod
    def from_pickle(file_path: str, 
                    validator_factory: Optional[ValidatorFactory] = None,
                    schema_validator: Optional['SchemaValidator'] = None) -> MMCIFDataContainer:
        return PickleLoader(validator_factory, schema_validator).load(file_path)

    @staticmethod
    def from_yaml(yaml_str_or_file: Union[str, IO], 
                  validator_factory: Optional[ValidatorFactory] = None,
                  schema_validator: Optional['SchemaValidator'] = None) -> MMCIFDataContainer:
        return YamlLoader(validator_factory, schema_validator).load(yaml_str_or_file)

    @classmethod
    def from_csv_files(cls, directory_path: str, 
                      validator_factory: Optional[ValidatorFactory] = None,
                      schema_validator: Optional['SchemaValidator'] = None) -> MMCIFDataContainer:
        return CsvLoader(validator_factory, schema_validator).load(directory_path)

    @classmethod
    def auto_detect_format(cls, file_path: str, 
                          validator_factory: Optional[ValidatorFactory] = None,
                          schema_validator: Optional['SchemaValidator'] = None,
                          validate_schema: bool = False) -> MMCIFDataContainer:
        """
        Auto-detect the format of the input file and load it.
        
        Args:
            file_path: Path to the file
            validator_factory: Optional validator factory for data validation
            schema_validator: Optional schema validator for format-specific schema validation
            validate_schema: Whether to validate against schema (if schema_validator is None,
                          will try to create one from SchemaValidatorFactory)
                          
        Returns:
            MMCIFDataContainer object
        """
        # Create schema validator if needed and requested
        format_specific_validator = None
        if validate_schema:
            if schema_validator is not None:
                format_specific_validator = schema_validator
            else:
                try:
                    # Import here to avoid circular imports
                    from .schemas import SchemaValidatorFactory
                    
                    # Detect format first
                    detected_format = None
                    if os.path.isdir(file_path):
                        detected_format = DataSourceFormat.CSV
                    else:
                        ext = os.path.splitext(file_path.lower())[1]
                        format_map = {
                            '.json': DataSourceFormat.JSON,
                            '.xml': DataSourceFormat.XML,
                            '.yaml': DataSourceFormat.YAML,
                            '.yml': DataSourceFormat.YAML,
                            '.pkl': DataSourceFormat.PICKLE,
                            '.pickle': DataSourceFormat.PICKLE
                        }
                        detected_format = format_map.get(ext)
                    
                    if detected_format:
                        format_specific_validator = SchemaValidatorFactory.create_validator(detected_format)
                except (ImportError, ValueError, Exception):
                    # If schema validation can't be created, continue without it
                    pass
        
        # Check if it's a directory (for CSV files)
        if os.path.isdir(file_path):
            return cls.from_csv_files(file_path, validator_factory, format_specific_validator)
        
        ext = os.path.splitext(file_path.lower())[1]
        if ext == '.json':
            return cls.from_json(file_path, validator_factory, format_specific_validator)
        elif ext == '.xml':
            return cls.from_xml(file_path, validator_factory, format_specific_validator)
        elif ext in ('.yaml', '.yml'):
            return cls.from_yaml(file_path, validator_factory, format_specific_validator)
        elif ext in ('.pkl', '.pickle'):
            return cls.from_pickle(file_path, validator_factory, format_specific_validator)
        elif ext == '.csv':
            return cls.from_csv_files(file_path, validator_factory)
        elif ext == '.cif':
            handler = MMCIFHandler()
            container = handler.parse(file_path)
            container.source_format = DataSourceFormat.MMCIF
            return container
        raise ValueError(f"Unsupported file extension: {ext}")


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
        self._file_obj = None

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

    def write(self, mmcif_data_container: MMCIFDataContainer) -> None:
        """
        Writes a data container to a file.

        :param mmcif_data_container: The data container to write.
        :type mmcif_data_container: MMCIFDataContainer
        :return: None
        """
        if hasattr(self, '_file_obj') and self._file_obj:
            self._writer = MMCIFWriter()
            self._writer.write(self._file_obj, mmcif_data_container)
        else:
            raise IOError("File is not open for writing")
            
    def export_to_json(self, mmcif_data_container: MMCIFDataContainer, file_path: Optional[str] = None, 
                       indent: int = 2) -> Optional[str]:
        """
        Export mmCIF data to JSON format.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :param file_path: Path to save the JSON file (optional)
        :type file_path: Optional[str]
        :param indent: Number of spaces for indentation
        :type indent: int
        :return: JSON string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        exporter = MMCIFExporter(mmcif_data_container)
        return exporter.to_json(file_path, indent)
        
    def export_to_xml(self, mmcif_data_container: MMCIFDataContainer, file_path: Optional[str] = None, 
                     pretty_print: bool = True) -> Optional[str]:
        """
        Export mmCIF data to XML format.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :param file_path: Path to save the XML file (optional)
        :type file_path: Optional[str]
        :param pretty_print: Whether to format XML with indentation
        :type pretty_print: bool
        :return: XML string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        exporter = MMCIFExporter(mmcif_data_container)
        return exporter.to_xml(file_path, pretty_print)
        
    def export_to_pickle(self, mmcif_data_container: MMCIFDataContainer, file_path: str) -> None:
        """
        Export mmCIF data to a Python pickle file.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :param file_path: Path to save the pickle file
        :type file_path: str
        :return: None
        """
        exporter = MMCIFExporter(mmcif_data_container)
        exporter.to_pickle(file_path)
        
    def export_to_yaml(self, mmcif_data_container: MMCIFDataContainer, file_path: Optional[str] = None) -> Optional[str]:
        """
        Export mmCIF data to YAML format.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :param file_path: Path to save the YAML file (optional)
        :type file_path: Optional[str]
        :return: YAML string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        exporter = MMCIFExporter(mmcif_data_container)
        return exporter.to_yaml(file_path)
        
    def export_to_pandas(self, mmcif_data_container: MMCIFDataContainer) -> Dict[str, Dict[str, Any]]:
        """
        Export mmCIF data to pandas DataFrames, with one DataFrame per category.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :return: Dictionary of DataFrames organized by data block and category
        :rtype: Dict[str, Dict[str, Any]]
        """
        exporter = MMCIFExporter(mmcif_data_container)
        return exporter.to_pandas()
        
    def export_to_csv(self, mmcif_data_container: MMCIFDataContainer, directory_path: str, prefix: str = "") -> Dict[str, Dict[str, str]]:
        """
        Export mmCIF data to CSV files, with one file per category.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :param directory_path: Directory to save the CSV files
        :type directory_path: str
        :param prefix: Prefix for CSV filenames
        :type prefix: str
        :return: Dictionary mapping block and category names to file paths
        :rtype: Dict[str, Dict[str, str]]
        """
        exporter = MMCIFExporter(mmcif_data_container)
        return exporter.to_csv(directory_path, prefix)
        
    def import_from_json(self, file_path: str, schema_validator=None) -> MMCIFDataContainer:
        """
        Import mmCIF data from a JSON file.
        
        :param file_path: Path to the JSON file
        :type file_path: str
        :param schema_validator: Optional schema validator for data validation
        :type schema_validator: SchemaValidator
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        container = MMCIFImporter.from_json(file_path, self.validator_factory, schema_validator)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.JSON
        return container
    
    def import_from_xml(self, file_path: str, schema_validator=None) -> MMCIFDataContainer:
        """
        Import mmCIF data from an XML file.
        
        :param file_path: Path to the XML file
        :type file_path: str
        :param schema_validator: Optional schema validator for data validation
        :type schema_validator: SchemaValidator
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        container = MMCIFImporter.from_xml(file_path, self.validator_factory, schema_validator)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.XML
        return container
    
    def import_from_pickle(self, file_path: str, schema_validator=None) -> MMCIFDataContainer:
        """
        Import mmCIF data from a pickle file.
        
        :param file_path: Path to the pickle file
        :type file_path: str
        :param schema_validator: Optional schema validator for data validation
        :type schema_validator: SchemaValidator
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        container = MMCIFImporter.from_pickle(file_path, self.validator_factory, schema_validator)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.PICKLE
        return container
    
    def import_from_yaml(self, file_path: str, schema_validator=None) -> MMCIFDataContainer:
        """
        Import mmCIF data from a YAML file.
        
        :param file_path: Path to the YAML file
        :type file_path: str
        :param schema_validator: Optional schema validator for data validation
        :type schema_validator: SchemaValidator
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        container = MMCIFImporter.from_yaml(file_path, self.validator_factory, schema_validator)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.YAML
        return container
    
    def import_from_csv_files(self, directory_path: str, schema_validator=None) -> MMCIFDataContainer:
        """
        Import mmCIF data from CSV files in a directory.
        
        :param directory_path: Directory containing CSV files
        :type directory_path: str
        :param schema_validator: Optional schema validator for data validation
        :type schema_validator: SchemaValidator
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        container = MMCIFImporter.from_csv_files(directory_path, self.validator_factory, schema_validator)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.CSV
        return container
    
    def import_auto_detect(self, file_path: str, validate_schema=False) -> MMCIFDataContainer:
        """
        Auto-detect file format and import mmCIF data.
        
        :param file_path: Path to the file to import
        :type file_path: str
        :param validate_schema: Whether to validate against schema
        :type validate_schema: bool
        :return: An MMCIFDataContainer instance with appropriate source_format flag set
        :rtype: MMCIFDataContainer
        """
        return MMCIFImporter.auto_detect_format(file_path, self.validator_factory, 
                                              validate_schema=validate_schema)

    @property
    def file_obj(self):
        """Provides access to the file object."""
        return self._file_obj

    @file_obj.setter
    def file_obj(self, file_obj):
        """Sets the file object."""
        self._file_obj = file_obj
