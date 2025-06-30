from typing import Callable, Dict, Tuple, List, Any, Union, Optional, IO, Iterator, Protocol, TypeVar, runtime_checkable, Type
from functools import cached_property
import os
import mmap
from enum import Enum, auto
from abc import ABC, abstractmethod
from .validator import ValidatorFactory, CategoryValidator
            

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
            
    def __init__(self, name: str, validator_factory: Optional[ValidatorFactory] = None,
                 mmap_obj: Optional[mmap.mmap] = None):
        self._name = name
        self._items: Dict[str, Union[List[str], Item]] = {}
        self._validator_factory = validator_factory
        self._mmap_obj = mmap_obj

    @property
    def name(self) -> str:
        return self._name
        
    @property
    def validator_factory(self) -> Optional[ValidatorFactory]:
        return self._validator_factory

    @property
    def items(self) -> List[str]:
        """Get names of contained items."""
        return list(self._items.keys())

    def __getattr__(self, item_name: str) -> Union[List[str], Item, CategoryValidator]:
        if item_name in self._items:
            item = self._items[item_name]
            # Return values for Item objects, the Item itself for direct access
            if isinstance(item, Item):
                return item.values
            return item
        elif item_name == 'validate':
            if self._validator_factory is None:
                raise ValueError("No validator factory provided to this category")
            return CategoryValidator(self.name, self._validator_factory)
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
