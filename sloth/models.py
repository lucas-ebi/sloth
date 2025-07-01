from typing import Callable, Dict, Tuple, List, Any, Union, Optional, IO, Iterator, Protocol, TypeVar, runtime_checkable, Type
from functools import cached_property
import os
import sys
import mmap
from enum import Enum, auto
from abc import ABC, abstractmethod
from .validator import ValidatorFactory, CategoryValidator
import sys
            

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


class LazyValueList:
    """A list-like object that loads values from mmap on-demand by index, providing O(1) per-value access."""
    
    def __init__(self, mmap_obj: mmap.mmap, value_offsets: List[Tuple[int, int]]):
        self._mmap_obj = mmap_obj
        self._value_offsets = value_offsets
        self._cached_values: Dict[int, str] = {}
    
    def __getitem__(self, index: Union[int, slice]) -> Union[str, List[str]]:
        if isinstance(index, int):
            # Handle negative indices
            if index < 0:
                index = len(self._value_offsets) + index
            if index < 0 or index >= len(self._value_offsets):
                raise IndexError(f"Value index {index} is out of range")
            
            # Load value on-demand
            if index not in self._cached_values:
                start_offset, end_offset = self._value_offsets[index]
                try:
                    value_bytes = self._mmap_obj[start_offset:end_offset]
                    self._cached_values[index] = value_bytes.decode('utf-8').strip()
                except (IndexError, UnicodeDecodeError):
                    self._cached_values[index] = ""
            
            return self._cached_values[index]
        elif isinstance(index, slice):
            # Handle slice access
            indices = range(*index.indices(len(self._value_offsets)))
            return [self[i] for i in indices]
        else:
            raise TypeError(f"Value indices must be integers or slices, not {type(index).__name__}")
    
    def __len__(self) -> int:
        return len(self._value_offsets)
    
    def __iter__(self):
        for i in range(len(self._value_offsets)):
            yield self[i]
    
    def __repr__(self):
        cached_count = len(self._cached_values)
        total_count = len(self._value_offsets)
        return f"LazyValueList({total_count} values, {cached_count} loaded)"


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

    @property
    def name(self) -> str:
        """Read-only access to the item name."""
        return self._name

    @cached_property
    def values(self) -> Union[List[str], LazyValueList]:
        """Lazy-loaded values with automatic caching via @cached_property."""
        if self._eager_values is not None:
            return self._eager_values
            
        if self._mmap_obj is None or not self._value_offsets:
            return []
        
        # ALWAYS use LazyValueList for memory-mapped data - consistent O(1) behavior regardless of size
        return LazyValueList(self._mmap_obj, self._value_offsets)

    def add_offset(self, start: int, end: int) -> None:
        """Add a new value offset for memory-mapped access."""
        self._value_offsets.append((start, end))
        # Clear cached_property cache when new offsets are added
        if hasattr(self, 'values'):
            delattr(self, 'values')

    def add_eager_value(self, value: str) -> None:
        """Add a value directly (for small datasets or immediate loading)."""
        if self._eager_values is None:
            self._eager_values = []
        self._eager_values.append(value)
        # Clear cached_property cache when new values are added
        if hasattr(self, 'values'):
            delattr(self, 'values')

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
        # Check if values property has been accessed (cached) by checking if the descriptor exists
        values_loaded = hasattr(self.__class__.__dict__['values'], 'func') and hasattr(self, '__dict__') and 'values' in self.__dict__
        return f"Item(name='{self.name}', length={len(self)}, loaded={values_loaded})"


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


class LazyRowList:
    """A list-like object that creates Row objects only when accessed."""
    
    def __init__(self, category: 'Category', row_count: int):
        self._category = category
        self._row_count = row_count
        self._cached_rows: Dict[int, 'Row'] = {}  # Cache created rows
    
    def __len__(self) -> int:
        return self._row_count
    
    def __getitem__(self, index: Union[int, slice]) -> Union['Row', List['Row']]:
        if isinstance(index, int):
            # Handle negative indices
            if index < 0:
                index = self._row_count + index
            if index < 0 or index >= self._row_count:
                raise IndexError(f"Row index {index} is out of range (0-{self._row_count-1})")
            
            # Return cached row or create new one
            if index not in self._cached_rows:
                self._cached_rows[index] = Row(self._category, index)
            return self._cached_rows[index]
        elif isinstance(index, slice):
            # Handle slice access
            indices = range(*index.indices(self._row_count))
            return [self[i] for i in indices]
        else:
            raise TypeError(f"Row indices must be integers or slices, not {type(index).__name__}")
    
    def __iter__(self):
        for i in range(self._row_count):
            yield self[i]
    
    def __repr__(self):
        return f"LazyRowList({self._row_count} rows, {len(self._cached_rows)} cached)"


class LazyItemDict:
    """A dict-like object that only loads Item values when accessed, providing O(1) creation."""
    
    def __init__(self, items: Dict[str, Union[List[str], 'Item']]):
        self._items = items
        self._cached_values: Dict[str, List[str]] = {}
    
    def __getitem__(self, key: str) -> List[str]:
        if key not in self._cached_values:
            item = self._items[key]
            self._cached_values[key] = item.values if hasattr(item, 'values') and callable(getattr(item, 'values', None)) is False else item
        return self._cached_values[key]
    
    def __setitem__(self, key: str, value: List[str]) -> None:
        # Read-only interface - raise error
        raise TypeError("LazyItemDict is read-only")
    
    def __contains__(self, key: str) -> bool:
        return key in self._items
    
    def __iter__(self):
        return iter(self._items.keys())
    
    def __len__(self) -> int:
        return len(self._items)
    
    def keys(self):
        return self._items.keys()
    
    def values(self):
        return [self[k] for k in self.keys()]
    
    def items(self):
        return [(k, self[k]) for k in self.keys()]
    
    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default
    
    def __eq__(self, other) -> bool:
        if isinstance(other, LazyItemDict):
            # Compare all items (forces loading)
            if len(self) != len(other):
                return False
            for key in self.keys():
                if key not in other or self[key] != other[key]:
                    return False
            return True
        elif isinstance(other, dict):
            return dict(self.items()) == other
        return False

    def __repr__(self):
        cached_count = len(self._cached_values)
        total_count = len(self._items)
        return f"LazyItemDict({total_count} items, {cached_count} loaded)"


class LazyKeyList:
    """A list that dynamically generates prefixed keys without storing them, providing O(1) creation."""
    
    def __init__(self, collection: dict, prefix: str = ""):
        self._collection = collection
        self._prefix = prefix
    
    def __getitem__(self, index: Union[int, slice]) -> Union[str, List[str]]:
        if isinstance(index, int):
            keys = list(self._collection.keys())
            return f"{self._prefix}{keys[index]}"
        elif isinstance(index, slice):
            keys = list(self._collection.keys())
            return [f"{self._prefix}{key}" for key in keys[index]]
        else:
            raise TypeError(f"LazyKeyList indices must be integers or slices, not {type(index).__name__}")
    
    def __len__(self) -> int:
        return len(self._collection)
    
    def __iter__(self):
        for key in self._collection.keys():
            yield f"{self._prefix}{key}"
    
    def __contains__(self, item: str) -> bool:
        if item.startswith(self._prefix):
            stripped = item[len(self._prefix):]
            return stripped in self._collection
        return False
    
    def index(self, item: str) -> int:
        if item.startswith(self._prefix):
            stripped = item[len(self._prefix):]
            keys = list(self._collection.keys())
            return keys.index(stripped)
        raise ValueError(f"{item} is not in list")
    
    def count(self, item: str) -> int:
        return 1 if item in self else 0
    
    def __eq__(self, other) -> bool:
        if isinstance(other, LazyKeyList):
            return list(self) == list(other)
        elif isinstance(other, list):
            return list(self) == other
        return False

    def __repr__(self):
        return f"LazyKeyList({len(self)} keys with prefix '{self._prefix}')"


class Category(DataContainer):
    """A class to represent a category in a data block."""
    
    # Define attributes that should be handled as normal Python attributes
    _RESERVED_ATTRS = {
        '_name', '_items', '_validator_factory', '_mmap_obj', '_batch_buffer', '_row_cache',
        'name', 'validator_factory', 'items', 'data', 'row_count', 'rows'
    }
            
    def __init__(self, name: str, validator_factory: Optional[ValidatorFactory] = None,
                 mmap_obj: Optional[mmap.mmap] = None):
        # Store the stripped name internally (remove _ prefix if present)
        if name.startswith('_'):
            self._name = name[1:]  # Store without the _ prefix
        else:
            self._name = name  # Already stripped
        self._items: Dict[str, Union[List[str], Item]] = {}
        self._validator_factory = validator_factory
        self._mmap_obj = mmap_obj
        self._batch_buffer: Dict[str, List] = {}  # For batching value additions
        self._row_cache: Dict[int, 'Row'] = {}  # Cache for Row objects

    @property
    def name(self) -> str:
        # Return the full name with _ prefix for external API consistency
        return f"_{self._name}"
        
    @property
    def validator_factory(self) -> Optional[ValidatorFactory]:
        return self._validator_factory

    @cached_property
    def items(self) -> LazyKeyList:
        """Get names of contained items - O(1) lazy list."""
        return LazyKeyList(self._items, "")

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

    def __setattr__(self, name: str, value) -> None:
        """
        Enable dot notation assignment for mmCIF items.
        
        Reserved attributes and internal attributes are handled normally.
        Everything else is treated as mmCIF item assignment.
        """
        # Handle reserved attributes and internal attributes normally
        if name in self._RESERVED_ATTRS or name.startswith('__') or name.startswith('_'):
            super().__setattr__(name, value)
            return
            
        # During object initialization, _items might not exist yet
        if not hasattr(self, '_items'):
            super().__setattr__(name, value)
            return
            
        # Validate value type for mmCIF items
        if not isinstance(value, (list, Item)):
            raise TypeError(f"mmCIF item '{name}' must be a list or Item object, got {type(value)}")
            
        # Set as mmCIF item (equivalent to self[name] = value)
        self._items[name] = value
        # Invalidate cached properties when items change
        if hasattr(self, 'items'):
            delattr(self, 'items')
        if hasattr(self, 'data'):
            delattr(self, 'data')
        if hasattr(self, 'rows'):
            delattr(self, 'rows')

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
            # Row access by index - use caching to avoid recreating Row objects
            row_count = self.row_count
            if row_count == 0:
                raise IndexError("Cannot access rows in empty category")
                
            # Handle negative indices
            if key < 0:
                key = row_count + key
            if key < 0 or key >= row_count:
                raise IndexError(f"Row index {key} is out of range (0-{row_count-1})")
            
            # OPTIMIZATION: Cache Row objects to avoid repeated creation
            if key not in self._row_cache:
                self._row_cache[key] = Row(self, key)
            return self._row_cache[key]
        elif isinstance(key, slice):
            # Multiple rows access by slice - use lazy approach
            row_count = self.row_count
            if row_count == 0:
                return []
                
            # OPTIMIZATION: Return lazy slice instead of creating all Row objects
            indices = range(*key.indices(row_count))
            return [self[i] for i in indices]  # This will use the int case above
        else:
            raise TypeError(f"Category indices must be strings, integers or slices, not {type(key).__name__}")

    def __setitem__(self, item_name: str, value: Union[List[str], Item]) -> None:
        self._items[item_name] = value
        # Invalidate cached properties when items change
        if hasattr(self, 'items'):
            delattr(self, 'items')
        if hasattr(self, 'data'):
            delattr(self, 'data')
        if hasattr(self, 'rows'):
            delattr(self, 'rows')

    def __iter__(self):
        return iter(self._items.items())

    def __len__(self):
        return len(self._items)

    def __repr__(self):
        return f"Category(name={self.name}, items={list(self._items.keys())})"

    @cached_property
    def data(self) -> LazyItemDict:
        """Provides O(1) lazy read-only access to the data (loads items on-demand)."""
        return LazyItemDict(self._items)

    @property
    def row_count(self) -> int:
        """Returns the number of rows in this category."""
        if not self._items:
            return 0
        
        # Get the length of the first item to determine row count
        any_item = next(iter(self._items.values()))
        return len(any_item)
        
    @cached_property
    def rows(self) -> LazyRowList:
        """Returns all rows in this category as a lazy list (O(1) creation, cached for performance)."""
        # Always use LazyRowList for consistent O(1) behavior and memory efficiency
        return LazyRowList(self, self.row_count)

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
        
        # Invalidate cached properties when values are added
        self._invalidate_caches()

    def _add_item_value_simple(self, item_name: str, value: str) -> None:
        """Fast value addition for small files without memory mapping overhead."""
        # Use batching for better performance with pre-allocation
        if item_name not in self._batch_buffer:
            self._batch_buffer[item_name] = []
            # Pre-allocate space for common case (helps avoid repeated list resizing)
            if hasattr(self._batch_buffer[item_name], 'extend'):
                # Reserve space for typical category sizes
                reserved_size = 1000 if item_name in ['id', 'Cartn_x', 'Cartn_y', 'Cartn_z'] else 100
                self._batch_buffer[item_name] = [None] * reserved_size
                self._batch_buffer[item_name].clear()  # Clear but keep capacity
        
        self._batch_buffer[item_name].append(value)
        
        # Commit batch when it gets large enough (larger batches for fewer invalidations)
        if len(self._batch_buffer[item_name]) >= 2000:  # Increased from 500
            self._commit_batch(item_name)
        
        # OPTIMIZATION: Only invalidate caches when batch is committed, not on every add
        # This reduces cache invalidation calls from 7000+ to ~20
    
    def _commit_batch(self, item_name: str) -> None:
        """Commit batched values to the actual items storage."""
        if item_name not in self._batch_buffer:
            return
            
        values = self._batch_buffer[item_name]
        if not values:
            return
        
        # OPTIMIZATION: Apply string interning to reduce memory usage
        interned_values = [intern_common_value(v) for v in values]
            
        if item_name not in self._items:
            self._items[item_name] = interned_values
        else:
            if isinstance(self._items[item_name], list):
                self._items[item_name].extend(interned_values)
            else:
                # Convert Item to list and extend
                if hasattr(self._items[item_name], 'values'):
                    existing_values = self._items[item_name].values[:]
                else:
                    existing_values = []
                existing_values.extend(interned_values)
                self._items[item_name] = existing_values
        
        # Clear the batch
        self._batch_buffer[item_name] = []
        
        # Invalidate caches when batch is committed
        self._invalidate_caches()
    
    def _commit_all_batches(self) -> None:
        """Commit all remaining batches at end of parsing."""
        for item_name in list(self._batch_buffer.keys()):
            self._commit_batch(item_name)

    def _invalidate_caches(self) -> None:
        """Invalidate all cached properties when data changes."""
        cache_attrs = ['items', 'data', 'rows']
        for attr in cache_attrs:
            if hasattr(self, attr):
                delattr(self, attr)
        # Also clear row cache
        self._row_cache.clear()


class CategoryCollection(dict):
    """A collection that supports both dict and list access for categories, with automatic _ prefix handling."""
    
    def __getitem__(self, key):
        if isinstance(key, int):
            # List-like access: categories[0], categories[1], etc.
            values_list = list(self.values())
            return values_list[key]
        elif isinstance(key, slice):
            # Slice access: categories[0:2], categories[1:], etc.
            values_list = list(self.values())
            return values_list[key]
        else:
            # Dict-like access with automatic _ prefix handling
            if isinstance(key, str):
                # If key starts with _, strip it for internal storage lookup
                if key.startswith('_'):
                    internal_key = key[1:]  # Remove the '_' prefix
                    return super().__getitem__(internal_key)
                else:
                    # Allow access without _ prefix too
                    return super().__getitem__(key)
            return super().__getitem__(key)
    
    def __setitem__(self, key, value):
        if isinstance(key, str) and key.startswith('_'):
            # Strip the _ prefix for internal storage
            internal_key = key[1:]
            super().__setitem__(internal_key, value)
        else:
            super().__setitem__(key, value)
    
    def __contains__(self, key):
        if isinstance(key, str) and key.startswith('_'):
            # Strip the _ prefix for internal storage lookup
            internal_key = key[1:]
            return super().__contains__(internal_key)
        return super().__contains__(key)
    
    def __iter__(self):
        # Iterate over values (Category objects) for consistency with list behavior
        return iter(self.values())
    
    def keys(self):
        # Return stripped keys for internal use
        return list(super().keys())
    
    def __repr__(self):
        return f"CategoryCollection({len(self)} categories)"


class DataBlock(DataContainer):
    """A class to represent a data block in an mmCIF file."""
    
    # Define attributes that should be handled as normal Python attributes  
    _RESERVED_ATTRS = {
        '_name', '_categories', 'name', 'categories', 'data'
    }
    
    def __init__(self, name: str, categories: Dict[str, Category] = None):
        self._name = name
        # Convert categories to use CategoryCollection with stripped names
        if categories is not None:
            # Strip _ prefix from category names for internal storage
            stripped_categories = {}
            for cat_name, category in categories.items():
                if cat_name.startswith('_'):
                    stripped_categories[cat_name[1:]] = category
                else:
                    stripped_categories[cat_name] = category
            self._categories = CategoryCollection(stripped_categories)
        else:
            self._categories = CategoryCollection()

    @property
    def name(self) -> str:
        return self._name

    @cached_property
    def categories(self) -> LazyKeyList:
        """Get names of contained categories (prefixed names for external API) - O(1) lazy."""
        return LazyKeyList(self._categories, "_")

    @property
    def data(self) -> CategoryCollection:
        """Provides read-only access to the category objects."""
        return self._categories

    def __getitem__(self, category_name: str) -> Category:
        # Handle both prefixed (_category) and unprefixed (category) names
        return self._categories[category_name]

    def __setitem__(self, category_name: str, category: Category) -> None:
        # Handle both prefixed (_category) and unprefixed (category) names
        self._categories[category_name] = category
        # Invalidate cached properties when categories change
        if hasattr(self, 'categories'):
            delattr(self, 'categories')

    def __getattr__(self, category_name: str) -> Category:
        try:
            # Handle both prefixed (_category) and unprefixed (category) names
            # CategoryCollection automatically handles _ prefix stripping/adding
            return self._categories[category_name]
        except KeyError:
            # Auto-create the category if it starts with _ (typical mmCIF category)
            if category_name.startswith('_'):
                new_category = Category(category_name)
                self._categories[category_name] = new_category  # CategoryCollection handles _ stripping
                # Invalidate cached properties when categories change
                if hasattr(self, 'categories'):
                    delattr(self, 'categories')
                return new_category
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{category_name}'")

    def __setattr__(self, name: str, value) -> None:
        """
        Enable dot notation assignment for categories.
        
        Reserved attributes and internal attributes are handled normally.
        Category names (starting with _ or regular names) are treated as category assignment.
        """
        # Handle reserved attributes and internal attributes normally
        if name in self._RESERVED_ATTRS or name.startswith('__'):
            super().__setattr__(name, value)
            return
            
        # During object initialization, _categories might not exist yet
        if not hasattr(self, '_categories'):
            super().__setattr__(name, value)
            return
            
        # For category names (starting with _ or regular category names), validate and set
        if name.startswith('_') or (hasattr(self, '_categories') and (name in self._categories or f"_{name}" in self._categories)):
            if not isinstance(value, Category):
                raise TypeError(f"Category '{name}' must be a Category object, got {type(value)}")
            self._categories[name] = value  # CategoryCollection handles _ stripping/adding
            # Invalidate cached properties when categories change
            if hasattr(self, 'categories'):
                delattr(self, 'categories')
        else:
            # Non-category attributes are handled normally
            super().__setattr__(name, value)

    def __iter__(self):
        return iter(self._categories.values())

    def __len__(self):
        return len(self._categories)

    def __repr__(self):
        return f"DataBlock(name={self.name}, categories={list(self.categories)})"


class DataBlockCollection(dict):
    """A collection that supports both dict and list access for data blocks, with automatic data_ prefix handling."""
    
    def __getitem__(self, key):
        if isinstance(key, int):
            # List-like access: data[0], data[1], etc.
            values_list = list(self.values())
            return values_list[key]
        elif isinstance(key, slice):
            # Slice access: data[0:2], data[1:], etc.
            values_list = list(self.values())
            return values_list[key]
        else:
            # Dict-like access with automatic data_ prefix handling
            if isinstance(key, str):
                # If key starts with data_, strip it for internal storage lookup
                if key.startswith('data_'):
                    internal_key = key[5:]  # Remove the 'data_' prefix
                    return super().__getitem__(internal_key)
                else:
                    # Allow access without data_ prefix too
                    return super().__getitem__(key)
            return super().__getitem__(key)
    
    def __setitem__(self, key, value):
        if isinstance(key, str) and key.startswith('data_'):
            # Strip the data_ prefix for internal storage
            internal_key = key[5:]
            super().__setitem__(internal_key, value)
        else:
            super().__setitem__(key, value)
    
    def __contains__(self, key):
        if isinstance(key, str) and key.startswith('data_'):
            # Strip the data_ prefix for internal storage lookup
            internal_key = key[5:]
            return super().__contains__(internal_key)
        return super().__contains__(key)
    
    def __iter__(self):
        # Iterate over values (DataBlock objects) for consistency with list behavior
        return iter(self.values())
    
    def keys(self):
        # Return stripped keys for internal use
        return list(super().keys())
    
    def __repr__(self):
        return f"DataBlockCollection({len(self)} blocks)"


class MMCIFDataContainer(DataContainer):
    """A class to represent an mmCIF data container."""
    
    # Define attributes that should be handled as normal Python attributes
    _RESERVED_ATTRS = {
        '_data_blocks', 'source_format', 'name', 'blocks', 'data'
    }
    
    def __init__(self, data_blocks: Dict[str, DataBlock] = None, source_format: DataSourceFormat = DataSourceFormat.MMCIF):
        self._data_blocks = DataBlockCollection(data_blocks if data_blocks is not None else {})
        self.source_format = source_format

    @property
    def name(self) -> str:
        return f"MMCIFDataContainer({len(self)} blocks)"

    def __getitem__(self, block_name: str) -> DataBlock:
        # Handle both prefixed (data_block) and unprefixed (block) names
        return self._data_blocks[block_name]

    def __setitem__(self, block_name: str, block: DataBlock) -> None:
        # Handle both prefixed (data_block) and unprefixed (block) names
        self._data_blocks[block_name] = block
        # Invalidate cached properties when blocks change
        if hasattr(self, 'blocks'):
            delattr(self, 'blocks')

    def __getattr__(self, block_name: str) -> DataBlock:
        if block_name.startswith("data_"):
            actual_block_name = block_name[5:]  # Remove the 'data_' prefix
            if actual_block_name in self._data_blocks:
                return self._data_blocks[actual_block_name]
            else:
                # Auto-create the data block
                new_block = DataBlock(actual_block_name)
                self._data_blocks[actual_block_name] = new_block
                # Invalidate cached properties when blocks change
                if hasattr(self, 'blocks'):
                    delattr(self, 'blocks')
                return new_block
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{block_name}'")

    def __setattr__(self, name: str, value) -> None:
        """
        Enable dot notation assignment for data blocks.
        
        Reserved attributes and internal attributes are handled normally.
        Data block names (with data_ prefix) are treated as block assignment.
        """
        # Handle reserved attributes and internal attributes normally
        if name in self._RESERVED_ATTRS or name.startswith('__'):
            super().__setattr__(name, value)
            return
            
        # During object initialization, _data_blocks might not exist yet
        if not hasattr(self, '_data_blocks'):
            super().__setattr__(name, value)
            return
            
        # For data block names (with data_ prefix), validate and set
        if name.startswith('data_'):
            block_name = name[5:]  # Remove 'data_' prefix
            if not isinstance(value, DataBlock):
                raise TypeError(f"Data block 'data_{block_name}' must be a DataBlock object, got {type(value)}")
            self._data_blocks[block_name] = value
            # Invalidate cached properties when blocks change
            if hasattr(self, 'blocks'):
                delattr(self, 'blocks')
        else:
            # Non-block attributes are handled normally
            super().__setattr__(name, value)

    def __iter__(self):
        return iter(self._data_blocks.values())

    def __len__(self):
        return len(self._data_blocks)

    def __repr__(self):
        return f"MMCIFDataContainer({len(self)} blocks)"

    @cached_property
    def blocks(self) -> LazyKeyList:
        """Provides O(1) lazy list of data block names (prefixed names for consistency)."""
        return LazyKeyList(self._data_blocks, "data_")

    @property
    def data(self) -> DataBlockCollection:
        """Provides access to data blocks with both list and dict interfaces."""
        return self._data_blocks


# Common mmCIF value interning for memory efficiency
_COMMON_VALUES = {
    'ATOM', 'HETATM', 'C', 'N', 'O', 'P', 'S', 'CA', 'CB', 'CG', 'CD', 'CE', 'CF',
    'A', 'B', 'X', 'Y', 'Z', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0',
    '.', '?', 'yes', 'no', 'true', 'false'
}
_INTERNED_VALUES = {val: sys.intern(val) for val in _COMMON_VALUES}

def intern_common_value(value: str) -> str:
    """Intern common mmCIF values to save memory."""
    return _INTERNED_VALUES.get(value, value)