from typing import (
    Dict,
    List,
    Union,
    Optional,
)
from difflib import get_close_matches
from functools import cached_property
from abc import ABC, abstractmethod
from .plugins import PluginFactory
from .defaults import PluginScope, DataSourceFormat
import sys
import warnings


class SchemaWarning(UserWarning):
    """Issued when a category or item name is not in the mmCIF dictionary."""
    pass


class _DictionarySchema:
    """Lazy singleton providing O(1) lookups against the bundled mmCIF dictionary.

    The dictionary is parsed once on first access (and disk-cached by
    :class:`~sloth.mmcif.serializer.CacheManager`), so subsequent look-ups
    are essentially free.
    """

    _instance: Optional["_DictionarySchema"] = None

    def __init__(self, categories: frozenset, items_by_category: dict):
        self._categories = categories
        self._items = items_by_category

    # -- public API ---------------------------------------------------------

    @classmethod
    def get(cls) -> Optional["_DictionarySchema"]:
        """Return the singleton, loading on first call.  Returns *None* if
        the dictionary cannot be parsed (graceful degradation)."""
        if cls._instance is None:
            try:
                cls._instance = cls._load()
            except Exception:
                return None
        return cls._instance

    def known_category(self, name: str) -> bool:
        return name in self._categories

    def known_item(self, category_name: str, item_name: str) -> bool:
        cat_items = self._items.get(category_name)
        if cat_items is None:
            return False
        return item_name in cat_items

    def category_items(self, category_name: str) -> frozenset:
        return self._items.get(category_name, frozenset())

    def all_categories(self) -> frozenset:
        return self._categories

    # -- loading ------------------------------------------------------------

    @classmethod
    def _load(cls) -> "_DictionarySchema":
        from pathlib import Path as _Path
        from .serializer import DictionaryParser, get_cache_manager
        from .defaults import DictDataType

        dict_path = str(
            _Path(__file__).parent / "schemas" / "mmcif_pdbx_v50.dic"
        )
        dp = DictionaryParser(get_cache_manager(), quiet=True)
        meta = dp.parse(dict_path)

        items_raw = meta.get(DictDataType.ITEMS.value, {})

        categories: set = set()
        items_by_cat: dict = {}

        for full_name in items_raw:
            parts = full_name.lstrip("_").split(".", 1)
            if len(parts) == 2:
                cat = f"_{parts[0]}"
                categories.add(cat)
                items_by_cat.setdefault(cat, set()).add(parts[1])

        # Also include categories from the categories dict (may contain
        # categories whose items are all defined via loops in parent frames)
        cats_raw = meta.get(DictDataType.CATEGORIES.value, {})
        for cat_name in cats_raw:
            c = f"_{cat_name}" if not cat_name.startswith("_") else cat_name
            categories.add(c)

        return cls(
            frozenset(categories),
            {k: frozenset(v) for k, v in items_by_cat.items()},
        )


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


class Item(DataNode):
    """Represents a column/item in a category. Always uses eager loading."""

    def __init__(self, name: str, values: Optional[List[str]] = None):
        """
        Initialize an Item with pre-loaded values.

        :param name: The name of the item
        :param values: Pre-loaded values
        """
        self._name = name
        self._values = values

    @property
    def name(self) -> str:
        """Read-only access to the item name."""
        return self._name

    @cached_property
    def values(self) -> List[str]:
        """Values with automatic caching via @cached_property."""
        if self._values is not None:
            return self._values

    def add_value(self, value: str) -> None:
        """Add a value directly (for small datasets or immediate loading)."""
        if self._values is None:
            self._values = []
        self._values.append(value)
        # Clear cached_property cache when new values are added
        if hasattr(self, "values"):
            delattr(self, "values")

    def __iter__(self):
        """Iterate over values."""
        return iter(self.values)

    def __len__(self):
        """Get the number of values."""
        if self._values is not None:
            return len(self._values)
        return 0

    def __getitem__(self, index: Union[int, slice]) -> Union[str, List[str]]:
        """Get value(s) by index."""
        return self.values[index]

    def __repr__(self):
        values_loaded = (
            hasattr(self.__class__.__dict__["values"], "func")
            and hasattr(self, "__dict__")
            and "values" in self.__dict__
        )
        return f"Item(name='{self.name}', length={len(self)}, loaded={values_loaded})"


class Row(DataNode):
    """Represents a single row of data in a Category."""

    def __init__(self, category: "Category", row_index: int):
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
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{item_name}'"
        )

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

    def __init__(self, category: "Category", row_count: int):
        self._category = category
        self._row_count = row_count
        self._cached_rows: Dict[int, "Row"] = {}  # Cache created rows

    def __len__(self) -> int:
        return self._row_count

    def __getitem__(self, index: Union[int, slice]) -> Union["Row", List["Row"]]:
        if isinstance(index, int):
            # Handle negative indices
            if index < 0:
                index = self._row_count + index
            if index < 0 or index >= self._row_count:
                raise IndexError(
                    f"Row index {index} is out of range (0-{self._row_count-1})"
                )

            # Return cached row or create new one
            if index not in self._cached_rows:
                self._cached_rows[index] = Row(self._category, index)
            return self._cached_rows[index]
        elif isinstance(index, slice):
            # Handle slice access
            indices = range(*index.indices(self._row_count))
            return [self[i] for i in indices]
        else:
            raise TypeError(
                f"Row indices must be integers or slices, not {type(index).__name__}"
            )

    def __iter__(self):
        for i in range(self._row_count):
            yield self[i]

    def __repr__(self):
        return f"LazyRowList({self._row_count} rows, {len(self._cached_rows)} cached)"


class LazyGemmiColumn(list):
    """
    Lazy wrapper for gemmi loop columns - data extracted only when accessed.
    Behaves like a list but loads data from gemmi on first access.
    """
    
    def __init__(self, gemmi_loop, column_index: int):
        """
        Initialize lazy column wrapper.
        
        Args:
            gemmi_loop: The gemmi loop object containing the data
            column_index: The column index in the loop
        """
        super().__init__()  # Don't populate the list yet
        self._gemmi_loop = gemmi_loop
        self._column_index = column_index
        self._loaded = False
    
    def _ensure_loaded(self):
        """Load column data from gemmi loop on first access."""
        if not self._loaded:
            # Extract all values from gemmi and populate the list
            for row_idx in range(self._gemmi_loop.length()):
                value = self._gemmi_loop[row_idx, self._column_index]
                super().append(str(value))
            self._loaded = True
            # Clear gemmi reference to save memory
            self._gemmi_loop = None
    
    def __getitem__(self, index):
        self._ensure_loaded()
        return super().__getitem__(index)
    
    def __len__(self):
        if self._loaded:
            return super().__len__()
        return self._gemmi_loop.length()
    
    def __iter__(self):
        self._ensure_loaded()
        return super().__iter__()
    
    def __repr__(self):
        # Automatically load data when repr is called (used in print, f-strings, etc.)
        self._ensure_loaded()
        return super().__repr__()
    
    def __str__(self):
        # Automatically load data when converting to string
        self._ensure_loaded()
        return super().__str__()
    
    def __format__(self, format_spec):
        # Automatically load data when used in f-strings
        self._ensure_loaded()
        return super().__format__(format_spec)


class LazyItemDict:
    """A dict-like object that only loads Item values when accessed, providing O(1) creation."""

    def __init__(self, items: Dict[str, Union[List[str], "Item"]]):
        self._items = items
        self._cached_values: Dict[str, List[str]] = {}

    def __getitem__(self, key: str) -> List[str]:
        if key not in self._cached_values:
            item = self._items[key]
            self._cached_values[key] = (
                item.values
                if hasattr(item, "values")
                and callable(getattr(item, "values", None)) is False
                else item
            )
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
        return [self[k] for k in self]

    def items(self):
        return [(k, self[k]) for k in self]

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
            for key in self:
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
            raise TypeError(
                f"LazyKeyList indices must be integers or slices, not {type(index).__name__}"
            )

    def __len__(self) -> int:
        return len(self._collection)

    def __iter__(self):
        for key in self._collection.keys():
            yield f"{self._prefix}{key}"

    def __contains__(self, item: str) -> bool:
        if item.startswith(self._prefix):
            stripped = item[len(self._prefix) :]
            return stripped in self._collection
        return False

    def index(self, item: str) -> int:
        if item.startswith(self._prefix):
            stripped = item[len(self._prefix) :]
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
        "_name",
        "_items",
        "_plugin_factory",
        "_batch_buffer",
        "_row_cache",
        "name",
        "plugin_factory",
        "items",
        "data",
        "row_count",
        "rows",
    }

    def __init__(
        self,
        name: str,
        plugin_factory: Optional[PluginFactory] = None,
    ):
        # Store the stripped name internally (remove _ prefix if present)
        if name.startswith("_"):
            self._name = name[1:]  # Store without the _ prefix
        else:
            self._name = name  # Already stripped
        self._items: Dict[str, Union[List[str], Item]] = {}
        self._plugin_factory = plugin_factory
        self._batch_buffer: Dict[str, List] = {}  # For batching value additions
        self._row_cache: Dict[int, "Row"] = {}  # Cache for Row objects

    @property
    def name(self) -> str:
        # Return the full name with _ prefix for external API consistency
        return f"_{self._name}"

    @property
    def plugin_factory(self) -> Optional[PluginFactory]:
        return self._plugin_factory

    @cached_property
    def items(self) -> LazyKeyList:
        """Get names of contained items - O(1) lazy list."""
        return LazyKeyList(self._items, "")

    def __getattr__(self, item_name: str) -> Union[List[str], Item, "PluginWrapper"]:
        if item_name in self._items:
            item = self._items[item_name]
            # Return values for Item objects, the Item itself for direct access
            if isinstance(item, Item):
                return item.values
            return item
        # Check for registered plugins (covers "validate" and any user plugins)
        if self._plugin_factory is not None:
            wrapper = self._plugin_factory.get_wrapper(item_name, self, PluginScope.CATEGORY)
            if wrapper is not None:
                return wrapper
        hint = _suggest(item_name, list(self.items))
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute "
            f"'{item_name}'.{hint}"
        )

    def __setattr__(self, name: str, value) -> None:
        """
        Enable dot notation assignment for mmCIF items.

        Reserved attributes and internal attributes are handled normally.
        Everything else is treated as mmCIF item assignment.
        """
        # Handle reserved attributes and internal attributes normally
        if (
            name in self._RESERVED_ATTRS
            or name.startswith("__")
            or name.startswith("_")
        ):
            super().__setattr__(name, value)
            return

        # During object initialization, _items might not exist yet
        if not hasattr(self, "_items"):
            super().__setattr__(name, value)
            return

        # Validate value type for mmCIF items
        if not isinstance(value, (list, Item)):
            raise TypeError(
                f"mmCIF item '{name}' must be a list or Item object, got {type(value)}"
            )

        # Set as mmCIF item (equivalent to self[name] = value)
        # Schema hint: warn on unknown item for known categories
        schema = _DictionarySchema.get()
        if schema and schema.known_category(self.name) and not schema.known_item(self.name, name):
            hint = _suggest(name, list(schema.category_items(self.name)))
            warnings.warn(
                f"Item '{name}' is not in the mmCIF dictionary "
                f"for category '{self.name}'.{hint}",
                SchemaWarning,
                stacklevel=2,
            )
        self._items[name] = value
        # Invalidate cached properties when items change
        if hasattr(self, "items"):
            delattr(self, "items")
        if hasattr(self, "data"):
            delattr(self, "data")
        if hasattr(self, "rows"):
            delattr(self, "rows")

    def __delattr__(self, name: str) -> None:
        """Delete an mmCIF item via ``del category.item_name``."""
        if name in self._RESERVED_ATTRS or name.startswith("__") or name.startswith("_"):
            super().__delattr__(name)
            return
        if name in self._items:
            del self._items[name]
            self._invalidate_caches()
            return
        raise AttributeError(
            f"Item '{name}' not found in category '{self.name}'"
        )

    def delete(self, item_name: str) -> None:
        """Delete an mmCIF item by name (string-based API).

        :param item_name: The item name to remove.
        :raises KeyError: If the item does not exist.
        """
        if item_name not in self._items:
            raise KeyError(
                f"Item '{item_name}' not found in category '{self.name}'"
            )
        del self._items[item_name]
        self._invalidate_caches()

    def __getitem__(
        self, key: Union[str, int, slice]
    ) -> Union[List[str], "Row", List["Row"]]:
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
            raise TypeError(
                f"Category indices must be strings, integers or slices, not {type(key).__name__}"
            )

    def __setitem__(self, item_name: str, value: Union[List[str], Item]) -> None:
        self._items[item_name] = value
        # Invalidate cached properties when items change
        if hasattr(self, "items"):
            delattr(self, "items")
        if hasattr(self, "data"):
            delattr(self, "data")
        if hasattr(self, "rows"):
            delattr(self, "rows")

    def __iter__(self):
        # Iterate over rows, not items, for user-facing API consistency
        return iter(self.rows)

    def __len__(self):
        return len(self._items)

    def __dir__(self):
        names = set(super().__dir__())
        names.update(self.items)
        if self._plugin_factory is not None:
            names.update(self._plugin_factory.list_plugins(PluginScope.CATEGORY))
        return sorted(names)

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

    def _add_item_value(self, item_name: str, value: str) -> None:
        """Fast value addition for small files without memory mapping overhead."""
        # Use batching for better performance with pre-allocation
        if item_name not in self._batch_buffer:
            self._batch_buffer[item_name] = []
            # Pre-allocate space for common case (helps avoid repeated list resizing)
            if hasattr(self._batch_buffer[item_name], "extend"):
                # Reserve space for typical category sizes
                reserved_size = (
                    1000
                    if item_name in ["id", "Cartn_x", "Cartn_y", "Cartn_z"]
                    else 100
                )
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
                if hasattr(self._items[item_name], "values"):
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
        cache_attrs = ["items", "data", "rows"]
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
                if key.startswith("_"):
                    internal_key = key[1:]  # Remove the '_' prefix
                    return super().__getitem__(internal_key)
                else:
                    # Allow access without _ prefix too
                    return super().__getitem__(key)
            return super().__getitem__(key)

    def __setitem__(self, key, value):
        if isinstance(key, str) and key.startswith("_"):
            # Strip the _ prefix for internal storage
            internal_key = key[1:]
            super().__setitem__(internal_key, value)
        else:
            super().__setitem__(key, value)

    def __contains__(self, key):
        if isinstance(key, str) and key.startswith("_"):
            # Strip the _ prefix for internal storage lookup
            internal_key = key[1:]
            return super().__contains__(internal_key)
        return super().__contains__(key)

    def __iter__(self):
        # Iterate over keys (category names) not values
        return iter(self.keys())

    def keys(self):
        # Return stripped keys for internal use
        return list(super().keys())

    def __repr__(self):
        return f"CategoryCollection({len(self)} categories)"


class DataBlock(DataContainer):
    """A class to represent a data block in an mmCIF file."""

    # Define attributes that should be handled as normal Python attributes
    _RESERVED_ATTRS = {
        "_name", "_categories", "_plugin_factory",
        "name", "categories", "data", "plugin_factory",
    }

    def __init__(
        self,
        name: str,
        categories: Dict[str, Category] = None,
        plugin_factory: Optional[PluginFactory] = None,
    ):
        self._name = name
        self._plugin_factory = plugin_factory
        # Convert categories to use CategoryCollection with stripped names
        if categories is not None:
            # Strip _ prefix from category names for internal storage
            stripped_categories = {}
            for cat_name, category in categories.items():
                if cat_name.startswith("_"):
                    stripped_categories[cat_name[1:]] = category
                else:
                    stripped_categories[cat_name] = category
            self._categories = CategoryCollection(stripped_categories)
        else:
            self._categories = CategoryCollection()

    @property
    def name(self) -> str:
        return self._name

    @property
    def plugin_factory(self) -> Optional[PluginFactory]:
        return self._plugin_factory

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
        if hasattr(self, "categories"):
            delattr(self, "categories")

    def __getattr__(self, category_name: str) -> Category:
        try:
            return self._categories[category_name]
        except KeyError:
            pass

        # Check for registered plugins
        if self._plugin_factory is not None:
            wrapper = self._plugin_factory.get_wrapper(category_name, self, PluginScope.BLOCK)
            if wrapper is not None:
                return wrapper

        # Return a pending proxy for category-like names (starts with _)
        if category_name.startswith("_"):
            return _PendingCategory(category_name, self)

        hint = _suggest(category_name, list(self.categories))
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute "
            f"'{category_name}'.{hint}"
        )

    def __setattr__(self, name: str, value) -> None:
        """
        Enable dot notation assignment for categories.

        Reserved attributes and internal attributes are handled normally.
        Category names (starting with _ or regular names) are treated as category assignment.
        """
        # Handle reserved attributes and internal attributes normally
        if name in self._RESERVED_ATTRS or name.startswith("__"):
            super().__setattr__(name, value)
            return

        # During object initialization, _categories might not exist yet
        if not hasattr(self, "_categories"):
            super().__setattr__(name, value)
            return

        # For category names (starting with _ or regular category names), validate and set
        if name.startswith("_") or (
            hasattr(self, "_categories")
            and (name in self._categories or f"_{name}" in self._categories)
        ):
            if not isinstance(value, Category):
                raise TypeError(
                    f"Category '{name}' must be a Category object, got {type(value)}"
                )
            self._categories[
                name
            ] = value  # CategoryCollection handles _ stripping/adding
            # Invalidate cached properties when categories change
            if hasattr(self, "categories"):
                delattr(self, "categories")
        else:
            # Non-category attributes are handled normally
            super().__setattr__(name, value)

    def __delattr__(self, name: str) -> None:
        """Delete a category via ``del block._category_name``."""
        if name in self._RESERVED_ATTRS or name.startswith("__"):
            super().__delattr__(name)
            return
        # Resolve key (CategoryCollection handles _ prefix)
        key = name[1:] if name.startswith("_") else name
        if key in self._categories:
            del self._categories[key]
            if hasattr(self, "categories"):
                delattr(self, "categories")
            return
        raise AttributeError(
            f"Category '{name}' not found in data block '{self.name}'"
        )

    def delete(self, category_name: str) -> None:
        """Delete a category by name (string-based API).

        :param category_name: The category name to remove (with or without ``_`` prefix).
        :raises KeyError: If the category does not exist.
        """
        key = category_name[1:] if category_name.startswith("_") else category_name
        if key not in self._categories:
            raise KeyError(
                f"Category '{category_name}' not found in data block '{self.name}'"
            )
        del self._categories[key]
        if hasattr(self, "categories"):
            delattr(self, "categories")

    def __iter__(self):
        return iter(self._categories.values())

    def __len__(self):
        return len(self._categories)

    def __dir__(self):
        names = set(super().__dir__())
        names.update(self.categories)
        if self._plugin_factory is not None:
            names.update(self._plugin_factory.list_plugins(PluginScope.BLOCK))
        return sorted(names)

    def __repr__(self):
        return f"DataBlock(name={self.name}, categories={list(self.categories)})"


# ---------------------------------------------------------------------------
# Helper: fuzzy name suggestion
# ---------------------------------------------------------------------------

def _suggest(name: str, candidates, n: int = 3, cutoff: float = 0.5) -> str:
    """Return a 'Did you mean ...?' suffix, or empty string."""
    matches = get_close_matches(name, candidates, n=n, cutoff=cutoff)
    if matches:
        opts = ", ".join(f"'{m}'" for m in matches)
        return f" Did you mean {opts}?"
    return ""


# ---------------------------------------------------------------------------
# Pending-object proxies (deferred auto-creation)
# ---------------------------------------------------------------------------

class _PendingCategory:
    """Proxy returned by :pymethod:`DataBlock.__getattr__` for category names
    that don't exist yet.

    *Write* operations (``__setitem__``, ``__setattr__``) commit a real
    :class:`Category` to the parent block.  *Read* operations
    (``__getitem__``, ``__getattr__``, iteration, …) raise
    :class:`AttributeError` with a "did you mean?" hint.
    """

    __slots__ = ("_pc_name", "_pc_parent", "_pc_real")

    def __init__(self, name: str, parent: "DataBlock"):
        object.__setattr__(self, "_pc_name", name)
        object.__setattr__(self, "_pc_parent", parent)
        object.__setattr__(self, "_pc_real", None)

    # -- internal -----------------------------------------------------------

    def _commit(self) -> "Category":
        real = object.__getattribute__(self, "_pc_real")
        if real is None:
            name = object.__getattribute__(self, "_pc_name")
            parent = object.__getattribute__(self, "_pc_parent")
            # Schema hint: warn on unknown category
            schema = _DictionarySchema.get()
            if schema and not schema.known_category(name):
                hint = _suggest(name, list(schema.all_categories()))
                warnings.warn(
                    f"Category '{name}' is not in the mmCIF dictionary.{hint}",
                    SchemaWarning,
                    stacklevel=4,
                )
            real = Category(name, plugin_factory=parent._plugin_factory)
            parent._categories[name] = real
            if hasattr(parent, "categories"):
                delattr(parent, "categories")
            object.__setattr__(self, "_pc_real", real)
        return real

    def _raise_does_not_exist(self, extra: str = "") -> None:
        name = object.__getattribute__(self, "_pc_name")
        parent = object.__getattribute__(self, "_pc_parent")
        hint = _suggest(f"_{name}" if not name.startswith("_") else name,
                        list(parent.categories))
        raise AttributeError(
            f"Category '_{name}' does not exist in data block "
            f"'{parent.name}'.{hint}{extra}"
        )

    # -- write operations (commit) ------------------------------------------

    def __setitem__(self, key, value):
        real = self._commit()
        # Schema hint: warn on unknown item for known categories
        schema = _DictionarySchema.get()
        if schema and schema.known_category(real.name) and not schema.known_item(real.name, key):
            hint = _suggest(key, list(schema.category_items(real.name)))
            warnings.warn(
                f"Item '{key}' is not in the mmCIF dictionary "
                f"for category '{real.name}'.{hint}",
                SchemaWarning,
                stacklevel=2,
            )
        real[key] = value

    def __setattr__(self, name, value):
        if name.startswith("_pc_"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._commit(), name, value)

    # -- read operations (raise or delegate) --------------------------------

    def __getitem__(self, key):
        real = object.__getattribute__(self, "_pc_real")
        if real is not None:
            return real[key]
        self._raise_does_not_exist()

    def __getattr__(self, attr):
        real = object.__getattribute__(self, "_pc_real")
        if real is not None:
            return getattr(real, attr)
        self._raise_does_not_exist()

    def __iter__(self):
        real = object.__getattribute__(self, "_pc_real")
        if real is not None:
            return iter(real)
        self._raise_does_not_exist()

    def __len__(self):
        real = object.__getattribute__(self, "_pc_real")
        if real is not None:
            return len(real)
        self._raise_does_not_exist()

    def __bool__(self):
        real = object.__getattribute__(self, "_pc_real")
        if real is not None:
            return bool(real)
        return False

    def __repr__(self):
        real = object.__getattribute__(self, "_pc_real")
        if real is not None:
            return repr(real)
        name = object.__getattribute__(self, "_pc_name")
        parent = object.__getattribute__(self, "_pc_parent")
        hint = _suggest(f"_{name}" if not name.startswith("_") else name,
                        list(parent.categories))
        return (
            f"<PendingCategory '_{name}' — category does not exist in "
            f"'{parent.name}'.{hint} Assign data to create it.>"
        )


class _PendingDataBlock:
    """Proxy returned by :pymethod:`MMCIFDataContainer.__getattr__` for
    block names that don't exist yet.

    Same write-commits / read-raises semantics as :class:`_PendingCategory`.
    Category access on a pending block commits the block first, then
    returns a :class:`_PendingCategory` for the category.
    """

    __slots__ = ("_pb_name", "_pb_parent", "_pb_real")

    def __init__(self, name: str, parent: "MMCIFDataContainer"):
        object.__setattr__(self, "_pb_name", name)
        object.__setattr__(self, "_pb_parent", parent)
        object.__setattr__(self, "_pb_real", None)

    # -- internal -----------------------------------------------------------

    def _commit(self) -> "DataBlock":
        real = object.__getattribute__(self, "_pb_real")
        if real is None:
            name = object.__getattribute__(self, "_pb_name")
            parent = object.__getattribute__(self, "_pb_parent")
            real = DataBlock(name, plugin_factory=parent._plugin_factory)
            parent._data_blocks[name] = real
            if hasattr(parent, "blocks"):
                delattr(parent, "blocks")
            object.__setattr__(self, "_pb_real", real)
        return real

    def _raise_does_not_exist(self) -> None:
        name = object.__getattribute__(self, "_pb_name")
        parent = object.__getattribute__(self, "_pb_parent")
        hint = _suggest(f"data_{name}", list(parent.blocks))
        raise AttributeError(
            f"Data block 'data_{name}' does not exist.{hint}"
        )

    # -- write operations (commit) ------------------------------------------

    def __setitem__(self, key, value):
        self._commit()[key] = value

    def __setattr__(self, name, value):
        if name.startswith("_pb_"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._commit(), name, value)

    # -- read operations (raise or delegate) --------------------------------

    def __getitem__(self, key):
        real = object.__getattribute__(self, "_pb_real")
        if real is not None:
            return real[key]
        self._raise_does_not_exist()

    def __getattr__(self, attr):
        real = object.__getattribute__(self, "_pb_real")
        if real is not None:
            return getattr(real, attr)
        # Category access (starts with _) commits the block,
        # then delegates to block's __getattr__ which returns
        # a _PendingCategory if the category doesn't exist.
        if attr.startswith("_"):
            return getattr(self._commit(), attr)
        self._raise_does_not_exist()

    def __iter__(self):
        real = object.__getattribute__(self, "_pb_real")
        if real is not None:
            return iter(real)
        self._raise_does_not_exist()

    def __len__(self):
        real = object.__getattribute__(self, "_pb_real")
        if real is not None:
            return len(real)
        self._raise_does_not_exist()

    def __bool__(self):
        real = object.__getattribute__(self, "_pb_real")
        if real is not None:
            return bool(real)
        return False

    def __repr__(self):
        real = object.__getattribute__(self, "_pb_real")
        if real is not None:
            return repr(real)
        name = object.__getattribute__(self, "_pb_name")
        parent = object.__getattribute__(self, "_pb_parent")
        hint = _suggest(f"data_{name}", list(parent.blocks))
        return (
            f"<PendingDataBlock 'data_{name}' — block does not exist.{hint} "
            f"Assign data to create it.>"
        )


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
                if key.startswith("data_"):
                    internal_key = key[5:]  # Remove the 'data_' prefix
                    return super().__getitem__(internal_key)
                else:
                    # Allow access without data_ prefix too
                    return super().__getitem__(key)
            return super().__getitem__(key)

    def __setitem__(self, key, value):
        if isinstance(key, str) and key.startswith("data_"):
            # Strip the data_ prefix for internal storage
            internal_key = key[5:]
            super().__setitem__(internal_key, value)
        else:
            super().__setitem__(key, value)

    def __contains__(self, key):
        if isinstance(key, str) and key.startswith("data_"):
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
        "_data_blocks", "_plugin_factory",
        "source_format", "name", "blocks", "data", "plugin_factory",
    }

    def __init__(
        self,
        data_blocks: Dict[str, DataBlock] = None,
        source_format: DataSourceFormat = DataSourceFormat.MMCIF,
        plugin_factory: Optional[PluginFactory] = None,
    ):
        self._data_blocks = DataBlockCollection(
            data_blocks if data_blocks is not None else {}
        )
        self._plugin_factory = plugin_factory
        self.source_format = source_format

    @property
    def name(self) -> str:
        return f"MMCIFDataContainer({len(self)} blocks)"

    @property
    def plugin_factory(self) -> Optional[PluginFactory]:
        return self._plugin_factory

    def __getitem__(self, block_name: str) -> DataBlock:
        # Handle both prefixed (data_block) and unprefixed (block) names
        return self._data_blocks[block_name]

    def __setitem__(self, block_name: str, block: DataBlock) -> None:
        # Handle both prefixed (data_block) and unprefixed (block) names
        self._data_blocks[block_name] = block
        # Invalidate cached properties when blocks change
        if hasattr(self, "blocks"):
            delattr(self, "blocks")

    def __getattr__(self, block_name: str) -> DataBlock:
        if block_name.startswith("data_"):
            actual_block_name = block_name[5:]
            if actual_block_name in self._data_blocks:
                return self._data_blocks[actual_block_name]
            # Return a pending proxy — commits on first write
            return _PendingDataBlock(actual_block_name, self)

        # Check for registered plugins
        if self._plugin_factory is not None:
            wrapper = self._plugin_factory.get_wrapper(block_name, self, PluginScope.CONTAINER)
            if wrapper is not None:
                return wrapper

        hint = _suggest(block_name, list(self.blocks))
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute "
            f"'{block_name}'.{hint}"
        )

    def __setattr__(self, name: str, value) -> None:
        """
        Enable dot notation assignment for data blocks.

        Reserved attributes and internal attributes are handled normally.
        Data block names (with data_ prefix) are treated as block assignment.
        """
        # Handle reserved attributes and internal attributes normally
        if name in self._RESERVED_ATTRS or name.startswith("__"):
            super().__setattr__(name, value)
            return

        # During object initialization, _data_blocks might not exist yet
        if not hasattr(self, "_data_blocks"):
            super().__setattr__(name, value)
            return

        # For data block names (with data_ prefix), validate and set
        if name.startswith("data_"):
            block_name = name[5:]  # Remove 'data_' prefix
            if not isinstance(value, DataBlock):
                raise TypeError(
                    f"Data block 'data_{block_name}' must be a DataBlock object, got {type(value)}"
                )
            self._data_blocks[block_name] = value
            # Invalidate cached properties when blocks change
            if hasattr(self, "blocks"):
                delattr(self, "blocks")
        else:
            # Non-block attributes are handled normally
            super().__setattr__(name, value)

    def __delattr__(self, name: str) -> None:
        """Delete a data block via ``del container.data_blockname``."""
        if name in self._RESERVED_ATTRS or name.startswith("__"):
            super().__delattr__(name)
            return
        if name.startswith("data_"):
            key = name[5:]
            if key in self._data_blocks:
                del self._data_blocks[key]
                if hasattr(self, "blocks"):
                    delattr(self, "blocks")
                return
        raise AttributeError(
            f"Data block '{name}' not found in container"
        )

    def delete(self, block_name: str) -> None:
        """Delete a data block by name (string-based API).

        :param block_name: The block name (with or without ``data_`` prefix).
        :raises KeyError: If the block does not exist.
        """
        key = block_name[5:] if block_name.startswith("data_") else block_name
        if key not in self._data_blocks:
            raise KeyError(f"Data block '{block_name}' not found in container")
        del self._data_blocks[key]
        if hasattr(self, "blocks"):
            delattr(self, "blocks")

    def __iter__(self):
        return iter(self._data_blocks.values())

    def __len__(self):
        return len(self._data_blocks)

    def __dir__(self):
        names = set(super().__dir__())
        names.update(self.blocks)
        if self._plugin_factory is not None:
            names.update(self._plugin_factory.list_plugins(PluginScope.CONTAINER))
        return sorted(names)

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
    "ATOM",
    "HETATM",
    "C",
    "N",
    "O",
    "P",
    "S",
    "CA",
    "CB",
    "CG",
    "CD",
    "CE",
    "CF",
    "A",
    "B",
    "X",
    "Y",
    "Z",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "0",
    ".",
    "?",
    "yes",
    "no",
    "true",
    "false",
}
_INTERNED_VALUES = {val: sys.intern(val) for val in _COMMON_VALUES}


def intern_common_value(value: str) -> str:
    """Intern common mmCIF values to save memory."""
    return _INTERNED_VALUES.get(value, value)
