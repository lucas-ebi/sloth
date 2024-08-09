from typing import Callable, Dict, Tuple, List, Any, Union, Optional, IO
import io
import mmap
import weakref


class Item:
    def __init__(self, mmap_obj=None, start_offset=None, end_offset=None, col_index=None):
        self._mmap_obj = mmap_obj
        self._start_offset = start_offset
        self._end_offset = end_offset
        self._col_index = col_index
        self._length = None  # Cache for storing the length

    def __iter__(self):
        return self._load_values()

    def _load_values(self):
        if self._mmap_obj is None or self._start_offset is None or self._end_offset is None or self._col_index is None:
            raise ValueError("mmap_obj, start_offset, end_offset, and col_index must be set to load values.")

        offset = self._start_offset
        while offset < self._end_offset:
            self._mmap_obj.seek(offset)
            line = self._mmap_obj.readline().strip().decode('utf-8')
            values = line.split()
            if len(values) > self._col_index:
                yield values[self._col_index]
            offset = self._mmap_obj.tell()

    def get_value(self):
        """Returns the content of the multi-line value as a single string."""
        if self._mmap_obj is None or self._start_offset is None or self._end_offset is None:
            raise ValueError("mmap_obj, start_offset, and end_offset must be set to get the value.")
        
        self._mmap_obj.seek(self._start_offset)
        content = self._mmap_obj.read(self._end_offset - self._start_offset).decode('utf-8')
        return content.strip()

    def __len__(self):
        if self._length is None:
            if self._mmap_obj is None or self._start_offset is None or self._end_offset is None:
                raise ValueError("mmap_obj, start_offset, and end_offset must be set to calculate length.")
            
            offset = self._start_offset
            self._length = 0
            while offset < self._end_offset:
                self._mmap_obj.seek(offset)
                line = self._mmap_obj.readline().strip()
                if line:
                    self._length += 1
                offset = self._mmap_obj.tell()

        return self._length

    def __repr__(self):
        return f"Item(col_index={self._col_index}, length={len(self)})"

class Table:
    def __init__(self, mmap_obj=None, start_offset=None, end_offset=None, header=None):
        self._mmap_obj = mmap_obj
        self._start_offset = start_offset
        self._end_offset = end_offset
        self.header = header
        self._items: Optional[Dict[str, Item]] = None  # Lazy-loaded or set via data property

    @property
    def data(self) -> Dict[str, Item]:
        if self._items is None:
            if self._mmap_obj is None or self._start_offset is None or self._end_offset is None or self.header is None:
                raise ValueError("mmap_obj, start_offset, end_offset, and header must be set to generate data.")
            
            # Generate the items lazily
            self._items = {
                key: Item(self._mmap_obj, self._start_offset, self._end_offset, idx)
                for idx, key in enumerate(self.header)
            }
        return self._items

    @data.setter
    def data(self, items: Dict[str, Item]):
        if not all(isinstance(value, Item) for value in items.values()):
            raise ValueError("All values in the data dictionary must be instances of Item.")
        self._items = items

    def __iter__(self):
        return iter(self.data)


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


class Category:
    """A class to represent a category in a data block."""
    def __init__(self, name: str, items: 'Table', validator_factory: Optional['ValidatorFactory'] = None):
        self.name: str = name
        self._items: 'Table' = items  # Store the Table object directly, still named _items for consistency
        self._validator_factory: Optional[ValidatorFactory] = validator_factory

    def __setattr__(self, name: str, value: Any) -> None:
        if name in ('name', '_items', '_validator_factory'):
            super().__setattr__(name, value)
        else:
            # Set an Item instance directly in the Table's data
            self._items.data[name] = value

    def __getattr__(self, item_name: str) -> 'Item':
        if item_name in self._items.data:
            return self._items.data[item_name]  # Return the Item instance from the Table
        elif item_name == 'validate':
            return self._create_validator()
        else:
            raise AttributeError(f"'Category' object has no attribute '{item_name}'")

    def _create_validator(self):
        return self.Validator(self, self._validator_factory)

    def __getitem__(self, item_name: str) -> 'Item':
        return self._items.data[item_name]  # Return the Item instance from the Table

    def __setitem__(self, item_name: str, value: 'Item') -> None:
        self._items.data[item_name] = value

    def __iter__(self):
        return iter(self._items.data.items())

    def __len__(self):
        return len(self._items.data)

    def __repr__(self):
        # Limit the output to avoid long print statements
        return f"Category(name={self.name}, items={list(self._items.data.keys())})"

    @property
    def items(self) -> Dict[str, 'Item']:
        """
        Provides read-only access to the data as a dictionary of
        item names and their corresponding Item instances.
        """
        return self._items.data

    class Validator:
        """A class to validate a category."""
        def __init__(self, category: 'Category', factory: 'ValidatorFactory'):
            self._category: 'Category' = category
            self._factory: 'ValidatorFactory' = factory
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
    def categories(self) -> Dict[str, Category]:
        """Provides read-only access to the categories."""
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
    """A class to read an mmCIF file and return a data container."""

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
        self._multi_line_start_offset = 0

    def parse(self, file_obj: IO) -> MMCIFDataContainer:
        with mmap.mmap(file_obj.fileno(), 0, access=mmap.ACCESS_READ) as mmap_obj:
            self._parse_mmap(mmap_obj)
        return MMCIFDataContainer(self._data_blocks)

    def _parse_mmap(self, mmap_obj) -> None:
        start_offset = None
        header = []

        for line_num, line in enumerate(iter(mmap_obj.readline, b"")):
            line = line.decode('utf-8').strip()
            if line.startswith('#'):
                continue  # Ignore comments
            if line.startswith('data_'):
                self._start_new_data_block(line.split('_', 1)[1])
            elif line.startswith('loop_'):
                self._start_loop()
            elif line.startswith('_'):
                try:
                    self._process_item(line, mmap_obj)
                except ValueError:
                    self._process_table(line, mmap_obj, start_offset, header)
            elif self._in_loop:
                self._process_table(line, mmap_obj, start_offset, header)
            elif self._multi_line_value:
                self._handle_multi_line_value(line, mmap_obj)
            else:
                continue  # Skip non-relevant lines

        # Ensure the last category is added
        if self._current_category is not None and not self._in_loop:
            self._finalize_category(mmap_obj, start_offset, self._current_category, header)

    def _process_item(self, line: str, mmap_obj) -> None:
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise ValueError("Invalid key-value pair")

        item_full, value = parts
        category, item = item_full.split('.', 1)
        if self.categories and category not in self.categories:
            return

        self._set_current_category(category)

        if value.startswith(';'):  # Indicates the start of a multi-line value
            self._multi_line_value = True
            self._multi_line_item_name = item
            self._multi_line_start_offset = mmap_obj.tell() - len(value)
        else:
            end_offset = mmap_obj.tell()  # Current position as end offset for single-line value
            item_obj = Item(mmap_obj, self._multi_line_start_offset, end_offset, 0)
            self._current_data[item] = item_obj

    def _handle_multi_line_value(self, line: str, mmap_obj) -> None:
        if line == ';':  # End of multi-line value
            self._multi_line_value = False
            end_offset = mmap_obj.tell()  # Current position as end offset

            # Determine if we're in a table or single key-value
            if self._in_loop:
                self._current_data._items[self._multi_line_item_name] = Item(
                    mmap_obj, self._multi_line_start_offset, end_offset, len(self._loop_items) - 1)
            else:
                self._current_data._items[self._multi_line_item_name] = Item(
                    mmap_obj, self._multi_line_start_offset, end_offset, 0)
        else:
            # Just track the offset, no need to store the content
            pass

    def _process_table(self, line: str, mmap_obj, start_offset, header) -> None:
        item_full = line.split(' ', 1)[0]
        category, item = item_full.split('.', 1)
        if self.categories and category not in self.categories:
            return

        self._set_current_category(category)

        table_obj = Table(mmap_obj, start_offset, mmap_obj.tell(), header)
        self._current_data._items[item] = table_obj

    def _set_current_category(self, category: str) -> None:
        if self._current_category != category:
            self._current_category = category
            if self._current_category not in self._data_blocks[self._current_block]._categories:
                self._data_blocks[self._current_block]._categories[self._current_category] = Category(
                    self._current_category, Table(), self.validator_factory)
            self._current_data = self._data_blocks[self._current_block]._categories[self._current_category]

    def _finalize_category(self, mmap_obj, start_offset, category, header):
        end_offset = mmap_obj.tell()
        table_obj = Table(mmap_obj, start_offset, end_offset, header)
        items = table_obj.data  # This gets the dictionary of `Item` objects

        # Set the items in the category
        for key, item in items.items():
            self._current_data._items[key] = item


class MMCIFWriter:
    """A class to write an mmCIF data container to a file."""

    def write(self, file_obj: IO, data_container: MMCIFDataContainer) -> None:
        try:
            for data_block in data_container:
                file_obj.write(f"data_{data_block.name}\n")
                file_obj.write("#\n")
                for category_name, category in data_block.categories.items():
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
        items = category.items  # Retrieve the dictionary of Item objects from the Table

        # Determine if it's a looped category (more than one column and rows of data)
        is_loop = len(items) > 1 and any(len(item) > 1 for item in items.values())

        if is_loop:
            file_obj.write("loop_\n")
            for item_name in items.keys():
                file_obj.write(f"{category_name}.{item_name}\n")
            for row in zip(*items.values()):
                formatted_row = [self._format_item_value(item) for item in row]
                file_obj.write(f"{' '.join(formatted_row)}\n".replace('\n\n', '\n'))
        else:
            for item_name, item in items.items():
                for value in item:
                    formatted_value = self._format_item_value(item)
                    file_obj.write(f"{category_name}.{item_name} {formatted_value}\n")

    def _format_item_value(self, item: Item) -> str:
        """
        Formats an Item's value for writing to a file.

        :param item: The Item instance containing the value.
        :type item: Item
        :return: The formatted value.
        :rtype: str
        """
        # For multi-line values, retrieve the full content
        value = item.get_value()

        return self._format_value(value)

    @staticmethod
    def _format_value(value: str) -> str:
        """
        Formats a value for writing to a file.

        :param value: The value to format.
        :type value: str
        :return: The formatted value.
        :rtype: str
        """
        if '\n' in value or value.startswith(' ') or value.startswith('_') or value.startswith(';'):
            return f"\n;{value.strip()}\n;\n"
        return f"{value} "


class MMCIFHandler:
    """A class to handle reading and writing mmCIF files."""
    
    class MMCIFHandlerError(Exception):
        """Custom exception for MMCIFHandler errors."""
        pass

    def __init__(self, validator_factory: Optional[ValidatorFactory] = None):
        self.validator_factory = validator_factory
        self._parser = None
        self._writer = MMCIFWriter()
        self._file_obj = None
        self._data_container = None

    def read(self, filename: str, categories: Optional[List[str]] = None) -> None:
        """
        Parses an mmCIF file and stores the data container internally.

        :param filename: The name of the file to parse.
        :type filename: str
        :param categories: The categories to parse.
        :type categories: Optional[List[str]]
        :return: None
        """
        try:
            self._parser = MMCIFParser(self.validator_factory, categories)
            with open(filename, 'r+b') as f:
                self.file_obj = f
                self.content = self._parser.parse(self.file_obj)
        except FileNotFoundError:
            raise self.MMCIFHandlerError(f"File '{filename}' not found. Please check the file path.")
        except IOError as e:
            raise self.MMCIFHandlerError(f"Error reading file '{filename}': {e}")
        except Exception as e:
            raise self.MMCIFHandlerError(f"An unexpected error occurred while parsing '{filename}': {e}")

    def write(self) -> None:
        """
        Writes the internally stored data container to a file.

        :return: None
        """
        try:
            if self.file_obj:
                self._writer.write(self.file_obj, self.content)
            else:
                raise self.MMCIFHandlerError("File is not open for writing. Please ensure a valid file is set.")
        except IOError as e:
            raise self.MMCIFHandlerError(f"Error writing to file: {e}")
        except Exception as e:
            raise self.MMCIFHandlerError(f"An unexpected error occurred during writing: {e}")

    @property
    def file_obj(self):
        """Provides access to the file object."""
        return self._file_obj

    @file_obj.setter
    def file_obj(self, file_obj):
        """Sets the file object."""
        self._file_obj = file_obj

    @property
    def content(self) -> MMCIFDataContainer:
        """Provides access to the data container."""
        return self._data_container

    @content.setter
    def content(self, data_container: MMCIFDataContainer):
        """Sets the data container."""
        self._data_container = data_container
