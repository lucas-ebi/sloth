from typing import Callable, Dict, Tuple, List, Any, Union, Optional, IO
import io
import shlex


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


# class Item: # TODO: Implement this class for lazy loading of values
#     """A class to represent an item in a Category."""
#     def __init__(self, name: str, file_obj: Optional[IO] = None, slices: Optional[List[Tuple[int, int]]] = None):
#         self._name = name
#         self._file_obj = file_obj
#         self._slices = slices

#     @property
#     def name(self) -> str:
#         """Provides read-only access to the item name."""
#         return self._name

#     def _load_values(self):
#         if self._file_obj is None or self._slices is None:
#             raise ValueError("file_obj and slices must be set to load values.")

#         for item, offsets in slices.items():
#             for start_offset, end_offset in offsets:
#                 bytes_to_read = end_offset - start_offset
#                 content.seek(start_offset - 1) # Adjust the offset
#                 value = content.read(bytes_to_read).decode('utf-8').strip()
#                 yield value

#     def __iter__(self):
#         # Lazy loading values only when iteration begins
#         return self._load_values()

#     def __len__(self):
#         if self._slices is None:
#             return 0
#         return len(self._slices)

#     def __repr__(self):
#         return f"Item(name={self.name}, length={len(self)})"


class Category:
    """A class to represent a category in a data block."""
    def __init__(self, name: str, validator_factory: Optional[ValidatorFactory]):
        self.name: str = name
        self._items: Dict[str, List[str]] = {}
        self._validator_factory: Optional[ValidatorFactory] = validator_factory

    def __setattr__(self, name: str, value: Any) -> None:
        if name in ('name', '_items', '_validator_factory'):
            super().__setattr__(name, value)
        else:
            self._items[name] = value

    def __getattr__(self, item_name: str) -> List[str]:
        if item_name in self._items:
            return self._items[item_name]
        elif item_name == 'validate':
            return self._create_validator()
        else:
            raise AttributeError(f"'Category' object has no attribute '{item_name}'")

    def _create_validator(self):
        return self.Validator(self, self._validator_factory)

    def __getitem__(self, item_name: str) -> List[str]:
        return self._items[item_name]

    def __setitem__(self, item_name: str, value: List[str]) -> None:
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
        """Provides read-only access to the data."""
        return self._items

    def _add_item_value(self, item_name: str, value: str) -> None: # TODO: Implement lazy loading of values
        """Adds a value to the list of values for the given item name."""
        if item_name not in self._items:
            self._items[item_name] = []
        self._items[item_name].append(value)


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
    """A class to parse an mmCIF file and return a data container."""

    def __init__(self, atoms: bool, validator_factory: Optional[ValidatorFactory], categories: Optional[List[str]] = None):
        self.atoms = atoms
        self.validator_factory = validator_factory
        self.categories = categories
        self._file_obj = None
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

    def parse(self, file_obj: IO) -> MMCIFDataContainer:
        self._file_obj = file_obj
        try:
            while True:
                line = self._file_obj.readline()
                if not line:
                    break
                self._process_line(line.rstrip())
        except IndexError as e:
            print(f"Error reading file: list index out of range - {e}")
        except KeyError as e:
            print(f"Missing data block or category: {e}")
        except Exception as e:
            print(f"Error reading file: {e}")

        return MMCIFDataContainer(self._data_blocks)

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
        return not (category.startswith('_atom_site') and not self.atoms) and \
               (not self.categories or category in self.categories)

    def _ensure_current_data(self, category: str):
        if self._current_category != category:
            self._current_category = category
            if category not in self._data_blocks[self._current_block]._categories:
                self._data_blocks[self._current_block]._categories[category] = Category(
                    category, self.validator_factory)
            self._current_data = self._data_blocks[self._current_block]._categories[category]


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
        items = category._items
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
    """A class to handle reading and writing mmCIF files."""
    def __init__(self, atoms: bool = False, validator_factory: Optional[ValidatorFactory] = None):
        self.atoms = atoms
        self.validator_factory = validator_factory
        self._parser = None
        self._writer = None

    def parse(self, filename: str, categories: Optional[List[str]] = None) -> MMCIFDataContainer:
        """
        Parses an mmCIF file and returns a data container.

        :param filename: The name of the file to parse.
        :type filename: str
        :param categories: The categories to parse.
        :type categories: Optional[List[str]]
        :return: The data container.
        :rtype: MMCIFDataContainer
        """
        self._parser = MMCIFParser(self.atoms, self.validator_factory, categories)
        with open(filename, 'r') as f:
            return self._parser.parse(f)

    def write(self, data_container: MMCIFDataContainer) -> None:
        """
        Writes a data container to a file.

        :param data_container: The data container to write.
        :type data_container: MMCIFDataContainer
        :return: None
        """
        if self._file_obj:
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
