from typing import Callable, Dict, Tuple, List, Any, Union, Optional, IO
import io

class ValidatorFactory:
    def __init__(self):
        self.validators: Dict[str, Callable[[str], None]] = {}
        self.cross_checkers: Dict[Tuple[str, str], Callable[[str, str], None]] = {}

    def register_validator(self, category_name: str, validator_function: Callable[[str], None]) -> None:
        self.validators[category_name] = validator_function

    def register_cross_checker(self, category_pair: Tuple[str, str], cross_checker_function: Callable[[str, str], None]) -> None:
        self.cross_checkers[category_pair] = cross_checker_function

    def get_validator(self, category_name: str) -> Optional[Callable[[str], None]]:
        return self.validators.get(category_name)

    def get_cross_checker(self, category_pair: Tuple[str, str]) -> Optional[Callable[[str, str], None]]:
        return self.cross_checkers.get(category_pair)


class Category:
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
        try:
            return self._items[item_name]
        except KeyError:
            raise AttributeError(f"'Category' object has no attribute '{item_name}'")

    def __getitem__(self, item_name: str) -> List[str]:
        return self._items[item_name]

    def __setitem__(self, item_name: str, value: List[str]) -> None:
        self._items[item_name] = value

    @property
    def items(self) -> List[str]:
        """Provides a list of item names."""
        return list(self._items.keys())

    def add_item(self, item_name: str, value: str) -> None:
        """Adds a value to the list of values for the given item name."""
        if item_name not in self._items:
            self._items[item_name] = []
        self._items[item_name].append(value)


    class Validator:
        def __init__(self, category: 'Category', factory: ValidatorFactory):
            self.category: 'Category' = category
            self.factory: ValidatorFactory = factory
            self.other_category: Optional['Category'] = None

        def __call__(self) -> 'Category.Validator':
            validator = self.factory.get_validator(self.category.name)
            if validator:
                validator(self.category.name)
            else:
                print(f"No validator registered for category '{self.category.name}'")
            return self

        def against(self, other_category: 'Category') -> 'Category.Validator':
            self.other_category = other_category
            cross_checker = self.factory.get_cross_checker((self.category.name, other_category.name))
            if cross_checker:
                cross_checker(self.category.name, other_category.name)
            else:
                print(f"No cross-checker registered for categories '{self.category.name}' and '{other_category.name}'")
            return self


    class Validator:
        def __init__(self, category: 'Category', factory: ValidatorFactory):
            self.category: 'Category' = category
            self.factory: ValidatorFactory = factory
            self.other_category: Optional['Category'] = None

        def __call__(self) -> 'Category.Validator':
            validator = self.factory.get_validator(self.category.name)
            if validator:
                validator(self.category.name)
            else:
                print(f"No validator registered for category '{self.category.name}'")
            return self

        def against(self, other_category: 'Category') -> 'Category.Validator':
            self.other_category = other_category
            cross_checker = self.factory.get_cross_checker((self.category.name, other_category.name))
            if cross_checker:
                cross_checker(self.category.name, other_category.name)
            else:
                print(f"No cross-checker registered for categories '{self.category.name}' and '{other_category.name}'")
            return self


class DataBlock:
    def __init__(self, name: str, categories: Dict[str, Category]):
        self.name = name
        self._categories = categories  # Internal storage

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

    @property
    def categories(self) -> Dict[str, Category]:
        """Provides read-only access to the categories."""
        return self._categories

    def add_category(self, category_name: str, category: Category) -> None:
        """Adds a category to the data block."""
        self._categories[category_name] = category


class MMCIFDataContainer:
    def __init__(self, data_blocks: Dict[str, DataBlock]):
        self._data_blocks = data_blocks  # Internal storage

    def __getitem__(self, block_name: str) -> DataBlock:
        return self._data_blocks[block_name]

    def __setitem__(self, block_name: str, block: DataBlock) -> None:
        self._data_blocks[block_name] = block

    def __getattr__(self, block_name: str) -> DataBlock:
        try:
            return self._data_blocks[block_name]
        except KeyError:
            raise AttributeError(f"'MMCIFDataContainer' object has no attribute '{block_name}'")

    def __iter__(self):
        return iter(self._data_blocks.values())

    def __len__(self):
        return len(self._data_blocks)

    @property
    def data_blocks(self) -> Dict[str, DataBlock]:
        """Provides read-only access to the data blocks."""
        return self._data_blocks


class MMCIFReader:
    def __init__(self, atoms: bool, validator_factory: Optional[ValidatorFactory], categories: Optional[List[str]] = None):
        self.atoms = atoms
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

    def read(self, file_obj: IO) -> MMCIFDataContainer:
        try:
            for line in file_obj:
                self._process_line(line.rstrip())
        except IndexError as e:
            print(f"Error reading file: list index out of range - {e}")
        except KeyError as e:
            print(f"Missing data block or category: {e}")
        except Exception as e:
            print(f"Error reading file: {e}")

        return MMCIFDataContainer(self._data_blocks)

    def _process_line(self, line: str):
        if line.startswith('#'):
            return
        if line.startswith('data_'):
            self._start_new_data_block(line)
        elif line.startswith('loop_'):
            self._start_loop()
        elif line.startswith('_'):
            try:
                self._process_item_simple(line)
            except ValueError:
                self._process_item_fallback(line)
        elif self._in_loop:
            self._process_loop_data(line)
        elif self._multi_line_value:
            self._handle_multi_line_value(line)

    def _start_new_data_block(self, line: str):
        self._current_block = line.split('_', 1)[1]
        self._data_blocks[self._current_block] = DataBlock(self._current_block, {})
        self._current_category = None
        self._in_loop = False

    def _start_loop(self):
        self._in_loop = True
        self._loop_items = []

    def _process_item_simple(self, line: str):
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise ValueError("Invalid key-value pair")

        item_full, value = parts
        category, item = item_full.split('.', 1)
        if category.startswith('_atom_site') and not self.atoms:
            return

        if self.categories and category not in self.categories:
            return

        self._set_current_category(category)
        self._handle_single_item_value(item, value.strip())

    def _process_item_fallback(self, line: str):
        item_full = line.split(' ', 1)[0]
        category, item = item_full.split('.', 1)
        if category.startswith('_atom_site') and not self.atoms:
            return

        if self.categories and category not in self.categories:
            return

        if self._in_loop:
            self._loop_items.append(item_full)
            self._set_current_category(category)
        else:
            value = line[len(item_full):].strip()
            self._set_current_category(category)
            self._handle_single_item_value(item, value)

    def _process_loop_data(self, line: str):
        if not self._multi_line_value:
            self._handle_loop_values(line)
        else:
            self._handle_multi_line_value(line)

    def _handle_loop_values(self, line: str):
        values = line.split()
        while len(self._current_row_values) < len(self._loop_items) and values:
            value = values.pop(0)
            if value.startswith(';'):
                self._multi_line_value = True
                self._multi_line_item_name = self._loop_items[len(self._current_row_values)].split('.', 1)[1]
                self._multi_line_value_buffer.append(value[1:])
                self._current_row_values.append(None)
                break
            else:
                self._current_row_values.append(value)
                self._value_counter += 1

        if self._value_counter == len(self._loop_items):
            self._add_loop_values_to_category()

    def _handle_multi_line_value(self, line: str):
        if line == ';':
            self._multi_line_value = False
            full_value = "\n".join(self._multi_line_value_buffer)
            self._current_row_values[-1] = full_value
            self._multi_line_value_buffer = []
            self._value_counter += 1
            if self._value_counter == len(self._loop_items):
                self._add_loop_values_to_category()
        else:
            self._multi_line_value_buffer.append(line)

    def _add_loop_values_to_category(self):
        for i, value in enumerate(self._current_row_values):
            item_name = self._loop_items[i].split('.', 1)[1]
            self._current_data.add_item(item_name, value)
        self._current_row_values = []
        self._value_counter = 0

    def _set_current_category(self, category: str):
        if self._current_category != category:
            self._current_category = category
            if self._current_category not in self._data_blocks[self._current_block]._categories:
                self._data_blocks[self._current_block]._categories[self._current_category] = Category(
                    self._current_category, self.validator_factory)
            self._current_data = self._data_blocks[self._current_block]._categories[self._current_category]

    def _handle_single_item_value(self, item: str, value: str):
        if value.startswith(';'):
            self._multi_line_value = True
            self._multi_line_item_name = item
            self._multi_line_value_buffer = []
        else:
            self._current_data.add_item(item, value)


class MMCIFWriter:
    def write(self, file_obj: IO, data_container: MMCIFDataContainer) -> None:
        try:
            for block_name, data_block in data_container.data_blocks.items():
                file_obj.write(f"data_{block_name}\n")
                file_obj.write("#\n")
                for category_name, category in data_block.categories.items():
                    if isinstance(category, Category):
                        self._write_category(file_obj, category_name, category)
                        file_obj.write("#\n")
        except IOError as e:
            print(f"Error writing to file: {e}")

    def _write_category(self, file_obj: IO, category_name: str, category: Category):
        items = category._items
        if len(items) > 1 and any(len(values) > 1 for values in items.values()):
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
        if '\n' in value or value.startswith(' ') or value.startswith('_') or value.startswith(';'):
            return f"\n;{value.strip()}\n;\n"
        return f"{value} "


class MMCIFHandler:
    def __init__(self, atoms: bool = False, validator_factory: Optional[ValidatorFactory] = None):
        self.atoms = atoms
        self.validator_factory = validator_factory
        self._reader = None
        self._writer = MMCIFWriter()
        self._file_obj = None

    def parse(self, filename: str, categories: Optional[List[str]] = None) -> MMCIFDataContainer:
        self._reader = MMCIFReader(self.atoms, self.validator_factory, categories)
        with open(filename, 'r') as f:
            return self._reader.read(f)

    def write(self, data_container: MMCIFDataContainer) -> None:
        if self._file_obj:
            self._writer.write(self._file_obj, data_container)
        else:
            raise IOError("File is not open for writing")

    @property
    def file_obj(self):
        return self._file_obj

    @file_obj.setter
    def file_obj(self, file_obj):
        self._file_obj = file_obj
