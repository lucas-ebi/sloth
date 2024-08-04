# mmcif_tools.py

from typing import Callable, Dict, Tuple, List, Any, Union, Optional
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement, tostring


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
        # This will handle attribute access via dot notation
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


class MMCIFHandler:
    def __init__(self, atoms: bool = False, validator_factory: Optional[ValidatorFactory] = None):
        self.atoms: bool = atoms
        self.validator_factory: Optional[ValidatorFactory] = validator_factory

    def parse(self, filename: str) -> MMCIFDataContainer:
        data_blocks: Dict[str, DataBlock] = {}
        current_block: Optional[str] = None
        current_category: Optional[str] = None
        current_data: Optional[Category] = None
        loop_items = []
        in_loop = False
        multi_line_value = False
        multi_line_item_name = ""
        multi_line_value_buffer = []
        current_row_values = []
        value_counter = 0

        try:
            with open(filename, 'r') as f:
                for line in f:
                    line = line.rstrip()

                    if line.startswith('#'):
                        continue  # Skip comment lines

                    if line.startswith('data_'):
                        current_block = line.split('_', 1)[1]
                        data_blocks[current_block] = DataBlock(current_block, {})
                        current_category = None
                        continue

                    if line.startswith('loop_'):
                        in_loop = True
                        loop_items = []
                        continue

                    if line.startswith('_'):
                        item_full = line.split(' ', 1)[0]
                        category, item = item_full.split('.', 1)
                        if category.startswith('_atom_site') and not self.atoms:
                            continue

                        if in_loop:
                            loop_items.append(item_full)
                            if current_category != category:
                                current_category = category
                                if current_category not in data_blocks[current_block]._categories:
                                    data_blocks[current_block]._categories[current_category] = Category(
                                        current_category, self.validator_factory)
                                current_data = data_blocks[current_block]._categories[current_category]
                        else:
                            if current_category != category:
                                current_category = category
                                if current_category not in data_blocks[current_block]._categories:
                                    data_blocks[current_block]._categories[current_category] = Category(
                                        current_category, self.validator_factory)
                                current_data = data_blocks[current_block]._categories[current_category]

                            value = line[len(item_full):].strip()
                            if value.startswith(';'):
                                multi_line_value = True
                                multi_line_item_name = item
                                multi_line_value_buffer = []
                            else:
                                current_data.add_item(item, value)
                        continue

                    if in_loop:
                        if not multi_line_value:
                            values = line.split()
                            while len(current_row_values) < len(loop_items) and values:
                                value = values.pop(0)
                                if value.startswith(';'):
                                    multi_line_value = True
                                    multi_line_item_name = loop_items[len(current_row_values)].split('.', 1)[1]
                                    multi_line_value_buffer.append(value[1:])
                                    current_row_values.append(None)  # Placeholder for multi-line data
                                    break
                                else:
                                    current_row_values.append(value)
                                    value_counter += 1

                            if value_counter == len(loop_items):
                                for i, value in enumerate(current_row_values):
                                    item_name = loop_items[i].split('.', 1)[1]
                                    current_data.add_item(item_name, value)
                                current_row_values = []
                                value_counter = 0

                        else:
                            if line == ';':
                                multi_line_value = False
                                full_value = "\n".join(multi_line_value_buffer)
                                current_row_values[-1] = full_value  # Replace placeholder
                                multi_line_value_buffer = []
                                value_counter += 1

                                if value_counter == len(loop_items):
                                    for i, value in enumerate(current_row_values):
                                        item_name = loop_items[i].split('.', 1)[1]
                                        current_data.add_item(item_name, value)
                                    current_row_values = []
                                    value_counter = 0
                            else:
                                multi_line_value_buffer.append(line)

        except FileNotFoundError:
            print(f"File not found: {filename}")
        except IndexError as e:
            print(f"Error reading file {filename}: list index out of range - {e}")
        except KeyError as e:
            print(f"Missing data block or category: {e}")
        except Exception as e:
            print(f"Error reading file {filename}: {e}")

        return MMCIFDataContainer(data_blocks)

    def _parse_loop(self, f: Any, current_data: Category, loop_items: List[str]) -> None:
        for item in loop_items:
            setattr(current_data, item, [])

        for line in f:
            line = line.strip()
            if line.startswith('_'):
                break
            self._parse_loop_item(line, current_data)

    def _parse_loop_item(self, line: str, current_data: Category) -> None:
        items = line.split()
        for i, item in enumerate(current_data.items.keys()):
            if i < len(items):
                getattr(current_data, item).append(items[i])
            else:
                getattr(current_data, item).append(None)

    def _parse_item(self, line: str, current_data: Category) -> None:
        item, value = line.split(None, 1)
        item = '_' + item.rstrip()
        if not self.atoms or not item.startswith('_atom_site'):
            setattr(current_data, item, value.strip())