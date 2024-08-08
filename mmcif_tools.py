from typing import Callable, Dict, Tuple, List, Any, Union, Optional, IO
import io

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
        """A class to validate a category."""
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
            """
            Cross-checks the current category against another category.

            :param other_category: The other category to cross-check against.
            :type other_category: Category
            :return: The validator object.
            :rtype: Category.Validator
            """
            self.other_category = other_category
            cross_checker = self.factory.get_cross_checker((self.category.name, other_category.name))
            if cross_checker:
                cross_checker(self.category.name, other_category.name)
            else:
                print(f"No cross-checker registered for categories '{self.category.name}' and '{other_category.name}'")
            return self

class DataBlock:
    """A class to represent a data block in an mmCIF file."""
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
    """A class to represent an mmCIF data container."""
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
    """A class to read an mmCIF file and return a data container."""
    
    def __init__(self, atoms: bool, validator_factory: Optional[ValidatorFactory], categories: Optional[List[str]] = None):
        """
        Initializes the MMCIFReader.

        :param atoms: Flag indicating whether to include atom site data.
        :type atoms: bool
        :param validator_factory: Factory for creating validators.
        :type validator_factory: Optional[ValidatorFactory]
        :param categories: List of categories to read, reads all if None.
        :type categories: Optional[List[str]]
        """
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
        """
        Reads an mmCIF file and returns a data container.

        Overview:
        This method reads through the mmCIF file line by line, processing each line to extract 
        data blocks, categories, and items. It handles simple items, loops, and multi-line values.

        Pseudocode:
        - Initialize an empty data container.
        - For each line in the file:
            - Process the line based on its type (data block, loop, item, etc.).
            - Add the processed data to the appropriate structures.
        - Return the populated data container.

        :param file_obj: The file object to read from.
        :type file_obj: IO
        :return: The data container.
        :rtype: MMCIFDataContainer
        """
        try:
            for line in file_obj:
                # Process the line based on its type (data block, loop, item, etc.).
                self._process_line(line.rstrip())
        except IndexError as e:
            print(f"Error reading file: list index out of range - {e}")
        except KeyError as e:
            print(f"Missing data block or category: {e}")
        except Exception as e:
            print(f"Error reading file: {e}")

        # Return the populated data container.
        return MMCIFDataContainer(self._data_blocks)

    def _process_line(self, line: str) -> None:
        """ 
        Processes a line from the mmCIF file.

        Overview:
        This method determines the type of the given line and calls the appropriate handler 
        function to process the line and extract relevant data.

        Pseudocode:
        - If the line is a comment, ignore it.
        - If the line starts a new data block, call _start_new_data_block.
        - If the line starts a loop, call _start_loop.
        - If the line defines an item, process the item.
        - If currently in a loop, process the loop data.
        - If handling a multi-line value, continue handling it.

        :param line: The line to process.
        :type line: str
        :return: None
        """
        if line.startswith('#'):
            return  # Ignore comments
        if line.startswith('data_'):
            # If the line starts a new data block, call _start_new_data_block.
            self._start_new_data_block(line)
        elif line.startswith('loop_'):
            # If the line starts a loop, call _start_loop.
            self._start_loop()
        elif line.startswith('_'):
            try:
                # If the line defines an item, process the item.
                self._process_item_simple(line)
            except ValueError:
                self._process_item_fallback(line)  # Process an item with fallback method
        elif self._in_loop:
            # If currently in a loop, process the loop data.
            self._process_loop_data(line)
        elif self._multi_line_value:
            # If handling a multi-line value, continue handling it.
            self._handle_multi_line_value(line)

    def _start_new_data_block(self, line: str) -> None:
        """
        Starts a new data block.

        Overview:
        This method initializes a new data block and resets the current category and loop status.

        Pseudocode:
        - Extract the data block name from the line.
        - Create a new DataBlock object and add it to the data blocks dictionary.
        - Reset the current category and loop status.

        :param line: The line containing the data block name.
        :type line: str
        :return: None
        """
        # Extract the data block name from the line.
        self._current_block = line.split('_', 1)[1]
        # Create a new DataBlock object and add it to the data blocks dictionary.
        self._data_blocks[self._current_block] = DataBlock(self._current_block, {})
        # Reset the current category and loop status.
        self._current_category = None
        self._in_loop = False

    def _start_loop(self) -> None:
        """
        Starts a loop.

        Overview:
        This method sets the loop status to true and initializes the loop items list.

        Pseudocode:
        - Set the loop status to true.
        - Initialize the loop items list.

        :return: None
        """
        # Set the loop status to true.
        self._in_loop = True
        # Initialize the loop items list.
        self._loop_items = []

    def _process_item_simple(self, line: str) -> None:
        """
        Processes a simple item line.

        Overview:
        This method extracts the category and item from the line, validates them, and adds the item
        to the current category.

        Pseudocode:
        - Split the line into the item and value.
        - Extract the category and item from the item name.
        - If the category is not in the list of categories to read (if specified), skip it.
        - Set the current category.
        - Add the item value to the current category.

        :param line: The line to process.
        :type line: str
        :return: None
        """
        # Split the line into the item and value.
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise ValueError("Invalid key-value pair")

        item_full, value = parts
        # Extract the category and item from the item name.
        category, item = item_full.split('.', 1)
        if category.startswith('_atom_site') and not self.atoms:
            return

        if self.categories and category not in self.categories:
            return

        # Set the current category.
        self._set_current_category(category)
        # Add the item value to the current category.
        self._handle_single_item_value(item, value.strip())

    def _process_item_fallback(self, line: str) -> None:
        """
        Processes an item line with a fallback method if the simple method fails due to loop or multi-line values.

        Overview:
        This method handles cases where the simple item processing fails, such as when dealing with
        looped items or multi-line values.

        Pseudocode:
        - Extract the full item name.
        - Extract the category and item from the item name.
        - If the category is not in the list of categories to read (if specified), skip it.
        - If in a loop, add the item to the loop items list and set the current category.
        - Otherwise, add the single item value to the current category.

        :param line: The line to process.
        :type line: str
        :return: None
        """
        # Extract the full item name.
        item_full = line.split(' ', 1)[0]
        # Extract the category and item from the item name.
        category, item = item_full.split('.', 1)
        if category.startswith('_atom_site') and not self.atoms:
            return

        if self.categories and category not in self.categories:
            return

        if self._in_loop:
            # If in a loop, add the item to the loop items list and set the current category.
            self._loop_items.append(item_full)
            self._set_current_category(category)
        else:
            # Otherwise, add the single item value to the current category.
            value = line[len(item_full):].strip()
            self._set_current_category(category)
            self._handle_single_item_value(item, value)

    def _process_loop_data(self, line: str) -> None:
        """
        Processes a line of data in a loop.

        Overview:
        This method handles the processing of looped data lines, adding values to the current row
        and managing multi-line values.

        Pseudocode:
        - If not handling a multi-line value, process loop values.
        - Otherwise, handle the multi-line value continuation.

        :param line: The line to process.
        :type line: str
        :return: None
        """
        if not self._multi_line_value:
            # If not handling a multi-line value, process loop values.
            self._handle_loop_values(line)
        else:
            # Otherwise, handle the multi-line value continuation.
            self._handle_multi_line_value(line)

    def _handle_loop_values(self, line: str) -> None:
        """
        Handles the values in a loop.

        Overview:
        This method processes the values within a loop, managing multi-line values and adding them
        to the current row.

        Pseudocode:
        - Split the line into values.
        - While there are still items in the loop and values to process:
            - If a value starts a multi-line value, handle it accordingly.
            - Otherwise, add the value to the current row.
        - If the current row is complete, add it to the category.

        :param line: The line to process.
        :type line: str
        :return: None
        """
        # Split the line into values.
        values = line.split()
        while len(self._current_row_values) < len(self._loop_items) and values:
            value = values.pop(0)
            if value.startswith(';'):
                # If a value starts a multi-line value, handle it accordingly.
                self._multi_line_value = True
                self._multi_line_item_name = self._loop_items[len(self._current_row_values)].split('.', 1)[1]
                self._multi_line_value_buffer.append(value[1:])
                self._current_row_values.append(None)
                break
            else:
                # Otherwise, add the value to the current row.
                self._current_row_values.append(value)
                self._value_counter += 1

        if self._value_counter == len(self._loop_items):
            # If the current row is complete, add it to the category.
            self._add_loop_values_to_category()

    def _handle_multi_line_value(self, line: str) -> None:
        """
        Handles a multi-line value.

        Overview:
        This method processes the continuation of a multi-line value, adding lines to the buffer
        until the multi-line value is complete.

        Pseudocode:
        - If the line ends the multi-line value, finalize it.
        - Otherwise, add the line to the multi-line buffer.

        :param line: The line to process.
        :type line: str
        :return: None
        """
        if line == ';':
            # If the line ends the multi-line value, finalize it.
            self._multi_line_value = False
            full_value = "\n".join(self._multi_line_value_buffer)
            self._current_row_values[-1] = full_value
            self._multi_line_value_buffer = []
            self._value_counter += 1
            if self._value_counter == len(self._loop_items):
                self._add_loop_values_to_category()
        else:
            # Otherwise, add the line to the multi-line buffer.
            self._multi_line_value_buffer.append(line)

    def _add_loop_values_to_category(self) -> None:
        """
        Adds the values in the current row to the current category.

        Overview:
        This method finalizes the processing of the current row of looped values, adding each value
        to the appropriate item in the current category.

        Pseudocode:
        - For each value in the current row:
            - Add the value to the corresponding item in the current category.
        - Reset the current row and value counter.

        :return: None
        """
        # For each value in the current row:
        for i, value in enumerate(self._current_row_values):
            # Add the value to the corresponding item in the current category.
            item_name = self._loop_items[i].split('.', 1)[1]
            self._current_data.add_item(item_name, value)
        # Reset the current row and value counter.
        self._current_row_values = []
        self._value_counter = 0

    def _set_current_category(self, category: str) -> None:
        """
        Sets the current category.

        Overview:
        This method updates the current category being processed, creating a new Category object
        if necessary.

        Pseudocode:
        - If the current category is different from the specified category:
            - Update the current category.
            - If the category does not exist in the current data block, create it.
            - Set the current data to the specified category.

        :param category: The category name.
        :type category: str
        :return: None
        """
        # If the current category is different from the specified category:
        if self._current_category != category:
            # Update the current category.
            self._current_category = category
            # If the category does not exist in the current data block, create it.
            if self._current_category not in self._data_blocks[self._current_block]._categories:
                self._data_blocks[self._current_block]._categories[self._current_category] = Category(
                    self._current_category, self.validator_factory)
            # Set the current data to the specified category.
            self._current_data = self._data_blocks[self._current_block]._categories[self._current_category]

    def _handle_single_item_value(self, item: str, value: str) -> None:
        """
        Handles a single item value.

        Overview:
        This method processes a single item value, handling multi-line values if necessary and
        adding the value to the current category.

        Pseudocode:
        - If the value starts a multi-line value, initialize the buffer.
        - Otherwise, add the value to the current category.

        :param item: The item name.
        :type item: str
        :param value: The item value.
        :type value: str
        :return: None
        """
        if value.startswith(';'):
            # If the value starts a multi-line value, initialize the buffer.
            self._multi_line_value = True
            self._multi_line_item_name = item
            self._multi_line_value_buffer = []
        else:
            # Otherwise, add the value to the current category.
            self._current_data.add_item(item, value)


class MMCIFWriter:
    """A class to write an mmCIF data container to a file."""
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
    def __init__(self, atoms: bool = False, validator_factory: Optional[ValidatorFactory] = None):
        self.atoms = atoms
        self.validator_factory = validator_factory
        self._reader = None
        self._writer = MMCIFWriter()
        self._file_obj = None

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
        self._reader = MMCIFReader(self.atoms, self.validator_factory, categories)
        with open(filename, 'r') as f:
            return self._reader.read(f)

    def write(self, data_container: MMCIFDataContainer) -> None:
        """
        Writes a data container to a file.

        :param data_container: The data container to write.
        :type data_container: MMCIFDataContainer
        :return: None
        """
        if self._file_obj:
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
