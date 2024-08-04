# mmcif_tools.py

from typing import Callable, Dict, Tuple, List, Any, Union, Optional
import json
import pickle
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement, tostring
import msgpack
from enum import Enum, auto

# Enum for file format
class Format(Enum):
    JSON = auto()
    XML = auto()
    PICKLE = auto()
    MMCIF_BINARY = auto()


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


class MMCIFReader:
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

    def load(self, data: Union[str, bytes], format: Format) -> MMCIFDataContainer:
        if format == Format.JSON:
            return MMCIFContentLoader.from_json(data, self.validator_factory)
        elif format == Format.XML:
            return MMCIFContentLoader.from_xml(data, self.validator_factory)
        elif format == Format.PICKLE:
            return MMCIFContentLoader.from_pickle(data, self.validator_factory)
        elif format == Format.MMCIF_BINARY:
            return MMCIFContentLoader.from_mmcif_binary(data, self.validator_factory)
        else:
            raise ValueError(f"Unsupported format: {format}")


class MMCIFContentLoader:
    @staticmethod
    def from_json(json_str: str, validator_factory: Optional[ValidatorFactory], atoms: bool = False) -> MMCIFDataContainer:
        data_dict = json.loads(json_str)
        data_blocks = {}
        for block_name, categories_dict in data_dict.items():
            categories = {}
            for cat_name, items in categories_dict.items():
                # Skip atomic coordinate categories if atoms is False
                if not atoms and cat_name.startswith('_atom_site'):
                    continue
                category = Category(cat_name, validator_factory)
                for item_name, value in items.items():
                    setattr(category, item_name, value)
                categories[cat_name] = category
            data_blocks[block_name] = DataBlock(block_name, categories)
        return MMCIFDataContainer(data_blocks)

    @staticmethod
    def from_xml(xml_str: str, validator_factory: Optional[ValidatorFactory], atoms: bool = False) -> MMCIFDataContainer:
        root = ElementTree.fromstring(xml_str)
        data_blocks = {}
        for block_elem in root.findall('DataBlock'):
            block_name = block_elem.get('name')
            categories = {}
            for cat_elem in block_elem.findall('Category'):
                cat_name = cat_elem.get('name')
                # Skip atomic coordinate categories if atoms is False
                if not atoms and cat_name.startswith('_atom_site'):
                    continue
                category = Category(cat_name, validator_factory)
                for item_elem in cat_elem.findall('Item'):
                    item_name = item_elem.get('name')
                    value = item_elem.text
                    if item_name not in category.items:
                        category.items[item_name] = []
                    category.items[item_name].append(value)
                categories[cat_name] = category
            data_blocks[block_name] = DataBlock(block_name, categories)
        return MMCIFDataContainer(data_blocks)

    @staticmethod
    def from_pickle(pickle_data: bytes, validator_factory: Optional[ValidatorFactory], atoms: bool = False) -> MMCIFDataContainer:
        data_dict = pickle.loads(pickle_data)
        data_blocks = {}
        for block_name, categories_dict in data_dict.items():
            categories = {}
            for cat_name, items in categories_dict.items():
                # Skip atomic coordinate categories if atoms is False
                if not atoms and cat_name.startswith('_atom_site'):
                    continue
                category = Category(cat_name, validator_factory)
                for item_name, value in items.items():
                    setattr(category, item_name, value)
                categories[cat_name] = category
            data_blocks[block_name] = DataBlock(block_name, categories)
        return MMCIFDataContainer(data_blocks)

    @staticmethod
    def from_mmcif_binary(binary_data: bytes, validator_factory: Optional[ValidatorFactory], atoms: bool = False) -> MMCIFDataContainer:
        unpacked_data = msgpack.unpackb(binary_data, raw=False)
        data_blocks = {}

        for block_data in unpacked_data['dataBlocks']:
            block_name = block_data['header']
            categories = {}
            
            for cat_data in block_data['categories']:
                cat_name = cat_data['name']
                # Skip atomic coordinate categories if atoms is False
                if not atoms and cat_name.startswith('_atom_site'):
                    continue
                category = Category(cat_name, validator_factory)
                column_data_list = cat_data['columns']
                
                for col_data in column_data_list:
                    col_name = col_data['name']
                    encoded_data = col_data['data']['data']
                    encodings = col_data['data']['encoding']
                    decoded_data = MMCIFContentLoader._decode_data(encoded_data, encodings)
                    category.items[col_name] = decoded_data
                
                categories[cat_name] = category
            
            data_blocks[block_name] = DataBlock(block_name, categories)

        return MMCIFDataContainer(data_blocks)

    @staticmethod
    def _decode_data(encoded_data, encodings):
        data = encoded_data
        for encoding in reversed(encodings):
            if encoding['kind'] == 'ByteArray':
                data = bytearray(data)
            elif encoding['kind'] == 'FixedPoint':
                factor = encoding['factor']
                data = [x / factor for x in data]
            elif encoding['kind'] == 'IntervalQuantization':
                min_val = encoding['min']
                max_val = encoding['max']
                num_steps = encoding['numSteps']
                step_size = (max_val - min_val) / num_steps
                data = [min_val + x * step_size for x in data]
            elif encoding['kind'] == 'RunLength':
                data = MMCIFContentLoader._decode_run_length(data, encoding)
            elif encoding['kind'] == 'Delta':
                origin = encoding['origin']
                data = [origin + sum(data[:i+1]) for i in range(len(data))]
            elif encoding['kind'] == 'IntegerPacking':
                pass
            elif encoding['kind'] == 'StringArray':
                pass
        return data

    @staticmethod
    def _decode_run_length(data, encoding):
        src_size = encoding['srcSize']
        decoded = []
        for i in range(0, len(data), 2):
            value, count = data[i], data[i+1]
            decoded.extend([value] * count)
        return decoded[:src_size]


class MMCIFWriter:
    def __init__(self, data_container: MMCIFDataContainer, atoms: bool = True):
        self.data_container = data_container
        self.atoms = atoms

    def write(self, filename: str) -> None:
        try:
            with open(filename, 'w') as f:
                for block_name, block in self.data_container.data_blocks.items():
                    f.write(f"data_{block_name}\n")
                    for category_name, category in block.categories.items():
                        if not self.atoms and category_name.startswith('_atom_site'):
                            continue
                        self._write_category(f, category_name, category)
            print(f"File written successfully to {filename}")
        except Exception as e:
            print(f"Error writing file {filename}: {e}")

    def _write_category(self, f: Any, category_name: str, category: Category) -> None:
        is_loop = any(isinstance(v, list) for v in category.items.values())

        if is_loop:
            self._write_loop(f, category_name, category)
        else:
            self._write_simple_items(f, category)

    def _write_simple_items(self, f: Any, category: Category) -> None:
        for item_name, value in category.items.items():
            if isinstance(value, str) and '\n' in value:
                f.write(f"{item_name}\n;\n{value}\n;\n")
            else:
                f.write(f"{item_name} {value}\n")

    def _write_loop(self, f: Any, category_name: str, category: Category) -> None:
        f.write(f"loop_\n")
        items = list(category.items.keys())
        for item_name in items:
            f.write(f"{item_name}\n")

        num_entries = max(len(val) if isinstance(val, list) else 1 for _, val in category.items.items())
        for i in range(num_entries):
            line = " ".join(
                str(category.items[item_name][i]) if category.items[item_name][i] is not None else '?'
                for item_name in items
            )
            f.write(line + "\n")

    def export(self, format: Format) -> Union[str, bytes]:
        if format == Format.JSON:
            exporter = JSONExporter()
        elif format == Format.XML:
            exporter = XMLExporter()
        elif format == Format.PICKLE:
            exporter = PickleExporter()
        elif format == Format.MMCIF_BINARY:
            exporter = MMCIFBinaryExporter()
        else:
            raise ValueError(f"Unsupported format: {format}")

        return exporter.export(self.data_container)


class Exporter:
    def export(self, data_container: MMCIFDataContainer) -> Union[str, bytes]:
        raise NotImplementedError("Export method must be implemented by subclasses.")


class JSONExporter(Exporter):
    def export(self, data_container: MMCIFDataContainer) -> str:
        data_dict = {}
        for block_name, block in data_container.data_blocks.items():
            block_dict = {}
            for cat_name, cat in block.categories.items():
                block_dict[cat_name] = cat.items
            data_dict[block_name] = block_dict
        return json.dumps(data_dict, indent=2)


class XMLExporter(Exporter):
    def export(self, data_container: MMCIFDataContainer) -> str:
        root = Element('MMCIFDataContainer')
        for block_name, block in data_container.data_blocks.items():
            block_elem = SubElement(root, 'DataBlock', name=block_name)
            for cat_name, cat in block.categories.items():
                cat_elem = SubElement(block_elem, 'Category', name=cat_name)
                for item_name, value in cat.items.items():
                    if isinstance(value, list):
                        for val in value:
                            SubElement(cat_elem, 'Item', name=item_name).text = str(val)
                    else:
                        SubElement(cat_elem, 'Item', name=item_name).text = str(value)
        return tostring(root, 'utf-8').decode('utf-8')


class PickleExporter(Exporter):
    def export(self, data_container: MMCIFDataContainer) -> bytes:
        data_dict = {}
        for block_name, block in data_container.data_blocks.items():
            block_dict = {}
            for cat_name, cat in block.categories.items():
                block_dict[cat_name] = cat.items
            data_dict[block_name] = block_dict
        return pickle.dumps(data_dict)


class MMCIFBinaryExporter(Exporter):
    def export(self, data_container: MMCIFDataContainer) -> bytes:
        data_blocks = []

        for block_name, block in data_container.data_blocks.items():
            categories = []
            
            for cat_name, cat in block.categories.items():
                columns = []
                for col_name, values in cat.items.items():
                    encoded_data, encodings = self._encode_column_data(values)
                    columns.append({
                        'name': col_name,
                        'data': {'data': encoded_data, 'encoding': encodings},
                        'mask': None  
                    })
                
                categories.append({
                    'name': cat_name,
                    'rowCount': len(values),
                    'columns': columns
                })
            
            data_blocks.append({
                'header': block_name,
                'categories': categories
            })

        file_structure = {
            'version': '1.0',
            'encoder': 'CustomEncoder',
            'dataBlocks': data_blocks
        }
        return msgpack.packb(file_structure, use_bin_type=True)

    def _encode_column_data(self, values):
        encodings = []
        encoded_data = values

        delta_encoded = self._apply_delta_encoding(encoded_data)
        run_length_encoded = self._apply_run_length_encoding(delta_encoded['data'])
        packed_data = self._apply_integer_packing(run_length_encoded)

        encodings.append({'kind': 'Delta', 'origin': delta_encoded['origin']})
        encodings.append({'kind': 'RunLength', 'srcSize': len(run_length_encoded)})
        encodings.append({
            'kind': 'IntegerPacking', 
            'byteCount': packed_data['byteCount'], 
            'srcSize': len(run_length_encoded),
            'isUnsigned': packed_data['isUnsigned']
        })
        encoded_data = packed_data['data']

        return encoded_data, encodings

    def _apply_delta_encoding(self, data):
        if not data:
            return {'origin': 0, 'data': []}
        origin = data[0]
        delta_encoded = [data[i] - data[i - 1] for i in range(1, len(data))]
        return {'origin': origin, 'data': delta_encoded}

    def _apply_run_length_encoding(self, data):
        if not data:
            return []
        encoded = []
        current_value = data[0]
        count = 0
        for value in data:
            if value == current_value:
                count += 1
            else:
                encoded.extend([current_value, count])
                current_value = value
                count = 1
        encoded.extend([current_value, count])
        return encoded

    def _apply_integer_packing(self, data):
        if not data:
            return {'data': bytearray(), 'byteCount': 1, 'isUnsigned': True}

        min_val = min(data)
        max_val = max(data)

        is_unsigned = min_val >= 0
        if is_unsigned:
            max_val = max(0, max_val)
        else:
            max_val = max(abs(min_val), max_val)

        byte_count = 1
        if max_val < 0x80:
            byte_count = 1
        elif max_val < 0x8000:
            byte_count = 2
        elif max_val < 0x80000000:
            byte_count = 4

        fmt = f'{len(data)}{"B" if is_unsigned else "b"}' if byte_count == 1 else f'{len(data)}{"H" if is_unsigned else "h"}' if byte_count == 2 else f'{len(data)}{"I" if is_unsigned else "i"}'
        packed_data = struct.pack(f'<{fmt}', *data)

        return {'data': packed_data, 'byteCount': byte_count, 'isUnsigned': is_unsigned}
