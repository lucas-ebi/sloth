# MMCIF Tools

This repository contains the implementation of tools for parsing and writing MMCIF files. The tools are designed to handle the complex data structures found in MMCIF files, providing a way to access and manipulate data through a simple API. The current focus is on parsing and writing functionality.

### Getting Started

Ensure you have the necessary dependencies installed. You can install them using:

```bash
pip install -r requirements.txt
```

### Basic Usage

#### Parse the CIF file

```python
from mmcif_tools import MMCIFHandler

handler = MMCIFHandler()
file = handler.parse("/Users/lucas/Desktop/em/emd_33233_md.cif")
```

#### Access the DataBlock

```python
file.data_blocks
# Output: {'7XJP': <mmcif_tools.DataBlock object at 0x7f8ab0263160>}

data = getattr(file, '7XJP')  # Accessing the DataBlock named '7XJP'
```

#### Access the '_database_2' category and its items

```python
data._database_2.items
# Output: {'database_id': ['PDB', 'WWPDB', 'EMDB'], 'database_code': ['7XJP', 'D_1300028976', 'EMD-33233'], 'pdbx_database_accession': ['pdb_00007xjp', '?', '?'], 'pdbx_DOI': ['10.2210/pdb7xjp/pdb', '?', '?']}


data._database_2.database_id
# Output: ['PDB', 'WWPDB', 'EMDB']

data._database_2.database_code
# Output: ['7XJP', 'D_1300028976', 'EMD-33233']

data._database_2.pdbx_database_accession
# Output: ['pdb_00007xjp', '?', '?']

data._database_2.pdbx_DOI
# Output: ['10.2210/pdb7xjp/pdb', '?', '?']
```

#### Update values in an item

```python
data._database_2.database_id[-1] = 'NEWDB'
data._database_2.database_id
# Output: ['PDB', 'WWPDB', 'NEWDB']
```

#### Write updated content in mmCIF format

```python
# Write the updated content back to a new mmCIF file
with open("/Users/lucas/Desktop/em/modified_emd_33233_md.cif", 'w') as f:
    handler.file_obj = f
    handler.write(data_container)
```

### Classes and Methods

#### `MMCIFHandler`

- **`parse(filename: str) -> MMCIFDataContainer`**: Parses an MMCIF file and returns an `MMCIFDataContainer` object containing the parsed contents.
- **`write(data_container: MMCIFDataContainer)`**: Writes the contents of an `MMCIFDataContainer` object to an open file in MMCIF format. Ensure to open the file using `open_file` method before calling this.
- **`open_file(filename: str, mode: str) -> None`**: Opens a file with the specified mode ('r+', 'w', etc.) for reading and writing.
- **`close_file() -> None`**: Closes the currently open file.

#### `MMCIFReader`

- **`read(file_obj: IO) -> MMCIFDataContainer`**: Reads an MMCIF file from a file object and returns an `MMCIFDataContainer` object containing the parsed contents.

#### `MMCIFWriter`

- **`write(file_obj: IO, data_container: MMCIFDataContainer)`**: Writes the contents of an `MMCIFDataContainer` object to a file object in MMCIF format.

#### `MMCIFDataContainer`

- **Attributes**:
  - **`data_blocks`**: Provides read-only access to the data blocks, which are instances of `DataBlock`.

#### `DataBlock`

- **Attributes**:
  - **`categories`**: Provides read-only access to the categories, which are instances of `Category`.

#### `Category`

- **Attributes**:
  - **`items`**: Provides a list of item names available in the category.
  - **Access Items**: Items can be accessed using dot-separated notation, e.g., `data_block.category_name.item_name`.

#### `ValidatorFactory`

- **Methods**:
  - **`register_validator(category_name: str, validator_function: Callable[[str], None])`**: Registers a validator function for a specific category.
  - **`register_cross_checker(category_pair: Tuple[str, str], cross_checker_function: Callable[[str, str], None])`**: Registers a cross-checker function for a pair of categories.

### Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your improvements or bug fixes.

### License

This project is licensed under the MIT License.

---

**Note**: This README provides a basic overview and usage example. For more detailed documentation, refer to the source code and inline comments.
