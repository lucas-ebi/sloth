# MMCIF Tools

This repository contains the implementation of tools for parsing and writing MMCIF files. The tools are designed to handle the complex data structures found in MMCIF files, providing a way to access and manipulate data through a simple API. The current focus is on parsing and writing functionality.

### Getting Started

Ensure you have the necessary dependencies installed. You can install them using:

```bash
pip install -r requirements.txt
```

### Basic Usage

Below is an example demonstrating how to parse an MMCIF file and access its contents using dot-separated notation, and how to write updates back to a new MMCIF file.

```python
>>> from mmcif_tools import MMCIFHandler

# Parse the CIF file
>>> handler = MMCIFHandler()
>>> file = handler.parse("/Users/lucas/Desktop/em/emd_33233_md.cif")

>>> file.data_blocks
{'7XJP': <mmcif_tools.DataBlock object at 0x7f8ab0263160>}

# Accessing the DataBlock named '7XJP'
>>> data = getattr(file, '7XJP') # Python doesn't allow such a syntax like `file.7XJP`, then it has to be through `getattr`.

# Access the '_database_2' category and its items using dot-separated notation
>>> data._database_2.items
['database_id', 'database_code', 'pdbx_database_accession', 'pdbx_DOI']

>>> data._database_2.database_id
['PDB', 'WWPDB', 'EMDB']

>>> data._database_2.database_code
['7XJP', 'D_1300028976', 'EMD-33233']

>>> data._database_2.pdbx_database_accession
['pdb_00007xjp', '?', '?']

>>> data._database_2.pdbx_DOI
['10.2210/pdb7xjp/pdb', '?', '?']

# Update values in an item
>>> data._database_2.database_id[-1] = 'NEWDB'
>>> data._database_2.database_id
['PDB', 'WWPDB', 'NEWDB']

# Write updated content in mmCIF format
>>> handler.write("/Users/lucas/Desktop/em/modified_emd_33233_md.cif", file)
```

### Classes and Methods

#### `MMCIFHandler`

- **`parse(filename: str) -> MMCIFDataContainer`**: Parses an MMCIF file and returns an `MMCIFDataContainer` object containing the parsed contents.
- **`write(filename: str, data_container: MMCIFDataContainer)`**: Writes the contents of an `MMCIFDataContainer` object to a file in MMCIF format.

#### `MMCIFReader`

- **`read(filename: str) -> MMCIFDataContainer`**: Reads an MMCIF file and returns an `MMCIFDataContainer` object containing the parsed contents.

#### `MMCIFWriter`

- **`write(filename: str, data_container: MMCIFDataContainer)`**: Writes the contents of an `MMCIFDataContainer` object to a file in MMCIF format.

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
