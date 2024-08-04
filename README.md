# MMCIF Tools

This repository contains the implementation of tools for parsing MMCIF files. The tools are designed to handle the complex data structures found in MMCIF files, providing a way to access and manipulate data through a simple API. The current focus is on parsing functionality.

### Getting Started

Ensure you have the necessary dependencies installed. You can install them using:

```bash
pip install -r requirements.txt
```

### Basic Usage

Below is an example demonstrating how to parse an MMCIF file and access its contents using dot-separated notation.

```python
>>> from mmcif_tools import MMCIFReader

# Parse the CIF file
>>> reader = MMCIFReader()
>>> file = reader.parse("/Users/lucas/Desktop/em/emd_33233_md.cif")

# Accessing the DataBlock named '7XJP'
>>> data = getattr(file, '7XJP')

# Access the '_database_2' category and its items using dot-separated notation
>>> print("Category items:", data._database_2.items)
Category items: ['database_id', 'database_code', 'pdbx_database_accession', 'pdbx_DOI']

>>> print("Database IDs:", data._database_2.database_id)
Database IDs: ['PDB', 'WWPDB', 'EMDB']

>>> print("Database Codes:", data._database_2.database_code)
Database Codes: ['7XJP', 'D_1300028976', 'EMD-33233']

>>> print("PDBx Database Accessions:", data._database_2.pdbx_database_accession)
PDBx Database Accessions: ['pdb_00007xjp', '?', '?']

>>> print("DOIs:", data._database_2.pdbx_DOI)
DOIs: ['10.2210/pdb7xjp/pdb', '?', '?']

# Update values in an item
>>> data._database_2.database_id[-1] = 'NEWDB'
>>> print("Updated Database IDs:", data._database_2.database_id)
Updated Database IDs: ['PDB', 'WWPDB', 'NEWDB']
```

### Classes and Methods

#### `MMCIFReader`

- **`parse(filename: str) -> MMCIFDataContainer`**: Parses an MMCIF file and returns an `MMCIFDataContainer` object containing the parsed contents.

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
