# MMCIF Tools

This repository contains the implementation of MMCIF tools for parsing, validating, and writing MMCIF files. The tools are designed to work with various file formats including JSON, XML, Pickle, and MMCIF Binary. Below are examples and explanations of how to use the provided classes and methods.

### Getting Started

To begin, ensure you have the necessary dependencies installed. You can install them using:

```bash
pip install -r requirements.txt
```

### Basic Usage

Here is a simple example demonstrating how to parse an MMCIF file and access its contents.

```python
>>> from mmcif_tools import MMCIFReader
>>> file = reader.parse("emd_33233_md.cif")
>>> data = getattr(file, '7XJP')
>>> # Access the '_database_2' category
>>> data._database_2
<mmcif_tools.Category object at 0x7fbc0024f430>
>>> # Access item names of '_database_2' category
>>> data._database_2.items
['database_id', 'database_code', 'pdbx_database_accession', 'pdbx_DOI']
>>> # Access item values using dot-separated notation
>>> data._database_2.database_id
['PDB', 'WWPDB', 'EMDB']
>>> data._database_2.database_code
['7XJP', 'D_1300028976', 'EMD-33233']
>>> data._database_2.pdbx_database_accession
['pdb_00007xjp', '?', '?']
>>> data._database_2.pdbx_DOI
['10.2210/pdb7xjp/pdb', '?', '?']
>>> # Update values in an item
>>> data._database_2.database_id = ['PDB', 'NEWDB']
>>> data._database_2.database_id
['PDB', 'NEWDB']
>>> data._database_2.database_code = ['7XJP', 'NEW_CODE']
>>> data._database_2.database_code
['7XJP', 'NEW_CODE']
>>> data._database_2.pdbx_database_accession = ['pdb_00007xjp', 'new_accession']
>>> data._database_2.pdbx_database_accession
['pdb_00007xjp', 'new_accession']
>>> data._database_2.pdbx_DOI = ['10.2210/pdb7xjp/pdb', '10.1000/new.doi']
>>> data._database_2.pdbx_DOI
['10.2210/pdb7xjp/pdb', '10.1000/new.doi']
```

### Classes and Methods

#### `MMCIFReader`

- **`parse(filename: str) -> MMCIFDataContainer`**: Parses an MMCIF file and returns a data container with the parsed contents.

#### `MMCIFDataContainer`

- **Attributes**:
  - **`data_blocks`**: A dictionary containing `DataBlock` objects, each corresponding to a data block in the MMCIF file.

#### `DataBlock`

- **Attributes**:
  - **`categories`**: A dictionary containing `Category` objects, each corresponding to a category in the data block.

#### `Category`

- **Attributes**:
  - **`items`**: A dictionary containing item names and their corresponding list of values.

#### `ValidatorFactory`

- **`register_validator(category_name: str, validator_function: Callable[[str], None])`**: Registers a validator function for a specific category.
- **`register_cross_checker(category_pair: Tuple[str, str], cross_checker_function: Callable[[str, str], None])`**: Registers a cross-checker function for a pair of categories.

### Additional Features

- **Format Support**: The library supports various formats including JSON, XML, Pickle, and MMCIF Binary.
- **Validation**: The `ValidatorFactory` allows for the registration of validation functions to ensure the correctness of the data.

### Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

### License

This project is licensed under the MIT License.

---

**Note**: This README file provides a basic overview and usage example. For a detailed understanding, refer to the source code and inline documentation.
