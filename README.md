# MMCIF Tools

This repository contains the implementation of MMCIF tools for parsing, validating, and writing MMCIF files. The tools are designed to work with various file formats including JSON, XML, Pickle, and MMCIF Binary. Below are examples and explanations of how to use the provided classes and methods.

## Getting Started

### Basic Usage

Here is a simple example demonstrating how to parse an MMCIF file and access its contents.

```python
from mmcif_tools import MMCIFReader

# Create a reader instance
reader = MMCIFReader()

# Parse the MMCIF file
file = reader.parse("emd_33233_md.cif")

# Accessing the DataBlock named '7XJP'
data = getattr(file, '7XJP')

# Accessing the Category named '_database_2' within the DataBlock '7XJP'
print(data._database_2)

# List item names in '_database_2' category
print("Item names in '_database_2':", data._database_2.items)

# Accessing specific item values
for item in data._database_2.items:
    item_values = getattr(data._database_2, item)
    print(f"item {item} values: {item_values}")
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

---

**a.** Add unit tests for the MMCIFReader class.  
**b.** Extend the README with more detailed examples for writing MMCIF files using the `MMCIFWriter` class.
