# 🦥 SLOTH

**S**tructural **L**oader with **O**n-demand **T**okenization and **H**andling

> 🧠 *Lazy by design. Fast by default.*

A Python library for parsing and writing mmCIF (macromolecular Crystallographic Information Framework) files with an ultra-simple API that's automatically optimized for performance.

![SLOTH: Lazy by Design, Fast by Default](https://img.shields.io/badge/SLOTH-Lazy%20by%20Design%2C%20Fast%20by%20Default-blueviolet?logo=python&logoColor=white)
[![PyPI version](https://badge.fury.io/py/sloth-mmcif.svg)](https://badge.fury.io/py/sloth-mmcif)
[![Python versions](https://img.shields.io/pypi/pyversions/sloth-mmcif.svg)](https://pypi.org/project/sloth-mmcif/)
[![Built in Python](https://img.shields.io/badge/Built%20in-Python.%20No%20Templates.%20No%20Regrets.-brightgreen?logo=python&logoColor=white)](https://python.org)
[![License](https://img.shields.io/github/license/lucas/sloth.svg)](https://github.com/lucas/sloth/blob/main/LICENSE)

**SLOTH** is a memory-mapped, lazily-loaded mmCIF parser written in pure Python.  
It loads what you need, when you need it — no more, no less.

## ✨ Key Features

🚀 **Simple API**: One way to create, one way to parse - always optimized  
⚡ **High Performance**: Automatically handles large files efficiently  
💾 **Memory Efficient**: Smart data loading and caching  
📦 **Complete**: Access to all mmCIF categories and items  
🔧 **Robust**: Handles files of any size with intelligent fallbacks  

## 🧠 Philosophy

*"Why rush when you can prefetch?"*  
*"Not everything needs to be a C++ monument."*  
*"Built in Python. Because being clever beats being loud."*

SLOTH embraces the art of doing just enough, just in time. No bloated dependencies, no over-engineered abstractions — just smart, lazy evaluation that gets faster the more you use it.

## 📦 Installation

```bash
pip install sloth-mmcif
```

### Development Installation

```bash
git clone https://github.com/lucas/sloth.git
cd sloth
pip install -e ".[dev]"
```

## 📦 Package Development

### Building the Package

```bash
# Install build dependencies
pip install build twine

# Build the package
python -m build

# Check the package
twine check dist/*
```

### Running Tests

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Run the demo
python -m sloth.demo --demo
```

### Publishing to PyPI

```bash
# Build and upload (requires PyPI credentials)
python -m build
twine upload dist/*
```

## Quick Start

```python
from sloth import MMCIFHandler

# Create handler - automatically optimized
handler = MMCIFHandler()

# Parse any mmCIF file
data = handler.parse("structure.cif")

# Access data naturally
block = data.data[0]
atom_sites = block._atom_site
coordinates = atom_sites.Cartn_x
```

## Design Philosophy

❌ **No Configuration Hell** - No flags, no options to choose from  
❌ **No Performance Trade-offs** - Always uses the best approach  
❌ **No Complex Setup** - Works immediately out of the box  

✅ **One Optimal Way** - Single API that's always efficient  
✅ **Automatic Optimization** - Handles large and small files intelligently  
✅ **Simple Usage** - Easy to learn, easy to use  

## Basic Usage

### Parsing Files

```python
from sloth import MMCIFHandler

handler = MMCIFHandler()

# Parse entire file
data = handler.parse("structure.cif")

# Parse only specific categories (faster for large files)
data = handler.parse("structure.cif", categories=['_atom_site', '_entry'])
```

### Accessing Data

#### Get Data Blocks

```python
# List all data blocks
blocks = data.blocks
print(blocks)  # ['7XJP']

# Get first data block
block = data.data[0]
# or access by name
block = data['7XJP']
# or using dot notation
block = data.data_7XJP
```

#### Get Categories

```python
# List all categories in a block
categories = block.categories
print(list(categories.keys()))  # ['_database_2', '_atom_site', ...]

# Access a category
db_info = block._database_2
atom_data = block._atom_site
```

#### Get Items and Values

```python
# List items in a category
items = db_info.items
print(items)  # ['database_id', 'database_code', ...]

# Access item values
database_ids = db_info.database_id
print(database_ids)  # ['PDB', 'WWPDB', 'EMDB']

# Access individual values
first_db = db_info.database_id[0]  # 'PDB'

# Get coordinates (for large atom datasets)
x_coords = atom_data.Cartn_x  # Efficiently loaded
y_coords = atom_data.Cartn_y
z_coords = atom_data.Cartn_z
```

### Modifying Data

```python
# Modify values
db_info.database_id[-1] = 'NEWDB'
print(db_info.database_id)  # ['PDB', 'WWPDB', 'NEWDB']
```

### Writing Files

```python
# Write modified data to a new file
with open("modified_structure.cif", 'w') as f:
    handler.file_obj = f
    handler.write(data)
```

### Performance Examples

#### Large File Processing

```python
# Efficiently handle large files
handler = MMCIFHandler()
data = handler.parse("huge_structure.cif")  # Fast startup

# Only loads data when accessed
if need_coordinates:
    x_coords = data.data[0]._atom_site.Cartn_x
    
if need_structure_info:
    title = data.data[0]._struct.title
```

#### Selective Parsing

```python
# Parse only what you need for maximum efficiency
data = handler.parse("large_file.cif", categories=['_entry', '_struct'])

# Much faster than loading everything
entry_info = data.data[0]._entry
structure_info = data.data[0]._struct
```

## Data Validation

The library supports custom validation of categories and cross-validation between categories.

### Setting Up Validation

```python
from sloth import MMCIFHandler, ValidatorFactory

def category_validator(category_name):
    print(f"Validating category: {category_name}")

def cross_checker(category_name_1, category_name_2):
    print(f"Cross-checking {category_name_1} with {category_name_2}")

# Create validator factory
validator_factory = ValidatorFactory()
validator_factory.register_validator('_database_2', category_validator)
validator_factory.register_cross_checker(('_database_2', '_atom_site'), cross_checker)

# Use with handler
handler = MMCIFHandler(validator_factory=validator_factory)
data = handler.parse("structure.cif")
```

### Running Validation

```python
# Validate a single category
data.data[0]._database_2.validate()

# Cross-validate between categories
data.data[0]._database_2.validate().against(data.data[0]._atom_site)
```

## API Reference

### MMCIFHandler

The main class for parsing and writing mmCIF files.

```python
handler = MMCIFHandler(validator_factory=None)
```

**Methods:**
- `parse(filename: str, categories: Optional[List[str]] = None) -> MMCIFDataContainer`
  - Parse an mmCIF file
  - `categories`: Optional list to parse only specific categories
- `write(data_container: MMCIFDataContainer) -> None`  
  - Write data to a file (requires `file_obj` to be set)

**Properties:**
- `file_obj`: Set this to an open file handle before writing

### MMCIFDataContainer

Container for all data blocks in an mmCIF file.

**Properties:**
- `blocks: List[str]` - List of data block names
- `data: List[DataBlock]` - List of data block objects

**Access Methods:**
- `container[block_name]` - Get block by name
- `container.data_BLOCKNAME` - Dot notation access

### DataBlock

Represents a single data block in an mmCIF file.

**Properties:**
- `name: str` - Block name
- `categories: Dict[str, Category]` - Dictionary of categories

**Access Methods:**
- `block[category_name]` - Get category by name  
- `block._category_name` - Dot notation access

### Category

Represents a category within a data block.

**Properties:**
- `name: str` - Category name
- `items: List[str]` - List of item names
- `data: Dict[str, List[str]]` - All data (forces loading)

**Access Methods:**
- `category[item_name]` - Get item values
- `category.item_name` - Dot notation access
- `category.get_item(item_name)` - Get raw item object

**Validation:**
- `category.validate()` - Validate this category
- `category.validate().against(other_category)` - Cross-validate

### ValidatorFactory

Factory for creating and managing validators.

**Methods:**
- `register_validator(category_name: str, validator_function: Callable)`
- `register_cross_checker(category_pair: Tuple[str, str], checker_function: Callable)`
- `get_validator(category_name: str) -> Optional[Callable]`
- `get_cross_checker(category_pair: Tuple[str, str]) -> Optional[Callable]`

## Performance Characteristics

| File Size | Startup Time | Access Speed | Memory Usage |
|-----------|--------------|--------------|--------------|
| Small (<1MB) | Instant | Instant | Minimal |
| Medium (1-100MB) | Fast | Fast | Efficient |
| Large (100MB-1GB) | Fast | Fast | Optimized |
| Huge (>1GB) | Fast | Fast | Smart |

**Key Benefits:**
- 🚀 **Fast startup** regardless of file size
- 💾 **Efficient processing** - optimized data access patterns  
- ⚡ **Instant access** to any category or item
- 📈 **Scalable** to files of any size

## Examples

### Basic Structure Analysis

```python
from sloth import MMCIFHandler

handler = MMCIFHandler()
data = handler.parse("1abc.cif")

block = data.data[0]
print(f"Structure: {block._entry.id[0]}")
print(f"Resolution: {block._refine.ls_d_res_high[0]}")
print(f"Atoms: {len(block._atom_site.Cartn_x)}")
```

### Coordinate Extraction

```python
# Get all atom coordinates efficiently
atom_data = block._atom_site
coordinates = list(zip(
    atom_data.Cartn_x,
    atom_data.Cartn_y, 
    atom_data.Cartn_z
))

# First 10 coordinates
for i, (x, y, z) in enumerate(coordinates[:10]):
    print(f"Atom {i+1}: ({x}, {y}, {z})")
```

### Large File Processing

```python
# Process only what you need from a large file
data = handler.parse("large_structure.cif", categories=['_atom_site'])

# This is very fast even for huge files
atom_count = len(data.data[0]._atom_site.Cartn_x)
print(f"Processed {atom_count} atoms efficiently")
```

## Error Handling

The library handles various error conditions gracefully:

```python
try:
    data = handler.parse("structure.cif")
except FileNotFoundError:
    print("File not found")
except Exception as e:
    print(f"Parsing error: {e}")
```

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality  
4. Submit a pull request

## License

This project is licensed under the MIT License.

---

**Note**: This library automatically optimizes performance for files of any size. No configuration needed - it just works efficiently.
