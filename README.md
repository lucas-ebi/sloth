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
mmcif_data_container = handler.parse("structure.cif")

# Get block names and access with elegant dot notation
block_names = mmcif_data_container.blocks                              # Get available block names
print(block_names)                                          # ['1ABC']
block = mmcif_data_container.data_1ABC                              # Access block directly by name with dot notation

# Direct access to categories and items with dot notation
atom_sites = mmcif_data_container.data_1ABC._atom_site              # Category access - clean and intuitive
coordinates = mmcif_data_container.data_1ABC._atom_site.Cartn_x     # Item access - no dictionary syntax needed

# The power of chained dot notation for incredibly concise code
first_x_coord = mmcif_data_container.data_1ABC._atom_site.Cartn_x[0]  # Everything in one line!
```

## The Power of Chained Dot Notation

SLOTH's most elegant feature is the ability to chain dot notation for incredibly concise code. Instead of assigning intermediate variables, you can directly access what you need in a single line.

### One-Liners with Chained Dot Notation

```python
from sloth import MMCIFHandler

# Parse file and extract data in a clean, fluent style
handler = MMCIFHandler()
mmcif_data_container = handler.parse("structure.cif")

# First, get the block names for reference
block_names = mmcif_data_container.blocks
print(f"Available blocks: {block_names}")  # e.g., ['1ABC']

# ✅ RECOMMENDED: Chained dot notation with meaningful block names
print(f"First atom X coordinate: {mmcif_data_container.data_1ABC._atom_site.Cartn_x[0]}")
print(f"Structure title: {mmcif_data_container.data_1ABC._struct.title[0]}")
print(f"Space group: {mmcif_data_container.data_1ABC._symmetry.space_group_name_H_M[0]}")

# You can still use other access methods when needed
print(f"Unit cell dimensions: {mmcif_data_container['1ABC']._cell.length_a[0]} × "
      f"{mmcif_data_container['1ABC']._cell.length_b[0]} × {mmcif_data_container['1ABC']._cell.length_c[0]} Å")

# Combining multiple chains in a single expression
print(f"PDB ID {mmcif_data_container.data_1ABC._entry.id[0]} has resolution {mmcif_data_container.data_1ABC._refine.ls_d_res_high[0]} Å")

# Instead of the verbose, multi-step approach:
# ❌ NOT RECOMMENDED: Too many intermediate variables
block = mmcif_data_container.data[0]  # Using index instead of the more readable dot notation
atom_site = block._atom_site
x_coords = atom_site.Cartn_x
first_x = x_coords[0]
print(f"First atom X coordinate: {first_x}")
```

### Advanced Chaining with Direct Access

```python
# Directly filter and process with chained dot notation using block name
ca_atoms = [atom for atom in mmcif_data_container.data_1ABC._atom_site if atom.type_symbol == "CA"]
print(f"Found {len(ca_atoms)} CA atoms")

# Calculate statistics in a single line without intermediate variables
print(f"Mean X coordinate: {sum(float(atom.Cartn_x) for atom in mmcif_data_container.data_1ABC._atom_site) / mmcif_data_container.data_1ABC._atom_site.row_count:.2f} Å")
print(f"Total atoms: {mmcif_data_container.data_1ABC._atom_site.row_count}")

# Direct count of elements by type in one line
element_counts = {elem: sum(1 for atom in mmcif_data_container.data_1ABC._atom_site if atom.type_symbol == elem) 
                 for elem in set(atom.type_symbol for atom in mmcif_data_container.data_1ABC._atom_site)}
print(f"Element distribution: {element_counts}")
```

### Data Extraction One-Liners - Combining with List Comprehensions

```python
# Extract residue names and numbers in a single line - no intermediate variables!
residues = [(atom.label_comp_id, atom.label_seq_id) for atom in mmcif_data_container.data_1ABC._atom_site[:10]]

# Get all unique chain IDs in one line
chain_ids = set(atom.label_asym_id for atom in mmcif_data_container.data_1ABC._atom_site)
print(f"Chains in structure: {', '.join(sorted(chain_ids))}")

# Find all waters in one line
water_count = sum(1 for atom in mmcif_data_container.data_1ABC._atom_site if atom.label_comp_id == "HOH")
print(f"Structure contains {water_count} water molecules")

# Extract all CA atoms from a specific chain in one line
chain_a_ca_atoms = [atom for atom in mmcif_data_container.data_1ABC._atom_site 
                    if atom.label_asym_id == "A" and atom.label_atom_id == "CA"]
```

### Real-World Examples with Chained Dot Notation

```python
# Calculate backbone RMSD between two structures
def calculate_rmsd(mmcif1: MMCIFDataContainer, mmcif2: MMCIFDataContainer):
    atoms1 = [atom for atom in mmcif1.data_1ABC._atom_site 
             if atom.label_atom_id in ("CA", "N", "C", "O")]
    atoms2 = [atom for atom in mmcif2.data_1ABC._atom_site 
             if atom.label_atom_id in ("CA", "N", "C", "O")]
    # Direct coordinate access with chained dot notation
    coords1 = [(float(atom.Cartn_x), float(atom.Cartn_y), float(atom.Cartn_z)) 
              for atom in atoms1]
    coords2 = [(float(atom.Cartn_x), float(atom.Cartn_y), float(atom.Cartn_z)) 
              for atom in atoms2]
    # Calculate RMSD...
    
# Find all disulfide bonds in one line
disulfides = [(conn.ptnr1_label_seq_id, conn.ptnr2_label_seq_id) 
             for conn in mmcif_data_container.data_1ABC._struct_conn 
             if "disulf" in conn.conn_type_id.lower()]

# Extract B-factors for protein backbone in a single expression
backbone_bfactors = [float(atom.B_iso_or_equiv) 
                    for atom in mmcif_data_container.data_1ABC._atom_site 
                    if atom.label_atom_id in ("CA", "N", "C", "O")]

# Get sequence as a string directly from mmCIF
sequence = ''.join(residue.mon_id for residue in mmcif_data_container.data_1ABC._entity_poly_seq)
```

### Nested Data Processing with Clean Chains

```python
# Process nested data structures with elegant chained expressions
def find_connected_atoms(mmcif: MMCIFDataContainer, residue_id: str):
    # Chained dot notation makes complex queries readable and maintainable
    return [
        (conn.ptnr1_label_atom_id, conn.ptnr2_label_atom_id) 
        for conn in mmcif.data_1ABC._struct_conn 
        if conn.ptnr1_label_seq_id == residue_id or conn.ptnr2_label_seq_id == residue_id
    ]

# Clean error handling with chained dot notation
try:
    symmetry = mmcif_data_container.data_1ABC._symmetry.space_group_name_H_M[0]
except (KeyError, IndexError, AttributeError):
    # One-liner fallbacks are easy with chained dot notation
    symmetry = mmcif_data_container.data_1ABC._cell.length_a[0] if '_cell' in mmcif_data_container.data_1ABC.categories else "Unknown"
```

### Interactive Data Exploration

```python
# Interactive exploration becomes intuitive and concise
# No need to store intermediate variables in notebooks
mmcif_data_container = MMCIFHandler().parse("structure.cif")

# Direct exploration in one line
print(f"Structure info: {mmcif_data_container.data_1ABC._entry.id[0]} - {mmcif_data_container.data_1ABC._struct.title[0]}")
print(f"Contains {mmcif_data_container.data_1ABC._atom_site.row_count} atoms")
print(f"First 3 atom types: {[atom.type_symbol for atom in mmcif_data_container.data_1ABC._atom_site[:3]]}")

# Easily test different filtering criteria
metal_atoms = [atom for atom in mmcif_data_container.data_1ABC._atom_site 
               if atom.type_symbol in ("FE", "ZN", "CA", "MG", "MN")]
```

Chained dot notation is especially valuable when exploring data interactively or writing compact data analysis scripts. It allows you to express complex data access patterns in a clear, readable way without cluttering your code with intermediate variables, leading to more maintainable and elegant code.

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
mmcif_data_container = handler.parse("structure.cif")

# Parse only specific categories (faster for large files)
mmcif_data_container = handler.parse("structure.cif", categories=['_atom_site', '_entry'])
```

### Accessing Data

#### Get Data Blocks

```python
# List all data blocks
blocks = mmcif_data_container.blocks
print(blocks)  # ['1ABC']

# Get data blocks in three elegant ways:
block1 = mmcif_data_container.data[0]          # Method 1: By index - access first block  
block2 = mmcif_data_container['1ABC']       # Method 2: By name - dictionary-style access
block3 = mmcif_data_container.data_1ABC     # Method 3: By dot notation - most elegant for known blocks

# Use any method to start your chain of dot notation access
print(f"Resolution via method 1: {mmcif_data_container.data_1ABC._refine.ls_d_res_high[0]}")
print(f"Resolution via method 2: {mmcif_data_container['1ABC']._refine.ls_d_res_high[0]}")
print(f"Resolution via method 3: {mmcif_data_container.data_1ABC._refine.ls_d_res_high[0]}")
```

#### Get Categories with Elegant Dot Notation

```python
# List all categories in a block
categories = block.categories
print(categories)  # ['_database_2', '_atom_site', ...]

# Access categories with clean dot notation
db_info = block._database_2       # Direct dot notation access - elegant and readable!
atom_data = block._atom_site      # This is SLOTH's intuitive dot notation at work!
entry_info = block._entry         # No dictionary syntax needed!

# Or access via data property if needed
category_objects = block.data
```

#### Get Items and Values with Dot Notation

```python
# List items in a category
items = db_info.items
print(items)  # ['database_id', 'database_code', ...]

# Access item values with elegant dot notation
database_ids = db_info.database_id              # Direct dot access - clean and intuitive!
database_codes = db_info.database_code          # So much more readable than dict syntax!
print(database_ids)  # ['PDB', 'WWPDB', 'EMDB']

# Access individual values (column-wise) with chained dot notation
first_db = db_info.database_id[0]               # 'PDB' - combines dot notation with indexing
first_code = db_info.database_code[0]           # Elegant dot notation for attributes

# Access row-wise with dot notation (2D slicing)
first_row = db_info[0]                          # Row object for first row
print(first_row.database_id)                    # Dot notation access to row item - clean!
print(first_row.database_code)                  # So much better than first_row['database_code']
print(first_row.data)                           # All values as a dictionary

# Get multiple rows with dot notation for each row
for row in db_info[:3]:                          # First three rows
    print(f"{row.database_id}: {row.database_code}")    # Dot notation makes this readable!
    
# Get coordinates with clean dot notation (for large atom datasets)
x_coords = atom_data.Cartn_x                     # Dot notation - efficient and readable!
y_coords = atom_data.Cartn_y                     # Dot notation is automatically lazy-loaded
z_coords = atom_data.Cartn_z                     # Much cleaner than atom_data['Cartn_z']

# Chain everything together for ultra-clean code
first_atom_x = block._atom_site.Cartn_x[0]       # Amazing chained dot notation!
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
    handler.write(mmcif_data_container)
```

### Performance Examples

#### Large File Processing

```python
# Efficiently handle large files
handler = MMCIFHandler()
mmcif_data_container = handler.parse("huge_structure.cif")  # Fast startup

# Only loads data when accessed
if need_coordinates:
    x_coords = mmcif_data_container.data_1ABC._atom_site.Cartn_x
    
if need_structure_info:
    title = mmcif_data_container.data_1ABC._struct.title
```

#### Selective Parsing

```python
# Parse only what you need for maximum efficiency
mmcif_data_container = handler.parse("large_file.cif", categories=['_entry', '_struct'])

# Much faster than loading everything
entry_info = mmcif_data_container.data_1ABC._entry
structure_info = mmcif_data_container.data_1ABC._struct
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
mmcif_data_container = handler.parse("structure.cif")
```

### Running Validation

```python
# Validate a single category
mmcif_data_container.data_1ABC._database_2.validate()

# Cross-validate between categories
mmcif_data_container.data_1ABC._database_2.validate().against(mmcif_data_container.data_1ABC._atom_site)
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
- `write(mmcif: MMCIFDataContainer) -> None`  
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
- `categories: List[str]` - List of category names
- `data: Dict[str, Category]` - Dictionary of category objects

**Access Methods:**

- `block[category_name]` - Get category by name  
- `block._category_name` - Dot notation access

### Category

Represents a category within a data block.

**Properties:**

- `name: str` - Category name
- `items: List[str]` - List of item names
- `data: Dict[str, List[str]]` - All data (forces loading)
- `row_count: int` - Number of rows in the category
- `rows: List[Row]` - All rows in the category

**Access Methods:**

- `category[item_name]` - Get item values (column-wise access)
- `category[index]` - Get a specific row (row-wise access)
- `category[start:end]` - Get multiple rows (slice access)
- `category.item_name` - Dot notation for column access
- `category.get_item(item_name)` - Get raw item object
- `category.iterrows()` - Iterator over (index, row) pairs

**Validation:**

- `category.validate()` - Validate this category
- `category.validate().against(other_category)` - Cross-validate

### Row

Represents a single row of data in a Category.

**Properties:**

- `data: Dict[str, str]` - Dictionary of all item values for this row

**Access Methods:**

- `row.item_name` - Get value for specific item in this row
- `row[item_name]` - Dictionary-style access to values

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

## Code Examples with Elegant Dot Notation

### Basic Structure Analysis with Dot Notation

```python
from sloth import MMCIFHandler

handler = MMCIFHandler()
mmcif_data_container = handler.parse("1abc.cif")

# Demonstrate all three methods of block access with chained dot notation 
print(f"Structure ID: {mmcif_data_container.data_1ABC._entry.id[0]}")          # By dot notation
print(f"Resolution: {mmcif_data_container['1ABC']._refine.ls_d_res_high[0]}")  # By name
print(f"Atoms: {len(mmcif_data_container.data_1ABC._atom_site.Cartn_x)}")      # By dot notation

# Compare the elegance of chained dot notation to traditional dictionary access
print(f"Title (elegant): {mmcif_data_container.data_1ABC._struct.title[0]}")
print(f"Title (traditional): {mmcif_data_container['1ABC']['_struct']['title'][0]}")  # Much less readable!

# Combine block access methods with chained dot notation in various ways
title = mmcif_data_container.data_1ABC._struct.title[0]                    # Dot notation block access
space_group = mmcif_data_container['1ABC']._symmetry.space_group_name_H_M[0]  # Name-based block access
atom_count = len(mmcif_data_container.data_1ABC._atom_site.Cartn_x)        # Dot notation block access
```

### Coordinate Extraction with Dot Notation

```python
# Get all atom coordinates efficiently with clean dot notation
atom_data = block._atom_site  # Direct dot notation access
coordinates = list(zip(
    atom_data.Cartn_x,        # Elegant dot notation - no dictionary syntax needed
    atom_data.Cartn_y,        # Dot notation is more readable
    atom_data.Cartn_z         # And works with lazy loading!
))

# First 10 coordinates
for i, (x, y, z) in enumerate(coordinates[:10]):
    print(f"Atom {i+1}: ({x}, {y}, {z})")
    
# Even cleaner with new Row objects and dot notation
for i, atom in enumerate(atom_data[:5]):  # Get first 5 rows
    print(f"Atom {i+1}: {atom.type_symbol} at ({atom.Cartn_x}, {atom.Cartn_y}, {atom.Cartn_z})")
```

### Efficient Large File Handling

```python
# Process only what you need from a large file
mmcif_data_container = handler.parse("large_structure.cif", categories=['_atom_site'])

# This is very fast even for huge files
atom_count = len(mmcif_data_container.data_1ABC._atom_site.Cartn_x)
print(f"Processed {atom_count} atoms efficiently")
```

## Error Handling

The library handles various error conditions gracefully:

```python
try:
    mmcif_data_container = handler.parse("structure.cif")
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
