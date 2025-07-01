# 🦥 SLOTH – Structural Loader with On-demand Traversal Handling

> 🧠 *Lazy by design. Fast by default.*

![SLOTH](https://img.shields.io/badge/SLOTH-Lazy%20by%20Design%2C%20Fast%20by%20Default-blueviolet?logo=python&logoColor=white)
[![PyPI](https://badge.fury.io/py/sloth-mmcif.svg)](https://badge.fury.io/py/sloth-mmcif)
[![Python](https://img.shields.io/pypi/pyversions/sloth-mmcif.svg)](https://pypi.org/project/sloth-mmcif/)
[![License](https://img.shields.io/github/license/lucas/sloth.svg)](https://github.com/lucas/sloth/blob/main/LICENSE)

---

## 🚀 Overview

**SLOTH** is a fast, Pythonic mmCIF parser that embraces lazy evaluation for maximum efficiency.

It eagerly parses files but lazily creates data objects only when accessed, giving you the best of both worlds.

Built for speed, simplicity, and elegance, SLOTH is ideal for:

- ⚡ Interactive structural analysis with instant navigation  
- 📊 Automated pipelines that process only what they need
- 🧠 Efficient exploration of large structural datasets

---

## ✨ Key Features

✅ **Simple API** – One optimal way to create, parse, and access  
⚡ **Smart Lazy Loading** – Row and item objects created only when accessed  
📦 **Complete** – Access to all mmCIF blocks, categories, and items  
🔧 **Robust** – Handles files from tiny samples to large structures  
🔄 **Import/Export** – JSON, XML, YAML, Pickle, CSV, Pandas  

---

## 🧠 Philosophy

> "*Why rush when you can be lazy?*"  
> "*Not everything needs to be a C++ monument.*"

SLOTH is unapologetically Pythonic:

- No overengineering
- No runtime flags  
- No manual optimization

Just smart defaults, lazy evaluation, and expressive code.

---

## 📦 Installation

```bash
pip install sloth-mmcif
```

### Developer Install

```bash
git clone https://github.com/lucas/sloth.git
cd sloth
pip install -e ".[dev]"
```

---

## 🧪 Quick Start

```python
from sloth import MMCIFHandler

handler = MMCIFHandler()
mmcif = handler.parse("1abc.cif")

# Access structure title and first atom X coordinate
print(mmcif.data_1ABC._struct.title[0])
print(mmcif.data_1ABC._atom_site.Cartn_x[0])
```

---

## 🔍 Elegant Dot Notation

SLOTH supports fully chained, Pythonic access:

```python
# All dot notation — no dicts, no brackets
pdb_id = mmcif.data_1ABC._entry.id[0]
space_group = mmcif.data_1ABC._symmetry.space_group_name_H_M[0]
x_coords = mmcif.data_1ABC._atom_site.Cartn_x
```

### One-liners for data analysis

```python
# Mean X coordinate
avg_x = sum(float(atom.Cartn_x) for atom in mmcif.data_1ABC._atom_site) / mmcif.data_1ABC._atom_site.row_count

# Get all CA atoms from chain A
ca_atoms = [a for a in mmcif.data_1ABC._atom_site if a.label_atom_id == "CA" and a.label_asym_id == "A"]
```

---

## 🔄 Import & Export

### Export Formats

```python
handler.export_to_json(mmcif, "out.json")
handler.export_to_xml(mmcif, "out.xml")
handler.export_to_pickle(mmcif, "out.pkl")
handler.export_to_yaml(mmcif, "out.yaml")
dfs = handler.export_to_pandas(mmcif)
handler.export_to_csv(mmcif, "csv_output_dir")
```

### Import Formats

```python
mmcif = handler.import_from_json("out.json")
mmcif = handler.import_from_xml("out.xml")
mmcif = handler.import_from_pickle("out.pkl")
mmcif = handler.import_from_yaml("out.yaml")
mmcif = handler.import_from_csv_files("csv_output_dir")
mmcif = handler.import_auto_detect("structure.json")
```

---

## ⚙️ Selective Parsing & Performance

### Load only selected categories

```python
mmcif = handler.parse("large_file.cif", categories=["_atom_site", "_entry"])
```

- **Startup time**: Fast (eager parsing)  
- **Object creation**: Lazy (on-demand Row/Item objects)  
- **Memory usage**: Optimized (cached with smart invalidation)  

---

## 🧪 Validation Support

```python
from sloth import ValidatorFactory

vf = ValidatorFactory()
vf.register_validator("_atom_site", lambda c: print("Validating", c.name))
handler = MMCIFHandler(validator_factory=vf)

mmcif = handler.parse("1abc.cif")
mmcif.data_1ABC._atom_site.validate()
```

---

## 📚 Cookbook - Real-World Examples

*All examples below are demonstrated in [`demo.py`](demo.py) - run `python demo.py --demo` to see them in action!*

### 🚀 Getting Started

#### Basic Parsing and Information

```python
from sloth import MMCIFHandler

# Create handler and parse file
handler = MMCIFHandler()
mmcif = handler.parse("structure.cif")

# Get file information
print(f"Data blocks: {len(mmcif.data)}")
block = mmcif.data[0]
print(f"Block name: {block.name}")
print(f"Categories: {len(block.categories)}")
print(f"Available: {', '.join(block.categories[:5])}")
```

#### Creating Sample Data

##### Method 1: Create sample mmCIF file manually

```python
sample_content = """data_1ABC
_entry.id 1ABC_STRUCTURE
_database_2.database_id PDB
_database_2.database_code 1ABC
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
ATOM 1 N 10.123 20.456 30.789
ATOM 2 C 11.234 21.567 31.890
"""

# Write sample file that SLOTH can then parse
with open("sample.cif", 'w') as f:
    f.write(sample_content)

# Parse with SLOTH
mmcif = handler.parse("sample.cif")
```

##### Method 2: Create sample data programmatically using dictionary notation

```python
from sloth.models import MMCIFDataContainer, DataBlock, Category

# Create container and block
mmcif = MMCIFDataContainer()
block = DataBlock("1ABC")

# Create categories and add data
entry_category = Category("_entry")
entry_category["id"] = ["1ABC_STRUCTURE"]

database_category = Category("_database_2")
database_category["database_id"] = ["PDB"]
database_category["database_code"] = ["1ABC"]

atom_site_category = Category("_atom_site")
atom_site_category["group_PDB"] = ["ATOM", "ATOM"]
atom_site_category["id"] = ["1", "2"]
atom_site_category["type_symbol"] = ["N", "C"]
atom_site_category["Cartn_x"] = ["10.123", "11.234"]
atom_site_category["Cartn_y"] = ["20.456", "21.567"]
atom_site_category["Cartn_z"] = ["30.789", "31.890"]

# Add categories to block
block["_entry"] = entry_category
block["_database_2"] = database_category
block["_atom_site"] = atom_site_category

# Add block to container
mmcif["1ABC"] = block

# Write using SLOTH
with open("sample_programmatic.cif", 'w') as f:
    handler.file_obj = f
    handler.write(mmcif)
```

##### Method 3: ✨ NEW! Auto-creation with Dot Notation

```python
# SLOTH can automatically create nested objects with elegant dot notation!
from sloth.models import MMCIFDataContainer

# Create an empty container
mmcif = MMCIFDataContainer()

# Use dot notation to auto-create everything
mmcif.data_1ABC._entry.id = ["1ABC_STRUCTURE"]
mmcif.data_1ABC._database_2.database_id = ["PDB"]
mmcif.data_1ABC._database_2.database_code = ["1ABC"]

# Add atom data
mmcif.data_1ABC._atom_site.group_PDB = ["ATOM", "ATOM"] 
mmcif.data_1ABC._atom_site.type_symbol = ["N", "C"]
mmcif.data_1ABC._atom_site.Cartn_x = ["10.123", "11.234"]

# Write using SLOTH
with open("sample_dot_notation.cif", 'w') as f:
    handler.file_obj = f
    handler.write(mmcif)
```

### 🎯 Elegant Data Access

#### Multiple Access Methods for Different Scenarios

```python
# Access data with elegant dot notation - SLOTH's signature feature!
block = mmcif.data[0]  # or mmcif.data_1ABC

# 1. Dot notation access (elegant for direct access)
atom_site = block._atom_site
x_coordinates = atom_site.Cartn_x
first_atom_id = atom_site[0].id

# 2. Dictionary notation access (powerful for dynamic field names)
atom_site = block["_atom_site"]
field_name = "Cartn_x"  # Could come from a variable or user input
x_coordinates = atom_site[field_name]
first_atom_id = atom_site[0]["id"]

# 3. Mixed approach (best of both worlds)
# Use dot notation for known fields, dictionary access for dynamic fields
category_name = "_atom_site"  # Could be dynamic
field_name = "Cartn_x"        # Could be dynamic
x_coordinates = block[category_name][field_name]

# 4. Iterative access (when processing all data)
for category_name in block.categories:
    category = block[category_name]
    for item_name in category.items:
        values = category[item_name]
        print(f"{category_name}.{item_name}: {len(values)} values")
```

#### Row-wise and Column-wise Access

```python
# Column-wise data access (Pythonic!)
x_coordinates = atom_site.Cartn_x        # All X coordinates
atom_types = atom_site.type_symbol       # All atom types
atom_ids = atom_site.id                  # All atom IDs

# Row-wise access with dot notation
first_atom = atom_site[0]
print(f"First atom: {first_atom.type_symbol} at ({first_atom.Cartn_x}, {first_atom.Cartn_y}, {first_atom.Cartn_z})")

# Slicing with elegant access
first_three_atoms = atom_site[0:3]
for atom in first_three_atoms:
    print(f"Atom {atom.id}: {atom.type_symbol}")
```

#### Advanced Data Analysis

```python
# One-liners for common structural analysis
ca_atoms = [a for a in block._atom_site if a.label_atom_id == "CA"]
chain_a_atoms = [a for a in block._atom_site if a.label_asym_id == "A"]

# Calculate center of mass (X coordinate)
if block._atom_site.Cartn_x:
    center_x = sum(float(x) for x in block._atom_site.Cartn_x) / len(block._atom_site.Cartn_x)
    print(f"Center X: {center_x:.3f}")
```

### ✏️ Data Modification

#### Safe Data Editing

```python
# Modify data using dot notation (the elegant approach)
if '_database_2' in block.categories:
    db_category = block._database_2
    if db_category.database_id:
        original = db_category.database_id[0]
        db_category.database_id[0] = 'MODIFIED_DB'
        print(f"Changed: '{original}' → 'MODIFIED_DB'")

# Save modified data
with open("modified.cif", 'w') as f:
    handler.file_obj = f
    handler.write(mmcif)
```

### 📊 Multi-Format Export/Import

#### Export to Multiple Formats

```python
# Export to various formats for different use cases
output_dir = "exports"
os.makedirs(output_dir, exist_ok=True)

# JSON - great for web applications
handler.export_to_json(mmcif, f"{output_dir}/data.json")

# XML - for enterprise systems
handler.export_to_xml(mmcif, f"{output_dir}/data.xml")

# YAML - human-readable configuration
handler.export_to_yaml(mmcif, f"{output_dir}/data.yaml")

# Pickle - fastest Python serialization
handler.export_to_pickle(mmcif, f"{output_dir}/data.pkl")

# CSV - for spreadsheet analysis
csv_files = handler.export_to_csv(mmcif, f"{output_dir}/csv_files")
print(f"Created CSV files: {list(csv_files.values())}")
```

#### Import from Any Format

```python
# Import from exported formats
json_data = handler.import_from_json(f"{output_dir}/data.json")
xml_data = handler.import_from_xml(f"{output_dir}/data.xml")
yaml_data = handler.import_from_yaml(f"{output_dir}/data.yaml")
pickle_data = handler.import_from_pickle(f"{output_dir}/data.pkl")
csv_data = handler.import_from_csv_files(f"{output_dir}/csv_files")

# Smart auto-detection - SLOTH figures out the format!
auto_data = handler.import_auto_detect(f"{output_dir}/data.json")
print("Auto-detected JSON format and loaded successfully!")
```

### 🛡️ Validation and Quality Control

#### Setting Up Validation

```python
from sloth import ValidatorFactory

# Create custom validators
def atom_site_validator(category_name):
    print(f"✅ Validating {category_name}")
    # Add your validation logic here

def cross_category_checker(cat1_name, cat2_name):
    print(f"🔗 Cross-checking {cat1_name} ↔ {cat2_name}")
    # Add cross-validation logic here

# Setup validation factory
validator_factory = ValidatorFactory()
validator_factory.register_validator("_atom_site", atom_site_validator)
validator_factory.register_cross_checker(("_atom_site", "_entity"), cross_category_checker)

# Create handler with validation
handler = MMCIFHandler(validator_factory=validator_factory)
mmcif = handler.parse("structure.cif")

# Run validation
if mmcif.data and '_atom_site' in mmcif.data[0].categories:
    mmcif.data[0]._atom_site.validate()
```

#### Schema Validation for Exports

```python
from sloth import SchemaValidatorFactory, DataSourceFormat, ValidationError

# JSON Schema validation
json_validator = SchemaValidatorFactory.create_validator(DataSourceFormat.JSON)

# Validate before import
try:
    with open(f"{output_dir}/data.json", 'r') as f:
        json_data = json.load(f)
    
    if json_validator.is_valid(json_data):
        valid_mmcif = handler.import_from_json(f"{output_dir}/data.json", schema_validator=json_validator)
        print("✅ JSON data passed schema validation")
    else:
        print("❌ JSON data failed schema validation")
        
except ValidationError as e:
    print(f"Schema validation error: {e}")
```

### 🔍 Advanced Data Exploration

#### 2D Data Slicing and Analysis

```python
# Demonstrate SLOTH's powerful 2D slicing capabilities
if '_atom_site' in block.categories:
    atom_site = block._atom_site
    
    print(f"📊 Atom site analysis:")
    print(f"   Total atoms: {atom_site.row_count}")
    print(f"   Available data: {', '.join(atom_site.items)}")
    
    # Column-wise analysis with dot notation
    if hasattr(atom_site, 'type_symbol'):
        unique_elements = set(atom_site.type_symbol)
        print(f"   Elements found: {', '.join(sorted(unique_elements))}")
    
    # Row-wise analysis with slicing
    sample_atoms = atom_site[0:3]  # First 3 atoms
    for i, atom in enumerate(sample_atoms):
        if hasattr(atom, 'type_symbol') and hasattr(atom, 'Cartn_x'):
            print(f"   Atom {i+1}: {atom.type_symbol} at X={atom.Cartn_x}")
```

#### Performance-Optimized Parsing

```python
# Parse only specific categories for better performance
specific_categories = ["_atom_site", "_database_2"]
mmcif_subset = handler.parse("large_structure.cif", categories=specific_categories)

print(f"Loaded only: {', '.join(mmcif_subset.data[0].categories)}")
print("Memory usage optimized for large files!")
```

### 🔄 Round-Trip Data Integrity

#### Verify Export/Import Consistency

```python
def verify_round_trip(original_mmcif, imported_mmcif, format_name):
    """Verify data integrity after export/import cycle"""
    orig_block = original_mmcif.data[0]
    import_block = imported_mmcif.data[0]
    
    # Compare category counts
    if len(orig_block.categories) == len(import_block.categories):
        print(f"✅ {format_name}: Category count matches")
    else:
        print(f"⚠️ {format_name}: Category count differs")
    
    # Compare sample data
    if '_atom_site' in orig_block.categories and '_atom_site' in import_block.categories:
        orig_atoms = len(orig_block._atom_site.Cartn_x) if hasattr(orig_block._atom_site, 'Cartn_x') else 0
        import_atoms = len(import_block._atom_site.Cartn_x) if hasattr(import_block._atom_site, 'Cartn_x') else 0
        
        if orig_atoms == import_atoms:
            print(f"✅ {format_name}: Atom count matches ({orig_atoms})")
        else:
            print(f"⚠️ {format_name}: Atom count differs")

# Test all formats
formats_to_test = {
    "JSON": handler.import_from_json(f"{output_dir}/data.json"),
    "XML": handler.import_from_xml(f"{output_dir}/data.xml"),
    "Pickle": handler.import_from_pickle(f"{output_dir}/data.pkl")
}

for format_name, imported_data in formats_to_test.items():
    verify_round_trip(mmcif, imported_data, format_name)
```

### 🚀 Command Line Interface

#### Using the Demo Script

```bash
# Run interactive demo with sample data
python demo.py --demo

# Process your own files
python demo.py input.cif output.cif

# Process specific categories only
python demo.py input.cif output.cif --categories _atom_site _database_2

# Enable validation
python demo.py input.cif output.cif --validate

# The demo script showcases ALL features above in one comprehensive run!
```

### 💡 Best Practices

#### Choosing the Right Access Method

```python
# ✅ DO: Use dot notation for clean, readable code with known field names
x_coords = mmcif.data_1ABC._atom_site.Cartn_x
first_atom_type = mmcif.data_1ABC._atom_site[0].type_symbol

# ✅ DO: Use dictionary notation for dynamic field access
category_name = input("Enter category name: ")  # User provides "_atom_site"
field_name = input("Enter field name: ")        # User provides "Cartn_x"
if category_name in mmcif.data[0].categories:
    values = mmcif.data[0][category_name][field_name]
    print(f"Found {len(values)} values")

# ✅ DO: Mix approaches when it makes sense
# Loop through categories (dictionary style) but access fields with dot notation
for block in mmcif.data:
    if "_atom_site" in block.categories:
        # Use dot notation for cleaner field access
        print(f"Found {len(block._atom_site.Cartn_x)} atom coordinates")

# ✅ DO: Chain operations naturally
ca_atoms_chain_a = [atom for atom in mmcif.data[0]._atom_site 
                    if atom.label_atom_id == "CA" and atom.label_asym_id == "A"]

# ✅ DO: Use lazy loading for large files
mmcif = handler.parse("huge_file.cif", categories=["_atom_site"])  # Only load what you need

# ✅ DO: Leverage auto-detection
mystery_data = handler.import_auto_detect("unknown_format_file")  # SLOTH figures it out!
```

---

## 🧱 API Reference

### `MMCIFHandler`

- `parse(path, categories=None)`
- `write(mmcif)`
- `import_from_* / export_to_*` for supported formats

### `MMCIFDataContainer`

- `.blocks` → list of block names  
- `.data_1ABC`, `.data[0]`, `['1ABC']` → access block  

### `DataBlock`

- `.categories` → list of categories  
- `._atom_site`, `._entry`, etc. → access categories  

### `Category` and `Row`

- `.item_name` for column access  
- `[i]` for row access  
- `.validate()`, `.row_count`, `.items`

---

## 🧪 Test & Build

```bash
# Install dev tools
pip install -e ".[dev]"

# Run tests
pytest

# Build and check
python -m build
twine check dist/*
```

---

## 📈 Performance Matrix

| File Size     | Full Parse   | Selective Parse | Access Speed | Memory Usage | Example |
|---------------|--------------|-----------------|--------------|---------------|---------|
| <10KB         | ~500μs       | ~400μs          | ~100μs       | ~30KB        | Small samples |
| 10KB–100KB    | ~3-30ms      | ~3-30ms         | ~25μs        | ~100KB-1MB   | Tiny structures |  
| 100KB–1MB     | ~30-300ms    | ~30-300ms       | ~30μs        | ~1-10MB      | Small structures |
| 1MB–10MB      | ~300ms-3s    | ~300ms-3s       | ~40μs        | ~10-100MB    | Medium structures |
| 10MB–100MB    | ~3-15s       | ~3-15s          | ~60μs        | ~100-400MB   | Large structures |
| >100MB        | >15s         | **Recommended** | ~80μs        | >400MB       | Massive datasets |

💡 **Pro tip**: Use `categories=['_atom_site', '_entry', ...]` for selective parsing on large files!  
🦥 **Architecture**: Row and item objects are lazily created and cached for efficiency  
📊 **Real benchmarks**: Based on actual SLOTH performance tests with structures up to 60MB

### Memory Usage and Lazy Architecture

SLOTH uses a smart lazy approach for optimal memory efficiency:

**File parsing**: Fast and eager - the entire file is parsed into memory structures  
**Object creation**: Lazy and cached - Row objects, LazyItemDict, and data access objects are created only when first accessed  
**Data access**: Intelligent caching - frequently accessed objects are cached, while rarely used ones are created on-demand  

**Key insight:** While file content is parsed eagerly, the expensive object creation and data organization is lazy. This means you get fast parsing combined with memory-efficient access patterns.

**Memory efficiency observations:**

- **Small files (<100KB)**: ~10-30x memory overhead due to Python object infrastructure
- **Medium files (100KB-10MB)**: ~10-20x overhead as content scales with fixed costs  
- **Large files (>10MB)**: ~5-15x overhead as raw data dominates object overhead
- **Access speed**: Consistently fast (20-80μs) regardless of file size due to lazy loading

**Lazy components in SLOTH:**

- **LazyRowList**: Row objects created only when accessed, with smart caching
- **LazyItemDict**: Item values loaded only when first requested  
- **LazyKeyList**: Key enumeration with O(1) prefix operations
- **Row caching**: Individual rows cached after first access for repeated use
- **Cached properties**: Expensive computations cached using `@cached_property`

---

## 🤝 Contributing

1. Fork the repo  
2. Create a feature branch  
3. Add tests  
4. Submit a pull request

---

## 📄 License

MIT License – do anything, just don’t sue.

---

## 🧠 Closing Words

> **Stop parsing. Start accessing.**  
> SLOTH is not just a parser — it’s a mindset:  
> Pythonic. Lazy. Elegant. Fast.
