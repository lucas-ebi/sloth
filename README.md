# SLOTH – *S*tructural *L*oader with *O*n-demand *T*raversal *H*andling

> *Lazy by design. Fast by default.*

<img width="256" height="256" alt="logo" src="logo.png" />

[![PyPI](https://badge.fury.io/py/sloth-mmcif.svg)](https://badge.fury.io/py/sloth-mmcif)
[![Version](https://img.shields.io/badge/version-0.3.44-blue)](https://github.com/lucas-ebi/sloth/releases)
[![Python](https://img.shields.io/pypi/pyversions/sloth-mmcif.svg)](https://pypi.org/project/sloth-mmcif/)
[![License](https://img.shields.io/github/license/lucas-ebi/sloth.svg)](https://github.com/lucas-ebi/sloth/blob/main/LICENSE)

---

## Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [Philosophy](#philosophy)
4. [Installation](#installation)
5. [Quick Start](#quick-start)
6. [API and Access Patterns](#api-and-access-patterns)

   * [Dot and Dictionary Notation](#dot-and-dictionary-notation)
   * [Row-wise and Column-wise Access](#row-wise-and-column-wise-access)
   * [Filtering and Slicing](#filtering-and-slicing)
   * [Iterative Access](#iterative-access)
7. [Data Creation](#data-creation)
8. [Import and Export](#import-and-export)
9. [Validation](#validation)
10. [Interactive Cookbook](#interactive-cookbook)
11. [Performance and Architecture](#performance-and-architecture)
12. [Best Practices](#best-practices)
13. [Contributing](#contributing)
14. [License](#license)
15. [Closing Notes](#closing-notes)

---

## Overview

**SLOTH** (***S**tructural **L**oader with **O**n-demand **T**raversal **H**andling*) is a fast, flexible mmCIF parser designed for structural biology workflows. Built on the C++ [gemmi](https://gemmi.readthedocs.io/) backend, SLOTH performs eager parsing and lazy object construction, making it efficient for both large-scale pipelines and interactive exploration.

---

## Features

* High-speed parsing via gemmi
* Lazy construction of row and item objects for memory efficiency
* Pythonic, dot-notation access to mmCIF data
* Pluggable custom validation system
* Export and import in nested JSON format with automatic relationship resolution

---

## Philosophy

> *"Why rush when you can be lazy?"*  
> *"Parse eagerly. Construct lazily. Access quickly."*

SLOTH is unapologetically Pythonic:

* No runtime flags
* No manual optimizations
* No overengineering

Just smart defaults, clear abstractions, and expressive maintainable code.

---

## Installation

<!-- Install via PyPI:

```bash
pip install sloth-mmcif
``` -->

Install from source:

```bash
git clone https://github.com/lucas-ebi/sloth.git
cd sloth
pip install -e ".[dev]"
```

---

## Quick Start

```python
from sloth import MMCIFHandler

handler = MMCIFHandler()
mmcif = handler.read("1abc.cif")

print(mmcif.data_1ABC._struct.title[0])
print(mmcif.data_1ABC._atom_site.Cartn_x[0])
```

---

## API and Access Patterns

### Dot and Dictionary Notation

```python
# Dot notation
block = mmcif.data_1ABC
atom_site = block._atom_site
print(atom_site.Cartn_x[0])

# Dictionary notation (dynamic fields)
category_name = "_atom_site"
field_name = "Cartn_x"
x = mmcif.data[0][category_name][field_name]
```

### Row-wise and Column-wise Access

```python
x_coords = atom_site.Cartn_x
first_atom = atom_site[0]
print(first_atom.type_symbol, first_atom.Cartn_x)
```

### Filtering and Slicing

```python
# CA atoms from chain A
ca_atoms = [a for a in atom_site if a.label_atom_id == "CA" and a.label_asym_id == "A"]

# Mean X coordinate
avg_x = sum(float(x) for x in atom_site.Cartn_x) / atom_site.row_count
```

### Iterative Access

```python
for cat_name in block.categories:
    category = block[cat_name]
    for item_name in category.items:
        print(f"{cat_name}.{item_name}: {len(category[item_name])} values")
```

---

## Data Creation

### Manual file creation

```python
sample = """data_1ABC
_entry.id 1ABC_STRUCTURE
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
with open("sample.cif", "w") as f:
    f.write(sample)
```

### Programmatic using object model

```python
from sloth.models import MMCIFDataContainer, DataBlock, Category

mmcif = MMCIFDataContainer()
block = DataBlock("1ABC")

cat = Category("_entry")
cat["id"] = ["1ABC_STRUCTURE"]
block["_entry"] = cat

mmcif["1ABC"] = block
```

### Dot-based auto-creation

```python
mmcif = MMCIFDataContainer()
mmcif.data_1ABC._entry.id = ["1ABC_STRUCTURE"]
mmcif.data_1ABC._atom_site.Cartn_x = ["10.1", "11.2"]
```

---

## Import and Export

### Export

```python
# Export to nested JSON format (with resolved parent-child relationships)
# Returns JSON string
json_str = handler.export(mmcif, indent=2)

# Export to file (pretty-printed)
handler.export(mmcif, file_path="out_nested.json", indent=2)

# Compact JSON (no indentation)
handler.export(mmcif, file_path="out_compact.json")
```

**Nested JSON Structure:**

SLOTH automatically resolves mmCIF dictionary relationships when exporting to JSON. Child categories are nested within their parent categories, creating a hierarchical structure:

```json
{
  "data_DEMO": {
    "_entry": [...],
    "_entity": [
      {
        "id": "1",
        "type": "polymer",
        "_entity_poly": [
          {
            "entity_id": "1",
            "_entity_poly_seq": [...]
          }
        ],
        "_struct_asym": [
          {
            "id": "A",
            "_atom_site": [...]
          }
        ]
      }
    ]
  }
}
```

Note: All category names maintain the `_` prefix convention, whether at the top level or nested.

### Import

```python
# Import from JSON file (automatically flattens nested structure back to mmCIF)
mmcif = handler.load("out_nested.json")

# Access data using standard mmCIF notation
print(mmcif.data[0]._entity.id)
print(mmcif.data[0]._atom_site.Cartn_x)
```

### Round-trip validation

```python
def verify_round_trip(original, imported):
    """Verify data integrity after JSON export/import round-trip."""
    orig_block = original.data[0]
    imp_block = imported.data[0]
    
    # Check categories preserved
    if set(orig_block.categories) == set(imp_block.categories):
        print("✅ Categories: OK")
    
    # Check atom data preserved
    if "_atom_site" in orig_block.categories:
        orig_count = len(orig_block._atom_site.Cartn_x)
        imp_count = len(imp_block._atom_site.Cartn_x)
        print(f"✅ Atoms: {'OK' if orig_count == imp_count else 'Mismatch'}")

# Test round-trip
handler.export(mmcif, file_path="test.json")
imported = handler.load("test.json")
verify_round_trip(mmcif, imported)
```

---

## Validation

```python
from sloth import ValidatorFactory

vf = ValidatorFactory()
vf.register_validator("_atom_site", lambda cat: print("Validating", cat.name))

handler = MMCIFHandler(validator_factory=vf)
mmcif = handler.read("1abc.cif")
mmcif.data_1ABC._atom_site.validate()
```

---

## Interactive Cookbook

SLOTH includes a comprehensive Jupyter notebook cookbook that demonstrates all features interactively:

```bash
jupyter notebook SLOTH_Cookbook.ipynb
```

The cookbook covers:

* Parsing mmCIF files with embedded data
* Exploring data structures with dot notation
* 2D slicing (column-wise and row-wise access)
* Validating mmCIF data
* Modifying data elegantly
* Creating sample data (manual, programmatic, and auto-creation)
* Exporting to nested JSON with automatic relationship resolution
* Importing from JSON with automatic flattening
* Round-trip validation
* Writing modified mmCIF files
* Complete workflow examples

Perfect for learning SLOTH interactively or as a reference guide.

---

## Performance and Architecture

| File Size     | Full Parse | Selective Parse | Access Speed | Memory Usage |
| ------------- | ---------- | --------------- | ------------ | ------------ |
| <10KB         | 28μs       | 204μs           | 51μs         | 4.0MB        |
| 10KB–100KB    | 703μs      | 634μs           | 22μs         | 172KB        |
| 100KB–1MB     | 6ms        | 5ms             | 35μs         | 2.1MB        |
| 1MB–10MB      | 77ms       | 57ms            | 52μs         | 18.8MB       |
| 10MB–50MB     | 601ms      | 540ms           | 64μs         | 243MB        |
| >50MB         | 2.9s       | 3.2s            | 66μs         | 271MB        |

SLOTH's lazy object creation ensures minimal overhead even on large files.

---

## Best Practices

```python
# Dot access for known fields
x = mmcif.data_1ABC._atom_site.Cartn_x

# Dict access for dynamic fields
val = mmcif.data[0]["_atom_site"]["Cartn_x"]

# Partial category loading
mmcif = handler.read("file.cif", categories=["_atom_site"])
```

---

## Contributing

1. Fork
2. Create a branch
3. Add tests
4. Submit a PR

---

## License

MIT License — use freely, modify responsibly.

---

## Closing Notes

> SLOTH is not just a parser — it’s a mindset.  
> Pythonic. Lazy. Elegant. Fast.
