# SLOTH – *S*tructural *L*oader with *O*n-demand *T*raversal *H*andling

> *Lazy by design. Fast by default.*

<img width="256" height="256" alt="logo" src="logo.png" />

[![PyPI](https://badge.fury.io/py/sloth-mmcif.svg)](https://badge.fury.io/py/sloth-mmcif)
[![Version](https://img.shields.io/badge/version-0.3.0-blue)](https://github.com/lucas-ebi/sloth/releases)
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
11. [Migration to Gemmi Backend](#migration-to-gemmi-backend)
12. [Legacy Support](#legacy-support)
13. [Performance and Architecture](#performance-and-architecture)
14. [Best Practices](#best-practices)
15. [Contributing](#contributing)
16. [License](#license)
17. [Closing Notes](#closing-notes)

---

## Overview

**SLOTH** (***S**tructural **L**oader with **O**n-demand **T**raversal **H**andling*) is a fast, flexible mmCIF parser designed for structural biology workflows. Built on the C++ [gemmi](https://gemmi.readthedocs.io/) backend, SLOTH performs eager parsing and lazy object construction, making it efficient for both large-scale pipelines and interactive exploration.

---

## Features

* High-speed parsing via gemmi
* Lazy construction of row and item objects for memory efficiency
* Pythonic, dot-notation access to mmCIF data
* Pluggable custom validation system
* Export and import in JSON format (nested and flat structures)

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
from sloth import StructureFormat

# Export to JSON nested format (default, pretty-printed)
json_nested = handler.export(mmcif, structure_format=StructureFormat.NESTED, indent=2)
handler.export(mmcif, structure_format=StructureFormat.NESTED, file_path="out_nested.json", indent=2)

# Export to JSON flat format (optimized for large datasets)
json_flat = handler.export(mmcif, structure_format=StructureFormat.FLAT, indent=2)
handler.export(mmcif, structure_format=StructureFormat.FLAT, file_path="out_flat.json", indent=2)

# Compact JSON (no indentation)
handler.export(mmcif, structure_format=StructureFormat.NESTED, file_path="out_compact.json")
```

### Import

```python
# Auto-detect structure format from JSON file
mmcif = handler.load("out_nested.json")

# Or specify format explicitly
from sloth import DataSourceFormat

mmcif = handler.load("out_flat.json", format_type=DataSourceFormat.JSON)
```

### Round-trip validation

```python
def verify_round_trip(orig, imported, fmt):
    ob = orig.data[0]
    ib = imported.data[0]
    if len(ob.categories) == len(ib.categories):
        print(f"{fmt}: Categories OK")
    if "_atom_site" in ob.categories:
        n1 = len(ob._atom_site.Cartn_x)
        n2 = len(ib._atom_site.Cartn_x)
        print(f"{fmt}: Atoms {'OK' if n1 == n2 else 'Mismatch'}")
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
* Exporting to JSON (nested and flat structures)
* Importing from JSON
* Round-trip validation
* Writing modified mmCIF files
* Complete workflow examples

Perfect for learning SLOTH interactively or as a reference guide.

---

## Migration to Gemmi Backend

| Feature       | v0.1.x      | v0.2.0+ (current) |
| ------------- | ----------- | ----------------- |
| Backend       | Pure Python | Gemmi (C++)       |
| `use_gemmi`   | Optional    | Removed           |
| Performance   | Moderate    | High              |
| Compatibility | N/A         | `sloth.legacy`    |

Your code will continue to work. Only explicit `use_gemmi=False` needs updating.

---

## Legacy Support

```python
from sloth.legacy import MMCIFParser, MMCIFWriter

parser = MMCIFParser()
mmcif = parser.parse("1abc.cif")
```

Same dot-notation access, same serialization features.

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
