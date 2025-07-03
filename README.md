# SLOTH – Structural Loader with On-demand Traversal Handling

> *Lazy by design. Fast by default.*

[![PyPI](https://badge.fury.io/py/sloth-mmcif.svg)](https://badge.fury.io/py/sloth-mmcif)
[![Version](https://img.shields.io/badge/version-0.2.0-blue)](https://github.com/lucas-ebi/sloth/releases)
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
10. [CLI Usage](#cli-usage)
11. [Migration to Gemmi Backend](#migration-to-gemmi-backend)
12. [Legacy Support](#legacy-support)
13. [Performance and Architecture](#performance-and-architecture)
14. [Best Practices](#best-practices)
15. [Contributing](#contributing)
16. [License](#license)
17. [Closing Notes](#closing-notes)

---

## Overview

**SLOTH** (*Structural Loader with On-demand Traversal Handling*) is a fast, flexible mmCIF parser designed for structural biology workflows. Built on the C++ [gemmi](https://gemmi.readthedocs.io/) backend, SLOTH performs eager parsing and lazy object construction, making it efficient for both large-scale pipelines and interactive exploration.

---

## Features

* High-speed parsing via gemmi
* Lazy construction of row and item objects for memory efficiency
* Pythonic, dot-notation access to mmCIF data
* Pluggable custom validation system
* Export and import in JSON, XML, YAML, Pickle, CSV, and Pandas formats

---

## Philosophy

> *"Why rush when you can be lazy?"*
> *Parse eagerly. Construct lazily. Access quickly.*

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
import sloth

handler = MMCIFHandler()
mmcif = handler.parse("1abc.cif")

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
handler.export_to_json(mmcif, "out.json")
handler.export_to_xml(mmcif, "out.xml")
handler.export_to_yaml(mmcif, "out.yml")
handler.export_to_pickle(mmcif, "out.pkl")
handler.export_to_csv(mmcif, "csv_dir")
dfs = handler.export_to_pandas(mmcif)
```

### Import

```python
mmcif = handler.import_from_json("out.json")
mmcif = handler.import_from_xml("out.xml")
mmcif = handler.import_from_yaml("out.yml")
mmcif = handler.import_from_pickle("out.pkl")
mmcif = handler.import_auto_detect("out.txt")
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
mmcif = handler.parse("1abc.cif")
mmcif.data_1ABC._atom_site.validate()
```

---

## Example CLI Usage

```bash
python demo.py --demo
python demo.py input.cif output.cif --categories _atom_site --validate
```

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
mmcif = parser.parse_file("1abc.cif")
```

Same dot-notation access, same serialization features.

---

## Performance and Architecture

| File Size     | Full Parse | Selective Parse | Access Speed | Memory Usage |
| ------------- | ---------- | --------------- | ------------ | ------------ |
| <10KB         | 28ms       | 204μs           | 51μs         | 4.0MB        |
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

# Auto-detect import
mmcif = handler.import_auto_detect("file.ext")

# Partial category loading
mmcif = handler.parse("file.cif", categories=["_atom_site"])
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
