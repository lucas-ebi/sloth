
# 🦥 SLOTH – Structural Loader with On-demand Tokenization and Handling

> 🧠 *Lazy by design. Fast by default.*

![SLOTH](https://img.shields.io/badge/SLOTH-Lazy%20by%20Design%2C%20Fast%20by%20Default-blueviolet?logo=python&logoColor=white)
[![PyPI](https://badge.fury.io/py/sloth-mmcif.svg)](https://badge.fury.io/py/sloth-mmcif)
[![Python](https://img.shields.io/pypi/pyversions/sloth-mmcif.svg)](https://pypi.org/project/sloth-mmcif/)
[![License](https://img.shields.io/github/license/lucas/sloth.svg)](https://github.com/lucas/sloth/blob/main/LICENSE)

---

## 🚀 Overview

**SLOTH** is a memory-mapped, lazily-loaded mmCIF parser written in pure Python.  
It only loads what you need, when you need it — no more, no less.

Built for speed, simplicity, and elegance, SLOTH is ideal for:
- ⚡ Interactive structural analysis
- 📊 Automated pipelines
- 🧠 Efficient exploration of massive mmCIF files

---

## ✨ Key Features

✅ **Simple API** – One optimal way to create, parse, and access  
⚡ **Fast and Lazy** – Memory-mapped + lazy loading = instant access  
📦 **Complete** – Access to all mmCIF blocks, categories, and items  
🔧 **Robust** – Handles anything from tiny files to >1GB behemoths  
🔄 **Import/Export** – JSON, XML, YAML, Pickle, CSV, Pandas  

---

## 🧠 Philosophy

> "*Why rush when you can prefetch?*"  
> "*Not everything needs to be a C++ monument.*"

SLOTH is unapologetically Pythonic:
- No overengineering
- No runtime flags
- No manual optimization

Just smart defaults and expressive code.

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

- **Startup time**: Fast  
- **Access time**: Instant (lazy evaluation)  
- **Memory usage**: Minimal  

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

| File Size     | Startup Time | Access Speed | Memory Usage |
|---------------|--------------|--------------|---------------|
| <1MB          | Instant      | Instant      | Tiny          |
| 1MB–100MB     | Fast         | Fast         | Efficient     |
| >100MB–1GB    | Fast         | Fast         | Optimized     |
| >1GB          | Fast         | Lazy         | Minimal       |

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
