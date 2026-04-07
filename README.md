# SLOTH – *S*tructural *L*oader with *O*n-demand *T*raversal *H*andling

> *Lazy by design. Fast by default.*

![logo](https://raw.githubusercontent.com/lucas-ebi/sloth/main/logo.png)

<!-- Uncomment when published to PyPI:
[![PyPI](https://badge.fury.io/py/sloth-mmcif.svg)](https://badge.fury.io/py/sloth-mmcif)
-->
[![Version](https://img.shields.io/badge/version-0.5.3-blue)](https://github.com/lucas-ebi/sloth/releases)
<!-- 
[![Python](https://img.shields.io/pypi/pyversions/sloth-mmcif.svg)](https://pypi.org/project/sloth-mmcif/)
-->
[![Python](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/github/license/lucas-ebi/sloth.svg)](https://github.com/lucas-ebi/sloth/blob/main/LICENSE)
[![Docs](https://img.shields.io/badge/docs-Read%20the%20Docs-blue)](https://sloth-mmcif.readthedocs.io/)

---

**SLOTH** is a fast, flexible mmCIF parser for structural biology workflows.
Built on the C++ [gemmi](https://gemmi.readthedocs.io/) backend, it performs
eager parsing and lazy object construction — efficient for both large-scale
pipelines and interactive exploration.

- **High-speed parsing** via gemmi
- **Lazy construction** of row and item objects for memory efficiency
- **Pythonic dot-notation** access to mmCIF data
- **Pluggable validation** with cross-category support
- **JSON export/import** with automatic relationship resolution

## Installation

```bash
pip install -i https://test.pypi.org/simple/ sloth-mmcif
```

Or from source:

```bash
git clone https://github.com/lucas-ebi/sloth.git
cd sloth
pip install -e ".[dev]"
```

## Quick Start

```python
from sloth import MMCIFHandler

handler = MMCIFHandler()
mmcif = handler.read("1abc.cif")

# Dot notation
print(mmcif.data_1ABC._struct.title[0])
print(mmcif.data_1ABC._atom_site.Cartn_x[0])

# Dictionary notation
x = mmcif.data[0]["_atom_site"]["Cartn_x"]

# Export to nested JSON
handler.export(mmcif, file_path="output.json", indent=2)
```

## Performance

Benchmarks on synthetic mmCIF files (macOS, Python 3.10):

| File Size | Full Parse | Selective | Access Speed | Memory (Parse) | Memory (Access) |
| --------- | ---------- | --------- | ------------ | -------------- | --------------- |
| 1KB       | 12ms       | 13ms      | 40μs         | 198KB          | 4KB             |
| 10KB      | 12ms       | 13ms      | 97μs         | 222KB          | 13KB            |
| 100KB     | 13ms       | 14ms      | 594μs        | 1.0MB          | 104KB           |
| 1.0MB     | 19ms       | 25ms      | 6ms          | 7.7MB          | 954KB           |
| 50.7MB    | 394ms      | 693ms     | 298ms        | 205.4MB        | 46.1MB          |
| 102.0MB   | 817ms      | 1.4s      | 607ms        | 386.8MB        | 75.5MB          |

> **Note:** Access memory can appear smaller than the file on disk because
> Python's [string interning](https://docs.python.org/3/library/sys.html#sys.intern)
> deduplicates repeated values in mmCIF columns (e.g., atom type symbols,
> residue names, chain IDs). When many rows share the same string, Python
> stores it only once — so memory usage after access reflects unique string
> content rather than total row count.

## Documentation

Full documentation, API reference, and interactive cookbook:

- **[Read the Docs](https://sloth-mmcif.readthedocs.io/)** — User guide & API reference
- **[Cookbook](https://sloth-mmcif.readthedocs.io/en/latest/cookbook.html)** — Interactive Jupyter notebook tutorial

## Contributing

1. Fork the repo
2. Create a feature branch
3. Add tests
4. Submit a PR

## License

MIT License — use freely, modify responsibly.
