"""
SLOTH: Structural Loader with On-demand Tokenization and Handling

Lazy by design. Fast by default.

A memory-mapped, lazily-loaded mmCIF parser written in pure Python.  
It loads what you need, when you need it — no more, no less.

Built in Python. No templates. No regrets.
"""

from .main import (
    MMCIFHandler,
    MMCIFParser,
    MMCIFWriter,
    MMCIFExporter,
    MMCIFDataContainer,
    DataBlock,
    Category,
    Row,
    Item,
    ValidatorFactory
)
from .version import __version__

__all__ = [
    'MMCIFHandler',
    'MMCIFParser',
    'MMCIFWriter',
    'MMCIFExporter',
    'MMCIFDataContainer',
    'DataBlock',
    'Category',
    'Item',
    'ValidatorFactory',
    '__version__'
]
