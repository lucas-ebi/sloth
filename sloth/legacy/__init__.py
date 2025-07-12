"""
SLOTH Legacy Module - Original Pure Python Implementation

This module contains the original SLOTH parsing and writing implementation
that was purely Python-based. These implementations have been moved to a
legacy archive and replaced with gemmi-based implementations for better performance.

The legacy implementations are still available for compatibility and reference purposes.

Usage:
    from legacy import MMCIFParser, MMCIFWriter
    
Note: The legacy implementations use the same API as the current implementations
but may have different performance characteristics.
"""

from .parser import MMCIFParser as MMCIFParser
from .writer import MMCIFWriter as MMCIFWriter

__all__ = [
    "MMCIFParser",
    "MMCIFWriter",
]
