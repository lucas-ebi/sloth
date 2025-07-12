"""
SLOTH Package - Simple re-export from mmcif

This allows both import styles:
- from sloth import MMCIFHandler
- from sloth.mmcif import MMCIFHandler
"""

# Re-export everything from mmcif submodule
from .mmcif import *
from .mmcif import __all__ as _mmcif_all

# Ensure __all__ is properly set
__all__ = _mmcif_all
