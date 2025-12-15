"""
SLOTH: Structural Loader with On-demand Traversal Handling

Lazy by design. Fast by default.

A high-performance mmCIF parser using gemmi backend with SLOTH's elegant API.
Gemmi is now the default backend for optimal performance.

Version: 0.4.1
"""

__version__ = "0.4.1"
__author__ = "Lucas Carrijo de Oliveira"
__email__ = "lucas@ebi.ac.uk"
__license__ = "MIT"

# Version info tuple for programmatic access
VERSION_INFO = tuple(map(int, __version__.split('.')))

from .models import (
    MMCIFDataContainer,
    DataBlock,
    Category,
    Row,
    Item,
    DataSourceFormat,
)
from .parser import MMCIFParser
from .writer import MMCIFWriter
from .exporter import JSONExporter
from .importer import JSONImporter
from .handler import MMCIFHandler
from .common import BaseImporter, BaseExporter
from .plugins import ValidatorFactory
from .validator import ValidationError, ValidationSeverity
from .serializer import (
    MappingGenerator,
    DictionaryParser,
    CacheManager,
    get_cache_manager,
    RelationshipResolver,
)

__all__ = [
    # Core components
    "MMCIFHandler",
    "MMCIFParser",
    "MMCIFWriter",
    # Data models
    "MMCIFDataContainer",
    "DataBlock",
    "Category",
    "Row",
    "Item",
    "DataSourceFormat",
    # Import/Export components
    "JSONExporter",
    "JSONImporter",
    # Base classes
    "BaseImporter",
    "BaseExporter",
    # Validation components
    "ValidatorFactory",
    "ValidationError",
    "ValidationSeverity",
    # Serializer components
    "MappingGenerator", 
    "DictionaryParser",
    "CacheManager", 
    "get_cache_manager",
    "RelationshipResolver",
    # Version information
    "__version__",
    "__author__",
    "__license__",
    "VERSION_INFO",
]
