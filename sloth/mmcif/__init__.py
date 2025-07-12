"""
SLOTH: Structural Loader with On-demand Traversal Handling

Lazy by design. Fast by default.

A high-performance mmCIF parser using gemmi backend with SLOTH's elegant API.
Gemmi is now the default backend for optimal performance.

Version: 0.2.0
"""

__version__ = "0.2.0"
__author__ = "Lucas"
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
from .exporter import JSONExporter, XMLExporter
from .importer import (
    JSONImporter,
    XMLImporter,
)
from .handler import MMCIFHandler
from .common import BaseImporter, BaseExporter
from .defaults import StructureFormat, ExportFormat
from .plugins import ValidatorFactory
from .validator import (
    SchemaValidator,
    JSONSchemaValidator,
    XMLSchemaValidator,
    YAMLSchemaValidator,
    CSVSchemaValidator,
    ValidationError,
    ValidationSeverity,
    SchemaValidatorFactory,
    default_mmcif_json_nested_schema,
    default_mmcif_json_flat_schema,
)
from .serializer import (
    PDBMLConverter,
    MappingGenerator,
    DictionaryParser,
    XSDParser,
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
    "XMLExporter",
    "JSONImporter", 
    "XMLImporter",
    # Base classes
    "BaseImporter",
    "BaseExporter",
    # Enums
    "StructureFormat",
    "ExportFormat",
    # Validation components
    "ValidatorFactory",
    "SchemaValidator",
    "JSONSchemaValidator",
    "XMLSchemaValidator",
    "YAMLSchemaValidator",
    "CSVSchemaValidator",
    "ValidationError",
    "ValidationSeverity",
    "SchemaValidatorFactory",
    "default_mmcif_json_nested_schema",
    "default_mmcif_json_flat_schema",
    # PDBML Converter components
    "PDBMLConverter",
    "MappingGenerator", 
    "DictionaryParser",
    "XSDParser",
    "CacheManager", 
    "get_cache_manager",
    "RelationshipResolver",
    # Version information
    "__version__",
    "__author__",
    "__license__",
    "VERSION_INFO",
]
