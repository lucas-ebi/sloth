"""
SLOTH: Structural Loader with On-demand Traversal Handling

Lazy by design. Fast by default.

A high-performance mmCIF parser using gemmi backend with SLOTH's elegant API.
Now using gemmi as the default backend for optimal performance while maintaining
the exact same API.

The original pure Python implementation has been moved to sloth.legacy for
compatibility and reference purposes.

MIGRATION NOTE (v0.1.x → v0.2.0+): 
- The `use_gemmi` parameter has been removed from MMCIFHandler as gemmi is now the default
- For legacy compatibility, use: from sloth.legacy import MMCIFParser, MMCIFWriter
- All existing code should continue to work with better performance
- Version 0.2.0+ requires gemmi as a core dependency

Version: 0.2.0
"""

__version__ = "0.2.0"
__author__ = "Lucas"
__email__ = "lucas@ebi.ac.uk"
__license__ = "MIT"

# Version info tuple for programmatic access
VERSION_INFO = tuple(map(int, __version__.split('.')))

# Migration information
MIGRATION_INFO = {
    "from_version": "0.1.x",
    "to_version": "0.2.0+",
    "breaking_changes": [
        "use_gemmi parameter removed (gemmi is now default)",
        "gemmi is now a required dependency",
    ],
    "compatibility": "Full backward compatibility maintained",
    "legacy_access": "sloth.legacy module for original implementation"
}

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
from .exporter import MMCIFExporter
from .loaders import (
    MMCIFImporter,
    FormatLoader,
    JsonLoader,
    XmlLoader,
    YamlLoader,
    PickleLoader,
    CsvLoader,
    DictToMMCIFConverter,
)
from .handler import MMCIFHandler
from .plugins import ValidatorFactory
from .validators import (
    SchemaValidator,
    JSONSchemaValidator,
    XMLSchemaValidator,
    YAMLSchemaValidator,
    CSVSchemaValidator,
    ValidationError,
    ValidationSeverity,
    SchemaValidatorFactory,
    default_mmcif_json_schema,
)
from .serializers import (
    PDBMLConverter,
    MappingGenerator,
    DictionaryParser,
    XSDParser,
    HybridCache,
    RelationshipResolver,
    MMCIFToPDBMLPipeline,
)

__all__ = [
    "MMCIFHandler",
    # Main components now use gemmi backend by default
    "MMCIFParser",
    "MMCIFWriter",
    "MMCIFExporter",
    "MMCIFImporter",
    "MMCIFDataContainer",
    "DataBlock",
    "Category",
    "Row",
    "Item",
    "ValidatorFactory",
    "DataSourceFormat",
    "FormatLoader",
    "JsonLoader",
    "XmlLoader",
    "YamlLoader",
    "PickleLoader",
    "CsvLoader",
    "DictToMMCIFConverter",
    "SchemaValidator",
    "JSONSchemaValidator",
    "XMLSchemaValidator",
    "YAMLSchemaValidator",
    "CSVSchemaValidator",
    "ValidationError",
    "ValidationSeverity",
    "SchemaValidatorFactory",
    "default_mmcif_json_schema",
    # PDBML Converter components
    "PDBMLConverter",
    "MappingGenerator", 
    "DictionaryParser",
    "XSDParser",
    "HybridCache", 
    "RelationshipResolver",
    "MMCIFToPDBMLPipeline",
    # Version information
    "__version__",
    "__author__",
    "__license__",
    "VERSION_INFO",
    "MIGRATION_INFO",
    # Note: For legacy implementations, use:
    # from sloth.legacy import MMCIFParser, MMCIFWriter
]
