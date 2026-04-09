"""
SLOTH: Structural Loader with On-demand Traversal Handling

Lazy by design. Fast by default.

A high-performance mmCIF parser using gemmi backend with SLOTH's elegant API.
Gemmi is now the default backend for optimal performance.

Version: 0.8.1
"""

__version__ = "0.8.1"
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
    SchemaWarning,
)
from .parser import MMCIFParser
from .writer import MMCIFWriter
from .exporter import JSONExporter
from .importer import JSONImporter
from .handler import MMCIFHandler
from .common import BaseImporter, BaseExporter
from .plugins import (
    PluginFactory,
    Plugin,
    PluginWrapper,
    FunctionPlugin,
)
from .validator import (
    ValidationError,
    ValidationSeverity,
    ValidatorPlugin,
    CategoryValidator,
    ValidationReport,
    DataBlockValidator,
    BlockValidationWrapper,
    ContainerValidator,
    ContainerValidationWrapper,
    # Validator classes
    SchemaValidator,
    MMCIFValidator,
    # Single-category rule factories
    mandatory_items,
    one_of_following,
    value_length,
    value_range,
    conditional_mandatory,
    regex_check,
    ordering_check,
    allowed_pairs,
    min_rows,
    enumeration_check,
    type_check,
    # Cross-category rule factories
    foreign_key,
    parent_child,
    composite_key,
    oper_expression,
    cross_mandatory,
    cross_ordering,
)
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
    "SchemaWarning",
    # Import/Export components
    "JSONExporter",
    "JSONImporter",
    # Base classes
    "BaseImporter",
    "BaseExporter",
    # Plugin system
    "PluginFactory",
    "Plugin",
    "PluginWrapper",
    "FunctionPlugin",
    # Validation components
    "ValidatorPlugin",
    "CategoryValidator",
    "ValidationError",
    "ValidationSeverity",
    "ValidationReport",
    "DataBlockValidator",
    "BlockValidationWrapper",
    "ContainerValidator",
    "ContainerValidationWrapper",
    # Rules
    "SchemaValidator",
    "MMCIFValidator",
    "mandatory_items",
    "one_of_following",
    "value_length",
    "value_range",
    "conditional_mandatory",
    "regex_check",
    "ordering_check",
    "allowed_pairs",
    "min_rows",
    "enumeration_check",
    "type_check",
    "foreign_key",
    "parent_child",
    "composite_key",
    "oper_expression",
    "cross_mandatory",
    "cross_ordering",
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
