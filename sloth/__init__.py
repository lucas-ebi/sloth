"""
SLOTH: Structural Loader with On-demand Traversal Handling

Lazy by design. Fast by default.

A memory-mapped, lazily-loaded mmCIF parser written in pure Python.  
It loads what you need, when you need it — no more, no less.

Built in Python. No templates. No regrets.
"""

__version__ = "0.1.0"

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
from .validator import ValidatorFactory
from .schemas import (
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

__all__ = [
    "MMCIFHandler",
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
    "__version__",
]
