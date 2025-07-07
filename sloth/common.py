"""
Common utilities and shared dependencies for the sloth package,
including abstract base classes for SLOTH parsers and writers.

This module contains shared functionality that would otherwise create circular imports
between different modules in the package. It also defines the common interfaces that 
all parsers and writers must implement, ensuring consistency across different backends 
(native SLOTH, gemmi, etc.).
"""

import os
from abc import ABC, abstractmethod
from typing import Optional, Union, IO
from pathlib import Path
from .models import MMCIFDataContainer, DataSourceFormat
from .plugins import ValidatorFactory
from .validators import SchemaValidator


def auto_detect_format_and_load(
    file_path: str,
    validator_factory: Optional[ValidatorFactory] = None,
    schema_validator: Optional[SchemaValidator] = None,
    validate_schema: bool = False,
) -> MMCIFDataContainer:
    """
    Auto-detect the format of the input file and load it.

    This function is extracted from MMCIFImporter to avoid circular imports
    between loaders.py and handler.py.

    Args:
        file_path: Path to the file
        validator_factory: Optional validator factory for data validation
        schema_validator: Optional schema validator for format-specific schema validation
        validate_schema: Whether to validate against schema (if schema_validator is None,
                      will try to create one from SchemaValidatorFactory)

    Returns:
        MMCIFDataContainer object
    """
    # Create schema validator if needed and requested
    format_specific_validator = None
    if validate_schema:
        if schema_validator is not None:
            format_specific_validator = schema_validator
        else:
            try:
                # Import here to avoid circular imports
                from .validators import SchemaValidatorFactory

                # Detect format first
                detected_format = None
                if os.path.isdir(file_path):
                    detected_format = DataSourceFormat.CSV
                else:
                    ext = os.path.splitext(file_path.lower())[1]
                    format_map = {
                        ".json": DataSourceFormat.JSON,
                        ".xml": DataSourceFormat.XML,
                        ".yaml": DataSourceFormat.YAML,
                        ".yml": DataSourceFormat.YAML,
                        ".pkl": DataSourceFormat.PICKLE,
                        ".pickle": DataSourceFormat.PICKLE,
                    }
                    detected_format = format_map.get(ext)

                if detected_format:
                    format_specific_validator = SchemaValidatorFactory.create_validator(
                        detected_format
                    )
            except (ImportError, ValueError, Exception):
                # If schema validation can't be created, continue without it
                pass

    # Import loaders here to avoid circular imports
    from .loaders import MMCIFImporter

    # Check if it's a directory (for CSV files)
    if os.path.isdir(file_path):
        return MMCIFImporter.from_csv_files(
            file_path, validator_factory, format_specific_validator
        )

    ext = os.path.splitext(file_path.lower())[1]
    if ext == ".json":
        return MMCIFImporter.from_json(
            file_path, validator_factory, format_specific_validator
        )
    elif ext == ".xml":
        return MMCIFImporter.from_xml(
            file_path, validator_factory, format_specific_validator
        )
    elif ext in (".yaml", ".yml"):
        return MMCIFImporter.from_yaml(
            file_path, validator_factory, format_specific_validator
        )
    elif ext in (".pkl", ".pickle"):
        return MMCIFImporter.from_pickle(
            file_path, validator_factory, format_specific_validator
        )
    elif ext == ".csv":
        return MMCIFImporter.from_csv_files(file_path, validator_factory)
    elif ext == ".cif":
        # Import here to avoid circular imports
        from .handler import MMCIFHandler

        handler = MMCIFHandler()
        container = handler.parse(file_path)
        container.source_format = DataSourceFormat.MMCIF
        return container
    raise ValueError(f"Unsupported file extension: {ext}")


class BaseParser(ABC):
    """
    Abstract base class for mmCIF parsers.
    
    All parser implementations (MMCIFParser, GemmiParser, etc.) must inherit from this
    class and implement the required abstract methods.
    """
    
    def __init__(
        self,
        validator_factory: Optional[ValidatorFactory] = None,
        categories: Optional[list] = None,
    ):
        """
        Initialize the parser.
        
        :param validator_factory: Optional validator factory for data validation
        :param categories: Optional list of categories to parse (for performance)
        """
        self.validator_factory = validator_factory
        self.categories = categories
    
    @abstractmethod
    def parse_file(self, file_path: Union[str, Path]) -> MMCIFDataContainer:
        """
        Parse mmCIF file and return a data container.
        
        :param file_path: Path to the mmCIF file to parse
        :type file_path: Union[str, Path]
        :return: The data container with parsed mmCIF data
        :rtype: MMCIFDataContainer
        """
        pass


class BaseWriter(ABC):
    """
    Abstract base class for mmCIF writers.
    
    All writer implementations (MMCIFWriter, GemmiWriter, etc.) must inherit from this
    class and implement the required abstract methods.
    """
    
    @abstractmethod
    def write(self, file_obj: IO, mmcif: MMCIFDataContainer) -> None:
        """
        Write mmCIF data container to a file object.
        
        :param file_obj: The file object to write to
        :type file_obj: IO
        :param mmcif: The data container to write
        :type mmcif: MMCIFDataContainer
        :return: None
        """
        pass
