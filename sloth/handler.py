from typing import Optional, List, Dict, Any
from .parser import MMCIFParser
from .writer import MMCIFWriter
from .exporter import JSONExporter
from .importer import JSONImporter
from .models import MMCIFDataContainer, DataSourceFormat
from .plugins import ValidatorFactory


class MMCIFHandler:
    """A class to handle reading and writing mmCIF files with high-performance gemmi backend."""

    def __init__(self, validator_factory: Optional[ValidatorFactory] = None):
        """
        Initialize the handler with gemmi backend for optimal performance.

        :param validator_factory: Optional validator factory for data validation
        """
        self.validator_factory = validator_factory
        self._parser = None
        self._writer = None
        self._file_obj = None

    def parse(
        self, filename: str, categories: Optional[List[str]] = None
    ) -> MMCIFDataContainer:
        """
        Parses an mmCIF file and returns a data container using gemmi's high-performance backend.

        :param filename: The name of the file to parse.
        :type filename: str
        :param categories: The categories to parse. If None, all categories are included.
        :type categories: Optional[List[str]]
        :return: The data container with lazy-loaded items.
        :rtype: MMCIFDataContainer
        """
        self._parser = MMCIFParser(self.validator_factory, categories)
        return self._parser.parse_file(filename)

    def write(self, mmcif: MMCIFDataContainer) -> None:
        """
        Writes a data container to a file using gemmi's high-performance backend.

        :param mmcif: The data container to write.
        :type mmcif: MMCIFDataContainer
        :return: None
        """
        if hasattr(self, "_file_obj") and self._file_obj:
            self._writer = MMCIFWriter()
            self._writer.write(self._file_obj, mmcif)
        else:
            raise IOError("File is not open for writing")

    def export_to_json(
        self,
        mmcif: MMCIFDataContainer,
        file_path: Optional[str] = None,
        indent: int = 2,
        permissive: bool = False,
    ) -> Optional[str]:
        """
        Export mmCIF data to nested JSON format.

        :param mmcif: The data container to export
        :type mmcif: MMCIFDataContainer
        :param file_path: Path to save the JSON file (optional)
        :type file_path: Optional[str]
        :param indent: Number of spaces for indentation
        :type indent: int
        :param permissive: Whether to skip validation
        :type permissive: bool
        :return: JSON string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        exporter = JSONExporter(permissive=permissive)
        return exporter.to_json(mmcif, file_path, indent)

    # Note: Legacy export methods (XML, YAML, pickle, CSV, pandas) have been moved to sloth.legacy
    # For multiple format support, use: from sloth.legacy import MMCIFHandler

    def import_from_json(
        self, 
        file_path: str, 
        permissive: bool = False,
        validate: bool = None
    ) -> MMCIFDataContainer:
        """
        Import mmCIF data from a nested JSON file.

        :param file_path: Path to the JSON file
        :type file_path: str
        :param permissive: Whether to skip validation
        :type permissive: bool
        :param validate: Override validation setting
        :type validate: bool
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        importer = JSONImporter(permissive=permissive)
        container = importer.import_from_json(file_path, validate=validate)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.JSON
        return container

    # Note: Legacy import methods (XML, YAML, pickle, CSV) have been moved to sloth.legacy
    # For multiple format support, use: from sloth.legacy import MMCIFHandler

    def import_auto_detect(
        self, file_path: str, validate_schema=False
    ) -> MMCIFDataContainer:
        """
        Auto-detect file format and import mmCIF data.
        Currently only supports JSON format.

        :param file_path: Path to the file to import
        :type file_path: str
        :param validate_schema: Whether to validate against schema
        :type validate_schema: bool
        :return: An MMCIFDataContainer instance with appropriate source_format flag set
        :rtype: MMCIFDataContainer
        """
        # For now, assume JSON format
        return self.import_from_json(file_path, permissive=not validate_schema)

    @property
    def file_obj(self):
        """Provides access to the file object."""
        return self._file_obj

    @file_obj.setter
    def file_obj(self, file_obj):
        """Sets the file object."""
        self._file_obj = file_obj
