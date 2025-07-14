from typing import Optional, List, Dict, Any, Union
from .parser import MMCIFParser
from .writer import MMCIFWriter
from .exporter import JSONExporter, XMLExporter
from .importer import JSONImporter, XMLImporter
from .models import MMCIFDataContainer, DataSourceFormat
from .defaults import ExportFormat, StructureFormat
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

    def read(
        self, filename: str, categories: Optional[List[str]] = None
    ) -> MMCIFDataContainer:
        """
        Parse an mmCIF file and returns a data container using gemmi's high-performance backend.

        :param filename: The name of the file to parse.
        :type filename: str
        :param categories: The categories to parse. If None, all categories are included.
        :type categories: Optional[List[str]]
        :return: The data container with lazy-loaded items.
        :rtype: MMCIFDataContainer
        """
        self._parser = MMCIFParser(self.validator_factory, categories)
        return self._parser.parse(filename)

    def write(self, mmcif: MMCIFDataContainer, filename: Optional[str] = None) -> None:
        """
        Writes a data container to a file using gemmi's high-performance backend.

        :param mmcif: The data container to write.
        :type mmcif: MMCIFDataContainer
        :param filename: Optional filename to write to. If not provided, uses pre-set file object.
        :type filename: Optional[str]
        :return: None
        """
        self._writer = MMCIFWriter()
        
        if filename:
            # Write to specified filename
            with open(filename, 'w') as file_obj:
                self._writer.write(file_obj, mmcif)
        elif hasattr(self, "_file_obj") and self._file_obj:
            # Write to pre-set file object
            self._writer.write(self._file_obj, mmcif)
        else:
            raise IOError("No filename provided and file is not open for writing")

    def export(
        self,
        mmcif: MMCIFDataContainer,
        format_type: Union[str, ExportFormat] = ExportFormat.JSON,
        file_path: Optional[str] = None,
        permissive: bool = False,
        **kwargs
    ) -> Optional[str]:
        """
        Export mmCIF data to various formats.

        :param mmcif: The data container to export
        :type mmcif: MMCIFDataContainer
        :param format_type: Export format ('json' or 'xml')
        :type format_type: Union[str, ExportFormat]
        :param file_path: Path to save the file (optional)
        :type file_path: Optional[str]
        :param permissive: Whether to skip validation
        :type permissive: bool
        :param kwargs: Additional format-specific options
        :return: String representation if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        # Convert string inputs to enums
        if isinstance(format_type, str):
            format_type = ExportFormat(format_type.lower())
        
        if format_type == ExportFormat.JSON:
            return self._export_json(mmcif, file_path, permissive, **kwargs)
        elif format_type == ExportFormat.XML:
            return self._export_xml(mmcif, file_path, permissive, **kwargs)
        else:
            raise ValueError(f"Unsupported export format: {format_type}")

    def load(
        self,
        file_path: str,
        format_type: Union[str, ExportFormat] = ExportFormat.JSON,
        permissive: bool = False,
        **kwargs
    ) -> MMCIFDataContainer:
        """
        Import mmCIF data from various formats.

        :param file_path: Path to the file to import
        :type file_path: str
        :param format_type: Import format ('json' or 'xml')
        :type format_type: Union[str, ExportFormat]
        :param permissive: Whether to skip validation
        :type permissive: bool
        :param kwargs: Additional format-specific options
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        # Convert string inputs to enums
        if isinstance(format_type, str):
            format_type = ExportFormat(format_type.lower())
        
        if format_type == ExportFormat.JSON:
            return self._import_json(file_path, permissive, **kwargs)
        elif format_type == ExportFormat.XML:
            return self._import_xml(file_path, permissive, **kwargs)
        else:
            raise ValueError(f"Unsupported import format: {format_type}")

    # Private methods for specific format handling
    def _export_json(
        self,
        mmcif: MMCIFDataContainer,
        file_path: Optional[str],
        permissive: bool,
        **kwargs
    ) -> Optional[str]:
        """Export to JSON format (always nested)."""
        exporter = JSONExporter(permissive=permissive)
        indent = kwargs.get('indent', 2)
        return exporter.export_data(mmcif, file_path, permissive, indent)

    def _export_xml(
        self,
        mmcif: MMCIFDataContainer,
        file_path: Optional[str],
        permissive: bool,
        **kwargs
    ) -> Optional[str]:
        """Export to XML format."""
        exporter = XMLExporter(permissive=permissive)
        pretty_print = kwargs.get('pretty_print', True)
        return exporter.export_data(mmcif, file_path, permissive, pretty_print)

    def _import_json(
        self,
        file_path: str,
        permissive: bool,
        **kwargs
    ) -> MMCIFDataContainer:
        """Import from JSON format (assumes nested structure)."""
        importer = JSONImporter(permissive=permissive)
        container = importer.import_data(file_path, permissive)
        container.source_format = DataSourceFormat.JSON
        return container

    def _import_xml(
        self,
        file_path: str,
        permissive: bool,
        **kwargs
    ) -> MMCIFDataContainer:
        """Import from XML format."""
        importer = XMLImporter(permissive=permissive)
        container = importer.import_data(file_path, permissive)
        container.source_format = DataSourceFormat.XML
        return container
