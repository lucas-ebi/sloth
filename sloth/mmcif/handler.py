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

    def export(
        self,
        mmcif: MMCIFDataContainer,
        format_type: Union[str, ExportFormat] = ExportFormat.JSON,
        structure: Union[str, StructureFormat] = StructureFormat.NESTED,
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
        :param structure: Structure type ('nested' or 'flat')
        :type structure: Union[str, StructureFormat]
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
        if isinstance(structure, str):
            structure = StructureFormat(structure.lower())
        
        if format_type == ExportFormat.JSON:
            return self._export_json(mmcif, file_path, structure, permissive, **kwargs)
        elif format_type == ExportFormat.XML:
            return self._export_xml(mmcif, file_path, structure, permissive, **kwargs)
        else:
            raise ValueError(f"Unsupported export format: {format_type}")

    def import_data(
        self,
        file_path: str,
        format_type: Union[str, ExportFormat] = ExportFormat.JSON,
        structure: Union[str, StructureFormat] = StructureFormat.NESTED,
        permissive: bool = False,
        **kwargs
    ) -> MMCIFDataContainer:
        """
        Import mmCIF data from various formats.

        :param file_path: Path to the file to import
        :type file_path: str
        :param format_type: Import format ('json' or 'xml')
        :type format_type: Union[str, ExportFormat]
        :param structure: Structure type ('nested' or 'flat')
        :type structure: Union[str, StructureFormat]
        :param permissive: Whether to skip validation
        :type permissive: bool
        :param kwargs: Additional format-specific options
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        # Convert string inputs to enums
        if isinstance(format_type, str):
            format_type = ExportFormat(format_type.lower())
        if isinstance(structure, str):
            structure = StructureFormat(structure.lower())
        
        if format_type == ExportFormat.JSON:
            return self._import_json(file_path, structure, permissive, **kwargs)
        elif format_type == ExportFormat.XML:
            return self._import_xml(file_path, structure, permissive, **kwargs)
        else:
            raise ValueError(f"Unsupported import format: {format_type}")

    # Private methods for specific format handling
    def _export_json(
        self,
        mmcif: MMCIFDataContainer,
        file_path: Optional[str],
        structure: StructureFormat,
        permissive: bool,
        **kwargs
    ) -> Optional[str]:
        """Export to JSON format."""
        exporter = JSONExporter(permissive=permissive)
        nested = (structure == StructureFormat.NESTED)
        indent = kwargs.get('indent', 2)
        return exporter.export_data(mmcif, file_path, nested, permissive, indent)

    def _export_xml(
        self,
        mmcif: MMCIFDataContainer,
        file_path: Optional[str],
        structure: StructureFormat,
        permissive: bool,
        **kwargs
    ) -> Optional[str]:
        """Export to XML format."""
        exporter = XMLExporter(permissive=permissive)
        nested = (structure == StructureFormat.NESTED)  # XML is inherently structured
        pretty_print = kwargs.get('pretty_print', True)
        return exporter.export_data(mmcif, file_path, nested, permissive, pretty_print)

    def _import_json(
        self,
        file_path: str,
        structure: StructureFormat,
        permissive: bool,
        **kwargs
    ) -> MMCIFDataContainer:
        """Import from JSON format."""
        importer = JSONImporter(permissive=permissive)
        nested = (structure == StructureFormat.NESTED)
        container = importer.import_data(file_path, nested, permissive)
        container.source_format = DataSourceFormat.JSON
        return container

    def _import_xml(
        self,
        file_path: str,
        structure: StructureFormat,
        permissive: bool,
        **kwargs
    ) -> MMCIFDataContainer:
        """Import from XML format."""
        importer = XMLImporter(permissive=permissive)
        nested = (structure == StructureFormat.NESTED)  # XML structure is fixed
        container = importer.import_data(file_path, nested, permissive)
        container.source_format = DataSourceFormat.XML
        return container

