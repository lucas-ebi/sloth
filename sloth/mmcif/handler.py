from typing import Optional, List, Dict, Any, Union
from .parser import MMCIFParser
from .writer import MMCIFWriter
from .exporter import JSONExporter
from .importer import JSONImporter
from .models import MMCIFDataContainer, DataSourceFormat
from .plugins import PluginFactory


class MMCIFHandler:
    """A class to handle reading and writing mmCIF files with high-performance gemmi backend."""

    def __init__(
        self,
        plugin_factory: Optional[PluginFactory] = None,
    ):
        """
        Initialize the handler with gemmi backend for optimal performance.

        :param plugin_factory: Optional plugin factory for dot-notation extensions
        """
        self.plugin_factory = plugin_factory
        self._parser = None
        self._writer = None
        self._file_obj = None

    def read(
        self, 
        filename: str, 
        categories: Optional[List[str]] = None
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
        self._parser = MMCIFParser(self.plugin_factory, categories)
        container = self._parser.parse(filename)
        
        return container

    def write(
        self, 
        mmcif: MMCIFDataContainer, 
        filename: Optional[str] = None
    ) -> None:
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
        file_path: Optional[str] = None,
        **kwargs
    ) -> Optional[str]:
        """
        Export mmCIF data to JSON format.

        :param mmcif: The data container to export
        :type mmcif: MMCIFDataContainer
        :param file_path: Path to save the file (optional)
        :type file_path: Optional[str]
        :param kwargs: Additional options (e.g., indent, quiet)
        :return: String representation if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        return self._export_json(mmcif, file_path, **kwargs)

    def load(
        self,
        file_path: str,
        **kwargs
    ) -> MMCIFDataContainer:
        """
        Import mmCIF data from JSON format.

        :param file_path: Path to the JSON file to import
        :type file_path: str
        :param kwargs: Additional options
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        return self._import_json(file_path, **kwargs)

    # Private methods for specific format handling
    def _export_json(
        self,
        mmcif: MMCIFDataContainer,
        file_path: Optional[str],
        **kwargs
    ) -> Optional[str]:
        """Export to JSON format (always nested)."""
        denormalize = kwargs.get('denormalize', False)
        exporter = JSONExporter(quiet=kwargs.get('quiet', False), denormalize=denormalize)
        indent = kwargs.get('indent', None)
        return exporter.export_data(mmcif, file_path, indent)

    def _import_json(
        self,
        file_path: str,
        **kwargs
    ) -> MMCIFDataContainer:
        """Import from JSON format (assumes nested structure)."""
        importer = JSONImporter()
        container = importer.import_data(file_path)
        container.source_format = DataSourceFormat.JSON
        return container
