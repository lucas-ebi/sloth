from typing import Optional, List, Dict, Any
from .parser import MMCIFParser
from .writer import MMCIFWriter  
from .exporter import MMCIFExporter
from .loaders import MMCIFImporter
from .models import MMCIFDataContainer, DataSourceFormat
from .validator import ValidatorFactory

class MMCIFHandler:
    """A class to handle reading and writing mmCIF files with efficient memory mapping and lazy loading."""
    
    def __init__(self, validator_factory: Optional[ValidatorFactory] = None):
        """
        Initialize the handler with memory mapping and lazy loading always enabled.
        
        :param validator_factory: Optional validator factory for data validation
        """
        self.validator_factory = validator_factory
        self._parser = None
        self._writer = None
        self._file_obj = None

    def parse(self, filename: str, categories: Optional[List[str]] = None) -> MMCIFDataContainer:
        """
        Parses an mmCIF file and returns a data container using memory mapping and lazy loading.

        :param filename: The name of the file to parse.
        :type filename: str
        :param categories: The categories to parse. If None, all categories are included.
        :type categories: Optional[List[str]]
        :return: The data container with lazy-loaded items.
        :rtype: MMCIFDataContainer
        """
        self._parser = MMCIFParser(self.validator_factory, categories)
        return self._parser.parse_file(filename)

    def write(self, mmcif_data_container: MMCIFDataContainer) -> None:
        """
        Writes a data container to a file.

        :param mmcif_data_container: The data container to write.
        :type mmcif_data_container: MMCIFDataContainer
        :return: None
        """
        if hasattr(self, '_file_obj') and self._file_obj:
            self._writer = MMCIFWriter()
            self._writer.write(self._file_obj, mmcif_data_container)
        else:
            raise IOError("File is not open for writing")
            
    def export_to_json(self, mmcif_data_container: MMCIFDataContainer, file_path: Optional[str] = None, 
                       indent: int = 2) -> Optional[str]:
        """
        Export mmCIF data to JSON format.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :param file_path: Path to save the JSON file (optional)
        :type file_path: Optional[str]
        :param indent: Number of spaces for indentation
        :type indent: int
        :return: JSON string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        exporter = MMCIFExporter(mmcif_data_container)
        return exporter.to_json(file_path, indent)
        
    def export_to_xml(self, mmcif_data_container: MMCIFDataContainer, file_path: Optional[str] = None, 
                     pretty_print: bool = True) -> Optional[str]:
        """
        Export mmCIF data to XML format.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :param file_path: Path to save the XML file (optional)
        :type file_path: Optional[str]
        :param pretty_print: Whether to format XML with indentation
        :type pretty_print: bool
        :return: XML string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        exporter = MMCIFExporter(mmcif_data_container)
        return exporter.to_xml(file_path, pretty_print)
        
    def export_to_pickle(self, mmcif_data_container: MMCIFDataContainer, file_path: str) -> None:
        """
        Export mmCIF data to a Python pickle file.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :param file_path: Path to save the pickle file
        :type file_path: str
        :return: None
        """
        exporter = MMCIFExporter(mmcif_data_container)
        exporter.to_pickle(file_path)
        
    def export_to_yaml(self, mmcif_data_container: MMCIFDataContainer, file_path: Optional[str] = None) -> Optional[str]:
        """
        Export mmCIF data to YAML format.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :param file_path: Path to save the YAML file (optional)
        :type file_path: Optional[str]
        :return: YAML string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        exporter = MMCIFExporter(mmcif_data_container)
        return exporter.to_yaml(file_path)
        
    def export_to_pandas(self, mmcif_data_container: MMCIFDataContainer) -> Dict[str, Dict[str, Any]]:
        """
        Export mmCIF data to pandas DataFrames, with one DataFrame per category.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :return: Dictionary of DataFrames organized by data block and category
        :rtype: Dict[str, Dict[str, Any]]
        """
        exporter = MMCIFExporter(mmcif_data_container)
        return exporter.to_pandas()
        
    def export_to_csv(self, mmcif_data_container: MMCIFDataContainer, directory_path: str, prefix: str = "") -> Dict[str, Dict[str, str]]:
        """
        Export mmCIF data to CSV files, with one file per category.
        
        :param mmcif_data_container: The data container to export
        :type mmcif_data_container: MMCIFDataContainer
        :param directory_path: Directory to save the CSV files
        :type directory_path: str
        :param prefix: Prefix for CSV filenames
        :type prefix: str
        :return: Dictionary mapping block and category names to file paths
        :rtype: Dict[str, Dict[str, str]]
        """
        exporter = MMCIFExporter(mmcif_data_container)
        return exporter.to_csv(directory_path, prefix)
        
    def import_from_json(self, file_path: str, schema_validator=None) -> MMCIFDataContainer:
        """
        Import mmCIF data from a JSON file.
        
        :param file_path: Path to the JSON file
        :type file_path: str
        :param schema_validator: Optional schema validator for data validation
        :type schema_validator: SchemaValidator
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        container = MMCIFImporter.from_json(file_path, self.validator_factory, schema_validator)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.JSON
        return container
    
    def import_from_xml(self, file_path: str, schema_validator=None) -> MMCIFDataContainer:
        """
        Import mmCIF data from an XML file.
        
        :param file_path: Path to the XML file
        :type file_path: str
        :param schema_validator: Optional schema validator for data validation
        :type schema_validator: SchemaValidator
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        container = MMCIFImporter.from_xml(file_path, self.validator_factory, schema_validator)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.XML
        return container
    
    def import_from_pickle(self, file_path: str, schema_validator=None) -> MMCIFDataContainer:
        """
        Import mmCIF data from a pickle file.
        
        :param file_path: Path to the pickle file
        :type file_path: str
        :param schema_validator: Optional schema validator for data validation
        :type schema_validator: SchemaValidator
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        container = MMCIFImporter.from_pickle(file_path, self.validator_factory, schema_validator)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.PICKLE
        return container
    
    def import_from_yaml(self, file_path: str, schema_validator=None) -> MMCIFDataContainer:
        """
        Import mmCIF data from a YAML file.
        
        :param file_path: Path to the YAML file
        :type file_path: str
        :param schema_validator: Optional schema validator for data validation
        :type schema_validator: SchemaValidator
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        container = MMCIFImporter.from_yaml(file_path, self.validator_factory, schema_validator)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.YAML
        return container
    
    def import_from_csv_files(self, directory_path: str, schema_validator=None) -> MMCIFDataContainer:
        """
        Import mmCIF data from CSV files in a directory.
        
        :param directory_path: Directory containing CSV files
        :type directory_path: str
        :param schema_validator: Optional schema validator for data validation
        :type schema_validator: SchemaValidator
        :return: An MMCIFDataContainer instance
        :rtype: MMCIFDataContainer
        """
        container = MMCIFImporter.from_csv_files(directory_path, self.validator_factory, schema_validator)
        # Make sure source format is set correctly
        container.source_format = DataSourceFormat.CSV
        return container
    
    def import_auto_detect(self, file_path: str, validate_schema=False) -> MMCIFDataContainer:
        """
        Auto-detect file format and import mmCIF data.
        
        :param file_path: Path to the file to import
        :type file_path: str
        :param validate_schema: Whether to validate against schema
        :type validate_schema: bool
        :return: An MMCIFDataContainer instance with appropriate source_format flag set
        :rtype: MMCIFDataContainer
        """
        return MMCIFImporter.auto_detect_format(file_path, self.validator_factory, 
                                              validate_schema=validate_schema)

    @property
    def file_obj(self):
        """Provides access to the file object."""
        return self._file_obj

    @file_obj.setter
    def file_obj(self, file_obj):
        """Sets the file object."""
        self._file_obj = file_obj
