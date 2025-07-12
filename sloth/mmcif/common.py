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
from typing import Optional, Union, IO, Dict, Any
from pathlib import Path
from .models import MMCIFDataContainer, DataSourceFormat
from .plugins import ValidatorFactory
from .validator import SchemaValidator


def auto_detect_format_and_load(
    file_path: str,
    validator_factory: Optional[ValidatorFactory] = None,
    schema_validator: Optional[SchemaValidator] = None,
    permissive_schema: bool = False,
    nested: bool = True,
) -> MMCIFDataContainer:
    """
    Auto-detect the format of the input file and load it using the unified architecture.

    This function supports the formats handled by the new unified importer/exporter system:
    - JSON (nested and flat)
    - XML (PDBML)
    - CIF (mmCIF)

    Args:
        file_path: Path to the file
        validator_factory: Optional validator factory for data validation (deprecated)
        schema_validator: Optional schema validator for format-specific schema validation (deprecated)
        permissive_schema: Whether to skip schema validation
        nested: Whether to expect nested structure (for JSON/XML)

    Returns:
        MMCIFDataContainer object
        
    Raises:
        ValueError: If file format is not supported
    """
    # Import unified importers
    from .importer import JSONImporter, XMLImporter

    ext = os.path.splitext(file_path.lower())[1]
    
    if ext == ".json":
        importer = JSONImporter(permissive=not permissive_schema)
        return importer.import_data(file_path, nested=nested, permissive=permissive_schema)
    elif ext == ".xml":
        importer = XMLImporter(permissive=not permissive_schema)
        return importer.import_data(file_path, nested=nested, permissive=permissive_schema)
    elif ext == ".cif":
        # Import here to avoid circular imports
        from .handler import MMCIFHandler
        handler = MMCIFHandler()
        container = handler.read(file_path)
        container.source_format = DataSourceFormat.MMCIF
        return container
    else:
        # Only support formats handled by the unified architecture
        supported_formats = ['.json', '.xml', '.cif']
        raise ValueError(
            f"Unsupported file extension: {ext}. "
            f"Supported formats: {', '.join(supported_formats)}. "
            f"Note: For additional format support, consider using the export/import system."
        )


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
    def parse(self, file_path: Union[str, Path]) -> MMCIFDataContainer:
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


class BaseImporter(ABC):
    """Abstract base class for all SLOTH importers."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        """
        Initialize the importer.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            xsd_path: Path to PDBML XSD schema file
            cache_dir: Directory for caching
            permissive: If False, performs validation
            quiet: Suppress output messages
        """
        self.permissive = permissive
        self.quiet = quiet
        
        # Set default schema paths
        if dict_path is None:
            dict_path = Path(__file__).parent / "schemas" / "mmcif_pdbx_v50.dic"
        if xsd_path is None:
            xsd_path = Path(__file__).parent / "schemas" / "pdbx-v50.xsd"
            
        self.dict_path = dict_path
        self.xsd_path = xsd_path
        self.cache_dir = cache_dir
        
        # Set up shared PDBML components
        self._setup_pdbml_components()
    
    def _setup_pdbml_components(self):
        """Set up shared PDBML conversion and validation components."""
        if not self.permissive:
            from .serializer import (
                PDBMLConverter, DictionaryParser, XSDParser, 
                MappingGenerator, get_cache_manager
            )
            from .validator import XMLSchemaValidator
            
            cache_manager = get_cache_manager(
                self.cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
            )
            
            # Set up metadata parsers
            dict_parser = DictionaryParser(cache_manager, self.quiet)
            xsd_parser = XSDParser(cache_manager, self.quiet)
            dict_parser.source = self.dict_path
            xsd_parser.source = self.xsd_path
            
            # Set up components for validation
            mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache_manager, self.quiet)
            self.converter = PDBMLConverter(mapping_generator, permissive=False, quiet=self.quiet)
            self.validator = XMLSchemaValidator(self.xsd_path)
        else:
            self.converter = None
            self.validator = None
    
    def _validate_content_via_pdbml(self, container: "MMCIFDataContainer") -> None:
        """Validate container content by converting to PDBML and checking against XSD."""
        try:
            if not self.quiet:
                print("🔍 Starting PDBML XSD content validation...")
            
            # Convert to PDBML XML
            pdbml_xml = self.converter.convert_to_pdbml(container)
            
            # Validate against XSD
            validation_result = self.validator.validate(pdbml_xml)
            
            if isinstance(validation_result, dict):
                if not validation_result.get('valid', False):
                    errors = validation_result.get('errors', [])
                    error_msg = '; '.join(errors) if errors else 'Unknown validation error'
                    from .validator import ValidationError
                    raise ValidationError(f"PDBML XSD content validation failed: {error_msg}")
                else:
                    if not self.quiet:
                        print("✅ PDBML XSD content validation passed")
            
        except Exception as e:
            from .validator import ValidationError
            if isinstance(e, ValidationError):
                raise
            else:
                raise ValidationError(f"Content validation process failed: {str(e)}")
    
    @abstractmethod
    def import_data(
        self, 
        data: Union[str, Dict[str, Any], Path], 
        nested: bool = True,
        permissive: bool = None
    ) -> "MMCIFDataContainer":
        """
        Import data back to mmCIF format.
        
        Args:
            data: Data to import (string, dict, or file path)
            nested: Whether to expect nested structure
            permissive: Override permissive mode setting for schema validation
            
        Returns:
            MMCIFDataContainer with imported data
        """
        pass


class BaseExporter(ABC):
    """Abstract base class for all SLOTH exporters."""
    
    def __init__(
        self,
        dict_path: Optional[Union[str, Path]] = None,
        xsd_path: Optional[Union[str, Path]] = None,
        cache_dir: Optional[str] = None,
        permissive: bool = False,
        quiet: bool = False
    ):
        """
        Initialize the exporter.
        
        Args:
            dict_path: Path to mmCIF dictionary file
            xsd_path: Path to PDBML XSD schema file  
            cache_dir: Directory for caching
            permissive: If False, validates during export
            quiet: Suppress output messages
        """
        self.permissive = permissive
        self.quiet = quiet
        
        # Set default schema paths
        if dict_path is None:
            dict_path = Path(__file__).parent / "schemas" / "mmcif_pdbx_v50.dic"
        if xsd_path is None:
            xsd_path = Path(__file__).parent / "schemas" / "pdbx-v50.xsd"
            
        self.dict_path = dict_path
        self.xsd_path = xsd_path
        self.cache_dir = cache_dir
        
        # Set up shared PDBML components
        self._setup_pdbml_components()
    
    def _setup_pdbml_components(self):
        """Set up shared PDBML conversion and validation components."""
        from .serializer import (
            PDBMLConverter, DictionaryParser, XSDParser, 
            MappingGenerator, get_cache_manager
        )
        from .validator import XMLSchemaValidator
        
        cache_manager = get_cache_manager(
            self.cache_dir or os.path.join(os.path.expanduser("~"), ".sloth_cache")
        )
        
        # Set up metadata parsers
        dict_parser = DictionaryParser(cache_manager, self.quiet)
        xsd_parser = XSDParser(cache_manager, self.quiet)
        dict_parser.source = self.dict_path
        xsd_parser.source = self.xsd_path
        
        # Set up mapping generator and components
        mapping_generator = MappingGenerator(dict_parser, xsd_parser, cache_manager, self.quiet)
        self.converter = PDBMLConverter(mapping_generator, self.permissive, self.quiet)
        
        # Set up validator if not permissive
        if not self.permissive:
            self.validator = XMLSchemaValidator(self.xsd_path)
        else:
            self.validator = None
    
    def _convert_to_pdbml(self, mmcif_data: "MMCIFDataContainer") -> str:
        """Convert mmCIF data to PDBML XML format."""
        return self.converter.convert_to_pdbml(mmcif_data)
    
    def _validate_pdbml(self, pdbml_xml: str) -> None:
        """Validate PDBML XML against XSD schema."""
        if self.validator:
            validation_result = self.validator.validate(pdbml_xml)
            if not validation_result.get("valid", False):
                errors = validation_result.get("errors", [])
                error_msg = f"PDBML validation failed: {'; '.join(errors)}"
                from .validator import ValidationError
                if not self.permissive:
                    raise ValidationError(error_msg)
                elif not self.quiet:
                    print(f"Warning: {error_msg}")
    
    @abstractmethod
    def export_data(
        self, 
        mmcif_data: "MMCIFDataContainer",
        file_path: Optional[Union[str, Path]] = None,
        nested: bool = True,
        permissive: bool = None,
        **kwargs
    ) -> Optional[str]:
        """
        Export mmCIF data to target format.
        
        Args:
            mmcif_data: The mmCIF data container to export
            file_path: Path to save the file (optional)
            nested: Whether to use nested structure
            permissive: Override permissive mode setting for schema validation
            **kwargs: Additional format-specific options
            
        Returns:
            String representation if no file_path provided, otherwise None
        """
        pass
