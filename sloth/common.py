"""
Common utilities and shared dependencies for the sloth package.

This module contains shared functionality that would otherwise create circular imports
between different modules in the package.
"""

from typing import Optional, Dict, Any, Union, IO, List
import os
from .models import MMCIFDataContainer, DataSourceFormat
from .validator import ValidatorFactory


def auto_detect_format_and_load(file_path: str, 
                               validator_factory: Optional[ValidatorFactory] = None,
                               schema_validator: Optional['SchemaValidator'] = None,
                               validate_schema: bool = False) -> MMCIFDataContainer:
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
                from .schemas import SchemaValidatorFactory
                
                # Detect format first
                detected_format = None
                if os.path.isdir(file_path):
                    detected_format = DataSourceFormat.CSV
                else:
                    ext = os.path.splitext(file_path.lower())[1]
                    format_map = {
                        '.json': DataSourceFormat.JSON,
                        '.xml': DataSourceFormat.XML,
                        '.yaml': DataSourceFormat.YAML,
                        '.yml': DataSourceFormat.YAML,
                        '.pkl': DataSourceFormat.PICKLE,
                        '.pickle': DataSourceFormat.PICKLE
                    }
                    detected_format = format_map.get(ext)
                
                if detected_format:
                    format_specific_validator = SchemaValidatorFactory.create_validator(detected_format)
            except (ImportError, ValueError, Exception):
                # If schema validation can't be created, continue without it
                pass
    
    # Import loaders here to avoid circular imports
    from .loaders import MMCIFImporter
    
    # Check if it's a directory (for CSV files)
    if os.path.isdir(file_path):
        return MMCIFImporter.from_csv_files(file_path, validator_factory, format_specific_validator)
    
    ext = os.path.splitext(file_path.lower())[1]
    if ext == '.json':
        return MMCIFImporter.from_json(file_path, validator_factory, format_specific_validator)
    elif ext == '.xml':
        return MMCIFImporter.from_xml(file_path, validator_factory, format_specific_validator)
    elif ext in ('.yaml', '.yml'):
        return MMCIFImporter.from_yaml(file_path, validator_factory, format_specific_validator)
    elif ext in ('.pkl', '.pickle'):
        return MMCIFImporter.from_pickle(file_path, validator_factory, format_specific_validator)
    elif ext == '.csv':
        return MMCIFImporter.from_csv_files(file_path, validator_factory)
    elif ext == '.cif':
        # Import here to avoid circular imports
        from .handler import MMCIFHandler
        handler = MMCIFHandler()
        container = handler.parse(file_path)
        container.source_format = DataSourceFormat.MMCIF
        return container
    raise ValueError(f"Unsupported file extension: {ext}")
