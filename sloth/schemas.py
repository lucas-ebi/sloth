"""
SLOTH Schema Validation System

This module provides schema definitions and validation logic for different data formats
supported by SLOTH. It enables validation of imported data before conversion to mmCIF format.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union, Optional, Type, TypeVar
from enum import Enum, auto
import os
import re
import json
from .main import DataSourceFormat


# Type definitions for schema structures
SchemaDict = Dict[str, Any]
ValidationResult = Dict[str, Any]
T = TypeVar('T')


class ValidationSeverity(Enum):
    """Severity levels for schema validation errors."""
    ERROR = auto()      # Validation failures that should prevent import
    WARNING = auto()    # Issues that should be flagged but don't prevent import
    INFO = auto()       # Informational notices


class ValidationError(Exception):
    """Exception raised for schema validation errors."""
    
    def __init__(self, message: str, path: str = "", severity: ValidationSeverity = ValidationSeverity.ERROR):
        """
        Initialize validation error.
        
        Args:
            message: Error message
            path: JSON path where the error occurred
            severity: Validation error severity
        """
        self.message = message
        self.path = path
        self.severity = severity
        super().__init__(f"{path}: {message}")


class SchemaValidator(ABC):
    """Base abstract class for all schema validators."""
    
    @abstractmethod
    def validate(self, data: Any) -> ValidationResult:
        """
        Validate data against the schema.
        
        Args:
            data: The data to validate
            
        Returns:
            ValidationResult: Dictionary with validation results
            
        Raises:
            ValidationError: If validation fails with ERROR severity
        """
        pass
    
    @abstractmethod
    def is_valid(self, data: Any) -> bool:
        """
        Check if data is valid according to the schema.
        
        Args:
            data: The data to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        pass


class BaseSchema:
    """Base class for all schema definitions."""
    
    def __init__(self, 
                 description: str = "", 
                 required: bool = True):
        """
        Initialize schema with common properties.
        
        Args:
            description: Human-readable description of what the schema validates
            required: Whether this field is required
        """
        self.description = description
        self.required = required
        
    def validate_type(self, value: Any, expected_type: Type[T]) -> T:
        """
        Validate that a value is of the expected type.
        
        Args:
            value: Value to validate
            expected_type: Expected type
            
        Returns:
            The validated value
            
        Raises:
            ValidationError: If type validation fails
        """
        if not isinstance(value, expected_type):
            raise ValidationError(
                f"Expected type {expected_type.__name__}, got {type(value).__name__}")
        return value


class JSONSchemaValidator(SchemaValidator):
    """Schema validator for JSON data using JSON Schema standard."""
    
    def __init__(self, schema: Dict[str, Any]):
        """
        Initialize with a JSON schema.
        
        Args:
            schema: JSON schema definition
        """
        self.schema = schema
        
        # Try to import jsonschema library
        try:
            import jsonschema
            self._jsonschema = jsonschema
            
            # Add Draft7Validator for more precise validation control
            from jsonschema import Draft7Validator
            self._validator = Draft7Validator(schema)
        except ImportError:
            raise ImportError(
                "jsonschema library is required for JSON schema validation. "
                "Install it using 'pip install jsonschema'."
            )
    
    def validate(self, data: Any) -> ValidationResult:
        """
        Validate data against JSON schema.
        
        Args:
            data: Data to validate
            
        Returns:
            ValidationResult containing validation outcome
            
        Raises:
            ValidationError: If validation fails
        """
        if not data:
            raise ValidationError("Data cannot be empty")
            
        # Using the validator directly gives us more control
        errors = list(self._validator.iter_errors(data))
        
        if errors:
            # Use the first error to raise the exception
            error = errors[0]
            path = "/".join(str(p) for p in error.path) if error.path else ""
            error_message = error.message
            raise ValidationError(error_message, path)
            
        return {"valid": True, "errors": []}
    
    def is_valid(self, data: Any) -> bool:
        """
        Check if data is valid according to the schema.
        
        Args:
            data: Data to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not data:
            return False
            
        return self._validator.is_valid(data)


class XMLSchemaValidator(SchemaValidator):
    """Schema validator for XML data."""
    
    def __init__(self, xsd_schema: str):
        """
        Initialize with an XSD schema.
        
        Args:
            xsd_schema: XSD schema as string or file path
        """
        self.xsd_schema = xsd_schema
        
        # Try to import lxml library
        try:
            from lxml import etree
            self._etree = etree
        except ImportError:
            raise ImportError(
                "lxml library is required for XML schema validation. "
                "Install it using 'pip install lxml'."
            )
            
        # Parse schema during initialization
        try:
            if isinstance(self.xsd_schema, str) and os.path.exists(self.xsd_schema):
                # It's a file path - let lxml handle encoding
                with open(self.xsd_schema, 'rb') as f:
                    schema_bytes = f.read()
                self.schema_doc = self._etree.fromstring(schema_bytes)
            elif isinstance(self.xsd_schema, str):
                # It's a string containing XML - encode to bytes
                self.schema_doc = self._etree.fromstring(self.xsd_schema.encode('utf-8'))
            else:
                # It's already an ElementTree or Element
                self.schema_doc = self.xsd_schema
                
            self.schema = self._etree.XMLSchema(self.schema_doc)
        except Exception as e:
            # If schema parsing fails during init, store the error
            self.schema = None
            self.schema_error = str(e)
    
    def validate(self, data: Any) -> ValidationResult:
        """
        Validate XML data against XSD schema.
        
        Args:
            data: XML data to validate (string, bytes, ElementTree, or Element)
            
        Returns:
            ValidationResult containing validation outcome
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            # Check if schema is already parsed
            if self.schema is None:
                raise ValidationError(f"XML schema is invalid: {getattr(self, 'schema_error', 'Unknown error')}")
            
            # Parse XML data
            if isinstance(data, str):
                # String input - encode to bytes first
                xml_doc = self._etree.fromstring(data.encode('utf-8'))
            elif isinstance(data, bytes):
                # Already bytes - parse directly
                xml_doc = self._etree.fromstring(data)
            elif isinstance(data, (self._etree._Element, self._etree._ElementTree)):
                # Already parsed - use as is
                xml_doc = data
            else:
                raise ValidationError(f"Unsupported XML data type: {type(data)}")
                
            # Validate
            self.schema.assertValid(xml_doc)
            return {"valid": True, "errors": []}
        except self._etree.XMLSyntaxError as e:
            raise ValidationError(f"XML syntax error: {str(e)}")
        except self._etree.DocumentInvalid as e:
            raise ValidationError(f"XML validation error: {str(e)}")
    
    def is_valid(self, data: Any) -> bool:
        """
        Check if XML data is valid.
        
        Args:
            data: XML data to validate (string, bytes, ElementTree, or Element)
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            if self.schema is None:
                return False
                
            # Parse XML data
            if isinstance(data, str):
                # String input - encode to bytes first
                xml_doc = self._etree.fromstring(data.encode('utf-8'))
            elif isinstance(data, bytes):
                # Already bytes - parse directly
                xml_doc = self._etree.fromstring(data)
            elif isinstance(data, (self._etree._Element, self._etree._ElementTree)):
                # Already parsed - use as is
                xml_doc = data
            else:
                return False
                
            # Validate using the already parsed schema
            return self.schema.validate(xml_doc)
        except Exception:
            return False


class YAMLSchemaValidator(SchemaValidator):
    """Schema validator for YAML data using JSON Schema standard internally."""
    
    def __init__(self, schema: Dict[str, Any]):
        """
        Initialize with a schema (JSON Schema format).
        
        Args:
            schema: Schema definition
        """
        self.schema = schema
        
        # Try to import required libraries
        try:
            import yaml
            self._yaml = yaml
        except ImportError:
            raise ImportError("PyYAML library is required. Install it using 'pip install pyyaml'.")
        
        try:
            import jsonschema
            self._jsonschema = jsonschema
        except ImportError:
            raise ImportError("jsonschema library is required. Install it using 'pip install jsonschema'.")
    
    def validate(self, data: Any) -> ValidationResult:
        """
        Validate YAML data.
        
        Args:
            data: YAML data as string or parsed dict
            
        Returns:
            ValidationResult containing validation outcome
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            # Parse YAML if it's a string
            parsed_data = data
            if isinstance(data, str):
                parsed_data = self._yaml.safe_load(data)
                
            # Validate using JSON Schema
            self._jsonschema.validate(instance=parsed_data, schema=self.schema)
            return {"valid": True, "errors": []}
        except self._yaml.YAMLError as e:
            raise ValidationError(f"YAML syntax error: {str(e)}")
        except self._jsonschema.exceptions.ValidationError as e:
            path = "/".join(str(p) for p in e.path)
            raise ValidationError(e.message, path)
    
    def is_valid(self, data: Any) -> bool:
        """
        Check if YAML data is valid.
        
        Args:
            data: YAML data to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            # Parse YAML if it's a string
            parsed_data = data
            if isinstance(data, str):
                parsed_data = self._yaml.safe_load(data)
                
            # Validate using JSON Schema
            self._jsonschema.validate(instance=parsed_data, schema=self.schema)
            return True
        except Exception:
            return False


class CSVSchemaValidator(SchemaValidator):
    """Schema validator for CSV files."""
    
    def __init__(self, 
                 expected_columns: Dict[str, Dict[str, Any]],
                 filename_pattern: Optional[str] = None):
        """
        Initialize with expected columns and optional filename pattern.
        
        Args:
            expected_columns: Dictionary mapping column names to their specifications
            filename_pattern: Optional regex pattern for validating file names
        """
        self.expected_columns = expected_columns
        self.filename_pattern = filename_pattern
    
    def validate(self, data: Any) -> ValidationResult:
        """
        Validate CSV data.
        
        Args:
            data: Either pandas DataFrame or dict with 'file' and 'data' keys
            
        Returns:
            ValidationResult containing validation outcome
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas library is required. Install it using 'pip install pandas'.")
            
        # Extract DataFrame and filename
        df = None
        filename = None
        
        if isinstance(data, dict) and 'file' in data and 'data' in data:
            filename = data['file']
            df = data['data']
        elif isinstance(data, pd.DataFrame):
            df = data
            
        errors = []
        
        # Validate filename if pattern provided
        if filename and self.filename_pattern:
            if not re.match(self.filename_pattern, filename):
                errors.append(
                    f"Filename '{filename}' does not match expected pattern '{self.filename_pattern}'")
        
        # Validate columns
        if df is not None:
            # Check required columns
            for col_name, specs in self.expected_columns.items():
                if specs.get('required', True) and col_name not in df.columns:
                    errors.append(f"Required column '{col_name}' is missing")
            
            # Check column types
            for col_name in df.columns:
                if col_name in self.expected_columns:
                    col_type = self.expected_columns[col_name].get('type')
                    if col_type:
                        try:
                            df[col_name].astype(col_type)
                        except Exception as e:
                            errors.append(f"Column '{col_name}' has invalid type: {str(e)}")
        
        if errors:
            raise ValidationError("\n".join(errors))
        
        return {"valid": True, "errors": []}
    
    def is_valid(self, data: Any) -> bool:
        """
        Check if CSV data is valid.
        
        Args:
            data: CSV data to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            self.validate(data)
            return True
        except Exception:
            return False


# Load default schemas from files
import os
import json

def _get_schema_dir():
    """Get the absolute path to the schema directory."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schemas')

def _load_schema_file(filename):
    """
    Load schema from file.
    
    Args:
        filename: Name of the schema file in the schemas directory
        
    Returns:
        Dict containing the parsed JSON schema, or None if file not found
    """
    schema_path = os.path.join(_get_schema_dir(), filename)
    if os.path.exists(schema_path):
        with open(schema_path, 'r') as f:
            return json.load(f)
    return None

def _load_text_file(filename):
    """
    Load text file from schemas directory.
    
    Args:
        filename: Name of the file in the schemas directory
        
    Returns:
        String containing the file contents, or None if file not found
    """
    schema_path = os.path.join(_get_schema_dir(), filename)
    if os.path.exists(schema_path):
        with open(schema_path, 'r') as f:
            return f.read()
    return None

# Default schemas for mmCIF data formats
default_mmcif_json_schema = _load_schema_file('mmcif_json_schema.json')


# Schema validator factory class
class SchemaValidatorFactory:
    """Factory class for creating schema validators based on format."""
    
    @staticmethod
    def create_validator(format_type: DataSourceFormat, 
                         custom_schema: Optional[Any] = None) -> SchemaValidator:
        """
        Create a schema validator for the specified format.
        
        Args:
            format_type: The data format type
            custom_schema: Optional custom schema definition
            
        Returns:
            SchemaValidator: Appropriate validator for the format
            
        Raises:
            ValueError: If format is not supported
        """
        if format_type == DataSourceFormat.JSON:
            schema = custom_schema if custom_schema else default_mmcif_json_schema
            return JSONSchemaValidator(schema)
            
        elif format_type == DataSourceFormat.XML:
            if not custom_schema:
                # Load the XSD schema from file
                xsd_schema_content = _load_text_file('mmcif_xml_schema.xsd')
                if not xsd_schema_content:
                    schema_path = os.path.join(_get_schema_dir(), 'mmcif_xml_schema.xsd')
                    raise ValueError(f"Required XML schema file not found: {schema_path}")

                try:
                    from lxml import etree
                    # Parse the schema from file
                    # Pass the XSD content to the validator, not the parsed schema
                    custom_schema = xsd_schema_content
                except Exception as e:
                    raise ValueError(f"Failed to parse XML schema file: {e}")
                    
            return XMLSchemaValidator(custom_schema)
            
        elif format_type == DataSourceFormat.YAML:
            schema = custom_schema if custom_schema else default_mmcif_json_schema
            return YAMLSchemaValidator(schema)
            
        elif format_type == DataSourceFormat.CSV:
            if not custom_schema:
                # Default is no schema
                custom_schema = {}
            return CSVSchemaValidator(custom_schema)
            
        else:
            supported_formats = ["JSON", "XML", "YAML", "CSV"]
            raise ValueError(
                f"Schema validation not supported for format: {format_type}. "
                f"Supported formats are: {', '.join(supported_formats)}"
            )