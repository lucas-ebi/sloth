from typing import (
    Dict,
    Any,
    Union,
    Optional,
    IO,
)
import os
import mmap
from abc import ABC, abstractmethod
from .models import MMCIFDataContainer, DataSourceFormat, Category, DataBlock
from .plugins import ValidatorFactory
from .common import auto_detect_format_and_load
from .validators import SchemaValidator


class DictToMMCIFConverter:
    def __init__(self, validator_factory: Optional[ValidatorFactory] = None):
        self.validator_factory = validator_factory

    def convert(self, data_dict: Dict[str, Any]) -> MMCIFDataContainer:
        data_blocks = {
            block_name: DataBlock(block_name, self._convert_categories(block_data))
            for block_name, block_data in data_dict.items()
        }
        return MMCIFDataContainer(data_blocks, source_format=DataSourceFormat.DICT)

    def _convert_categories(self, block_data: Dict[str, Any]) -> Dict[str, Category]:
        categories = {}
        for category_name, category_data in block_data.items():
            category = Category(category_name, self.validator_factory)
            if self._is_multi_row(category_data):
                self._populate_multiline_category(category, category_data)
            else:
                self._populate_singleline_category(category, category_data)
            categories[category_name] = category
        return categories

    def _is_multi_row(self, category_data: Any) -> bool:
        return (
            isinstance(category_data, list)
            and category_data
            and isinstance(category_data[0], dict)
        )

    def _populate_multiline_category(self, category: Category, rows: list):
        all_item_names = {k for row in rows for k in row}
        for item_name in all_item_names:
            category[item_name] = []
        for row in rows:
            for item_name in all_item_names:
                category[item_name].append(row.get(item_name, ""))

    def _populate_singleline_category(self, category: Category, data: Dict[str, Any]):
        for item_name, value in data.items():
            category[item_name] = value if isinstance(value, list) else [value]


class FormatLoader(ABC):
    def __init__(
        self,
        validator_factory: Optional[ValidatorFactory] = None,
        schema_validator: Optional[SchemaValidator] = None,
    ):
        self.validator_factory = validator_factory
        self.schema_validator = schema_validator

    @abstractmethod
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        raise NotImplementedError

    def validate_schema(self, data: Any) -> None:
        """
        Validate data against schema if a schema validator is provided.

        Args:
            data: Data to validate

        Raises:
            ValidationError: If schema validation fails
        """
        if self.schema_validator and data:  # Only validate if data is not empty
            self.schema_validator.validate(data)


class JsonLoader(FormatLoader):
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        import json

        try:
            if isinstance(input_, str) and os.path.exists(input_):
                if os.path.getsize(input_) > 0:
                    try:
                        with open(input_, "rb") as f:
                            mmap_obj = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                            data = json.loads(mmap_obj[:].decode("utf-8"))
                    except Exception:
                        with open(input_, "r") as f:
                            data = json.load(f)
                else:
                    # Empty file - create an empty data dictionary
                    data = {}
            elif isinstance(input_, str):
                data = json.loads(input_)
            else:
                data = json.load(input_)
        except json.JSONDecodeError as e:
            # Raise a clear error instead of silently converting to empty dict
            raise ValueError(f"Invalid JSON input: {e}") from e

        # Validate data against schema if provided
        self.validate_schema(data)

        container = DictToMMCIFConverter(self.validator_factory).convert(data)
        container.source_format = DataSourceFormat.JSON
        return container


class XmlLoader(FormatLoader):
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        from xml.etree import ElementTree as ET

        # Parse XML
        xml_data = input_

        if isinstance(input_, str) and os.path.exists(input_):
            # It's a file path
            root = ET.parse(input_).getroot()
            # For schema validation, use the parsed root
            xml_data = root
        elif isinstance(input_, str):
            # It's an XML string
            try:
                root = ET.fromstring(input_)
                xml_data = root
            except Exception:
                # Try encoding it if parsing as a string failed
                root = ET.fromstring(input_.encode("utf-8"))
                xml_data = root
        else:
            # It's a file-like object
            root = ET.parse(input_).getroot()
            xml_data = root

        # Validate XML against schema if provided
        if self.schema_validator:
            self.validate_schema(xml_data)

        data_dict = {}
        for block_elem in root.findall(".//data_block"):
            block_name = block_elem.get("name")
            block_dict = {}
            for category_elem in block_elem.findall("category"):
                category_name = category_elem.get("name")
                rows = category_elem.findall("row")
                if rows:
                    category_list = []
                    for row_elem in rows:
                        row_dict = {
                            item.get("name"): item.text or ""
                            for item in row_elem.findall("item")
                        }
                        category_list.append(row_dict)
                    block_dict[category_name] = category_list
                else:
                    block_dict[category_name] = {
                        item.get("name"): item.text or ""
                        for item in category_elem.findall("item")
                    }
            data_dict[block_name] = block_dict
        container = DictToMMCIFConverter(self.validator_factory).convert(data_dict)
        container.source_format = DataSourceFormat.XML
        return container


class PickleLoader(FormatLoader):
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        import pickle

        if not isinstance(input_, str):
            raise TypeError("PickleLoader requires a file path string.")
        if os.path.getsize(input_) > 0:
            try:
                with open(input_, "rb") as f:
                    mmap_obj = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                    data = pickle.loads(mmap_obj[:])
            except Exception:
                with open(input_, "rb") as f:
                    data = pickle.load(f)
        else:
            with open(input_, "rb") as f:
                data = pickle.load(f)

        # Validate data against schema if provided
        self.validate_schema(data)

        container = DictToMMCIFConverter(self.validator_factory).convert(data)
        container.source_format = DataSourceFormat.PICKLE
        return container


class YamlLoader(FormatLoader):
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        import yaml

        # Store original YAML for schema validation if string
        original_yaml = input_ if isinstance(input_, str) else None

        try:
            if isinstance(input_, str) and os.path.exists(input_):
                if os.path.getsize(input_) > 0:
                    try:
                        with open(input_, "rb") as f:
                            mmap_obj = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                            data = yaml.safe_load(mmap_obj[:].decode("utf-8"))
                            # Store original YAML for schema validation
                            original_yaml = mmap_obj[:].decode("utf-8")
                    except Exception:
                        with open(input_, "r") as f:
                            original_yaml = f.read()
                            data = yaml.safe_load(original_yaml)
                else:
                    # Empty file - create empty data dictionary
                    data = {}
            elif isinstance(input_, str):
                data = yaml.safe_load(input_)
                # Original YAML already stored
            else:
                original_yaml = None  # Can't get original from file handle
                data = yaml.safe_load(input_)

            # Handle None result from yaml.safe_load (empty file)
            if data is None:
                data = {}

        except Exception:
            # Handle any YAML parsing errors by returning an empty data dictionary
            data = {}

        # For schema validation, prefer the parsed data over the original string
        # as the YAMLSchemaValidator can handle both the string and parsed data
        validation_data = data if data else original_yaml
        self.validate_schema(validation_data)

        container = DictToMMCIFConverter(self.validator_factory).convert(data)
        container.source_format = DataSourceFormat.YAML
        return container


class CsvLoader(FormatLoader):
    def load(self, input_: Union[str, IO]) -> MMCIFDataContainer:
        import pandas as pd
        import glob
        import re

        if not isinstance(input_, str):
            raise TypeError("CsvLoader requires a directory path string.")
        
        # Pattern: matches block_name followed by single underscore, then category_name  
        # Category name may or may not start with underscore - we'll normalize it
        # e.g., "1ABC__entry.csv" -> block="1ABC", category="_entry"
        # e.g., "block1_category1.csv" -> block="block1", category="category1" (we'll add _ prefix)
        pattern = r"^(.+?)_(.+)\.csv$"  # block_name + _ + category_name
        data_dict = {}
        csv_file_data = {}  # Store file data for validation

        for csv_file in glob.glob(os.path.join(input_, "*.csv")):
            match = re.match(pattern, os.path.basename(csv_file))
            if match:
                block_name, category_name = match.groups()
                
                # Normalize category name: 
                # - If it starts with underscore, remove it for internal storage
                # - If it doesn't start with underscore, keep it as-is (and Category class will handle the prefix)
                if category_name.startswith('_'):
                    internal_category_name = category_name[1:]  # Remove the _ prefix for internal storage
                else:
                    internal_category_name = category_name  # Keep as-is for internal storage
                
                if block_name not in data_dict:
                    data_dict[block_name] = {}

                try:
                    df = pd.read_csv(csv_file)

                    # Skip empty files (no data rows)
                    if df.empty:
                        continue

                    # Store dataframe with filename for validation
                    csv_file_data[os.path.basename(csv_file)] = {
                        "file": os.path.basename(csv_file),
                        "data": df,
                    }

                    data_dict[block_name][internal_category_name] = df.to_dict("records")

                except pd.errors.EmptyDataError:
                    # Skip files with no columns or empty files
                    continue

        # Validate CSV data if a schema validator is provided
        # Pass each CSV file's data to the validator
        if self.schema_validator:
            for file_data in csv_file_data.values():
                self.validate_schema(file_data)

        container = DictToMMCIFConverter(self.validator_factory).convert(data_dict)
        container.source_format = DataSourceFormat.CSV
        return container


FORMAT_LOADERS = {
    DataSourceFormat.JSON: JsonLoader,
    DataSourceFormat.XML: XmlLoader,
    DataSourceFormat.YAML: YamlLoader,
    DataSourceFormat.PICKLE: PickleLoader,
    DataSourceFormat.CSV: CsvLoader,
}


class MMCIFImporter:
    @staticmethod
    def from_dict(
        data_dict: Dict[str, Any], validator_factory: Optional[ValidatorFactory] = None
    ) -> MMCIFDataContainer:
        return DictToMMCIFConverter(validator_factory).convert(data_dict)

    @staticmethod
    def from_json(
        json_str_or_file: Union[str, IO],
        validator_factory: Optional[ValidatorFactory] = None,
        schema_validator: Optional["SchemaValidator"] = None,
    ) -> MMCIFDataContainer:
        return JsonLoader(validator_factory, schema_validator).load(json_str_or_file)

    @staticmethod
    def from_xml(
        xml_str_or_file: Union[str, IO],
        validator_factory: Optional[ValidatorFactory] = None,
        schema_validator: Optional["SchemaValidator"] = None,
    ) -> MMCIFDataContainer:
        return XmlLoader(validator_factory, schema_validator).load(xml_str_or_file)

    @staticmethod
    def from_pickle(
        file_path: str,
        validator_factory: Optional[ValidatorFactory] = None,
        schema_validator: Optional["SchemaValidator"] = None,
    ) -> MMCIFDataContainer:
        return PickleLoader(validator_factory, schema_validator).load(file_path)

    @staticmethod
    def from_yaml(
        yaml_str_or_file: Union[str, IO],
        validator_factory: Optional[ValidatorFactory] = None,
        schema_validator: Optional["SchemaValidator"] = None,
    ) -> MMCIFDataContainer:
        return YamlLoader(validator_factory, schema_validator).load(yaml_str_or_file)

    @classmethod
    def from_csv_files(
        cls,
        directory_path: str,
        validator_factory: Optional[ValidatorFactory] = None,
        schema_validator: Optional["SchemaValidator"] = None,
    ) -> MMCIFDataContainer:
        return CsvLoader(validator_factory, schema_validator).load(directory_path)

    @classmethod
    def auto_detect_format(
        cls,
        file_path: str,
        validator_factory: Optional[ValidatorFactory] = None,
        schema_validator: Optional["SchemaValidator"] = None,
        validate_schema: bool = False,
    ) -> MMCIFDataContainer:
        """
        Auto-detect the format of the input file and load it.

        Args:
            file_path: Path to the file
            validator_factory: Optional validator factory for data validation
            schema_validator: Optional schema validator for format-specific schema validation
            validate_schema: Whether to validate against schema (if schema_validator is None,
                          will try to create one from SchemaValidatorFactory)

        Returns:
            MMCIFDataContainer object
        """
        # Use the common module to avoid circular imports
        return auto_detect_format_and_load(
            file_path, validator_factory, schema_validator, validate_schema
        )
