from typing import Dict, Any, Optional
from .models import MMCIFDataContainer


class MMCIFExporter:
    """A class to export mmCIF data to different formats like JSON, XML, Pickle, YAML, etc."""

    def __init__(self, mmcif: MMCIFDataContainer):
        """
        Initialize the exporter with an mmCIF data container.

        :param mmcif: The mmCIF data container to export
        :type mmcif: MMCIFDataContainer
        """
        self.mmcif = mmcif

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the mmCIF data container to a dictionary structure.

        :return: A dictionary representation of the mmCIF data
        :rtype: Dict[str, Any]
        """
        result = {}

        for block in self.mmcif:
            block_dict = {}

            for category_name in block.categories:
                category = block[category_name]
                category_dict = {}

                # Get all data (this will force loading of lazy items)
                items = category.data

                # Check if we have multiple rows
                if any(len(values) > 1 for values in items.values()):
                    # For multi-row categories, create a list of row objects
                    rows = []
                    for i in range(category.row_count):
                        row = {}
                        for item_name, values in items.items():
                            if i < len(values):
                                row[item_name] = values[i]
                        rows.append(row)
                    category_dict = rows
                else:
                    # For single-row categories, create a simple key-value object
                    for item_name, values in items.items():
                        if values:  # Check if there are any values
                            category_dict[item_name] = values[0]

                block_dict[category_name] = category_dict

            result[block.name] = block_dict

        return result

    def to_json(
        self, file_path: Optional[str] = None, indent: int = 2
    ) -> Optional[str]:
        """
        Export mmCIF data to JSON format.

        :param file_path: Path to save the JSON file (optional)
        :type file_path: Optional[str]
        :param indent: Number of spaces for indentation
        :type indent: int
        :return: JSON string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        import json

        data_dict = self.to_dict()

        if file_path:
            with open(file_path, "w") as f:
                json.dump(data_dict, f, indent=indent)
            return None
        else:
            return json.dumps(data_dict, indent=indent)

    def to_xml(
        self, file_path: Optional[str] = None, pretty_print: bool = True
    ) -> Optional[str]:
        """
        Export mmCIF data to XML format.

        :param file_path: Path to save the XML file (optional)
        :type file_path: Optional[str]
        :param pretty_print: Whether to format XML with indentation
        :type pretty_print: bool
        :return: XML string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        from xml.dom import minidom
        from xml.etree import ElementTree as ET

        root = ET.Element("mmcif_data")

        for block in self.mmcif:
            block_elem = ET.SubElement(root, "data_block", name=block.name)

            for category_name in block.categories:
                category = block[category_name]
                category_elem = ET.SubElement(
                    block_elem, "category", name=category_name
                )

                # Get all data (this will force loading of lazy items)
                items = category.data

                if any(len(values) > 1 for values in items.values()):
                    # For multi-row categories
                    for i in range(category.row_count):
                        row_elem = ET.SubElement(category_elem, "row", index=str(i))
                        for item_name, values in items.items():
                            if i < len(values):
                                item_elem = ET.SubElement(
                                    row_elem, "item", name=item_name
                                )
                                item_elem.text = values[i]
                else:
                    # For single-row categories
                    for item_name, values in items.items():
                        if values:  # Check if there are any values
                            item_elem = ET.SubElement(
                                category_elem, "item", name=item_name
                            )
                            item_elem.text = values[0]

        # Convert to string
        rough_string = ET.tostring(root, "utf-8")

        if pretty_print:
            reparsed = minidom.parseString(rough_string)
            xml_string = reparsed.toprettyxml(indent="  ")
        else:
            xml_string = rough_string.decode("utf-8")

        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(xml_string)
            return None
        else:
            return xml_string

    def to_pickle(self, file_path: str) -> None:
        """
        Export mmCIF data to a Python pickle file.

        :param file_path: Path to save the pickle file
        :type file_path: str
        :return: None
        """
        import pickle

        data_dict = self.to_dict()

        with open(file_path, "wb") as f:
            pickle.dump(data_dict, f)

    def to_yaml(self, file_path: Optional[str] = None) -> Optional[str]:
        """
        Export mmCIF data to YAML format.

        :param file_path: Path to save the YAML file (optional)
        :type file_path: Optional[str]
        :return: YAML string if no file_path provided, otherwise None
        :rtype: Optional[str]
        """
        # Using PyYAML package
        try:
            import yaml
        except ImportError:
            raise ImportError(
                "PyYAML package is required for YAML export. Install it using 'pip install pyyaml'."
            )

        data_dict = self.to_dict()

        if file_path:
            with open(file_path, "w") as f:
                yaml.dump(data_dict, f, default_flow_style=False)
            return None
        else:
            return yaml.dump(data_dict, default_flow_style=False)

    def to_pandas(self) -> Dict[str, Dict[str, Any]]:
        """
        Export mmCIF data to pandas DataFrames, with one DataFrame per category.

        :return: Dictionary of DataFrames organized by data block and category
        :rtype: Dict[str, Dict[str, Any]]
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas package is required for DataFrame export. Install it using 'pip install pandas'."
            )

        result = {}

        for block in self.mmcif:
            block_dict = {}

            for category_name in block.categories:
                category = block[category_name]

                # Get all data (this will force loading of lazy items)
                items = category.data

                # Create DataFrame from items - convert LazyItemDict to regular dict first
                df = pd.DataFrame(dict(items))
                block_dict[category_name] = df

            result[block.name] = block_dict

        return result

    def to_csv(
        self, directory_path: str, prefix: str = ""
    ) -> Dict[str, Dict[str, str]]:
        """
        Export mmCIF data to CSV files, with one file per category.

        :param directory_path: Directory to save the CSV files
        :type directory_path: str
        :param prefix: Prefix for CSV filenames
        :type prefix: str
        :return: Dictionary mapping block and category names to file paths
        :rtype: Dict[str, Dict[str, str]]
        """
        try:
            import pandas as pd
            import os
        except ImportError:
            raise ImportError(
                "pandas package is required for CSV export. Install it using 'pip install pandas'."
            )

        # Create directory if it doesn't exist
        os.makedirs(directory_path, exist_ok=True)

        file_paths = {}

        for block in self.mmcif:
            block_dict = {}

            for category_name in block.categories:
                category = block[category_name]

                # Get all data (this will force loading of lazy items)
                items = category.data

                # Create DataFrame from items - convert LazyItemDict to regular dict first
                df = pd.DataFrame(dict(items))

                # Create CSV filename
                filename = f"{prefix}{block.name}_{category_name}.csv"
                filepath = os.path.join(directory_path, filename)

                # Save to CSV
                df.to_csv(filepath, index=False)
                block_dict[category_name] = filepath

            file_paths[block.name] = block_dict

        return file_paths
