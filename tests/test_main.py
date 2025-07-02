#!/usr/bin/env python3
"""
Comprehensive test suite for Sloth.

Tests all core functionality including parsing, writing, validation,
and data manipulation using the simple, always-optimized API.
"""

import unittest
import tempfile
import os
import json
import shutil
import pickle
import yaml
from io import StringIO
from unittest.mock import mock_open, patch
from sloth import (
    MMCIFHandler,
    MMCIFWriter,
    MMCIFExporter,
    MMCIFImporter,
    MMCIFDataContainer,
    DataBlock,
    Category,
    Row,
    Item,
    ValidatorFactory,
    DataSourceFormat,
    JsonLoader,
    XmlLoader,
    YamlLoader,
    PickleLoader,
    CsvLoader,
    DictToMMCIFConverter,
)


class TestMMCIFParser(unittest.TestCase):
    mmcif_content = """
data_7XJP
#
_database_2.database_id      PDB
_database_2.database_code    7XJP
#
"""

    def setUp(self):
        self.handler = MMCIFHandler(validator_factory=None)

    def test_read_empty_file(self):
        # Create a temporary file since mmap requires a real file
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(mode="w", suffix=".cif", delete=False) as f:
            f.write("data_empty\n#\n")
            f.flush()  # Ensure content is written to disk
            temp_file = f.name

        try:
            mmcif = self.handler.parse(temp_file)
            self.assertEqual(len(mmcif), 1)
            self.assertIn("data_empty", mmcif.blocks)
        finally:
            os.unlink(temp_file)

    def test_read_file_with_data(self):
        # Create a temporary file for testing since mmap requires a real file
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(mode="w", suffix=".cif", delete=False) as f:
            f.write(self.mmcif_content)
            temp_file = f.name

        try:
            mmcif = self.handler.parse(temp_file, categories=["_database_2"])
            self.assertIn("data_7XJP", mmcif.blocks)
            data_block = mmcif["7XJP"]
            self.assertIn("_database_2", data_block.categories)
            category = data_block["_database_2"]
            self.assertEqual(category["database_id"], ["PDB"])
            self.assertEqual(category["database_code"], ["7XJP"])
        finally:
            os.unlink(temp_file)


class TestMMCIFWriter(unittest.TestCase):
    def setUp(self):
        self.data_block = DataBlock(
            name="7XJP",
            categories={
                "_database_2": Category(name="_database_2", validator_factory=None)
            },
        )
        self.data_block["_database_2"]._add_item_value("database_id", "PDB")
        self.data_block["_database_2"]._add_item_value("database_code", "7XJP")
        # Commit batches to make data available for writing
        self.data_block["_database_2"]._commit_all_batches()
        self.mmcif = MMCIFDataContainer(data_blocks={"7XJP": self.data_block})
        self.writer = MMCIFWriter()

    @patch("builtins.open", new_callable=mock_open)
    def test_write_file(self, mock_file):
        with open("dummy.cif", "w") as f:
            self.writer.write(f, self.mmcif)
        mock_file().write.assert_any_call("data_7XJP\n")
        mock_file().write.assert_any_call("#\n")
        mock_file().write.assert_any_call("_database_2.database_id PDB \n")
        mock_file().write.assert_any_call("_database_2.database_code 7XJP \n")
        mock_file().write.assert_any_call("#\n")


class TestMMCIFHandler(unittest.TestCase):
    mmcif_content = """
data_7XJP
#
_database_2.database_id      PDB
_database_2.database_code    7XJP
#
"""

    def setUp(self):
        self.handler = MMCIFHandler(validator_factory=None)

    def test_parse_file(self):
        # Create a temporary file since mmap requires a real file
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(mode="w", suffix=".cif", delete=False) as f:
            f.write(self.mmcif_content)
            temp_file = f.name

        try:
            mmcif = self.handler.parse(temp_file, categories=["_database_2"])
            self.assertIn("data_7XJP", mmcif.blocks)
            data_block = mmcif["7XJP"]
            self.assertIn("_database_2", data_block.categories)
            category = data_block["_database_2"]
            self.assertEqual(category["database_id"], ["PDB"])
            self.assertEqual(category["database_code"], ["7XJP"])
        finally:
            os.unlink(temp_file)

    @patch("builtins.open", new_callable=mock_open)
    def test_write_file(self, mock_file):
        data_block = DataBlock(
            name="7XJP",
            categories={
                "_database_2": Category(name="_database_2", validator_factory=None)
            },
        )
        data_block["_database_2"]._add_item_value("database_id", "PDB")
        data_block["_database_2"]._add_item_value("database_code", "7XJP")
        # Commit batches to make data available for writing
        data_block["_database_2"]._commit_all_batches()
        mmcif = MMCIFDataContainer(data_blocks={"7XJP": data_block})
        with open("dummy.cif", "w") as f:
            self.handler.file_obj = f
            self.handler.write(mmcif)
        mock_file().write.assert_any_call("data_7XJP\n")
        mock_file().write.assert_any_call("#\n")
        mock_file().write.assert_any_call("_database_2.database_id PDB \n")
        mock_file().write.assert_any_call("_database_2.database_code 7XJP \n")
        mock_file().write.assert_any_call("#\n")


class TestValidatorFactory(unittest.TestCase):
    def setUp(self):
        self.factory = ValidatorFactory()

    def test_register_and_get_validator(self):
        def validator(category_name: str):
            pass

        self.factory.register_validator("test_category", validator)
        self.assertEqual(self.factory.get_validator("test_category"), validator)

    def test_register_and_get_cross_checker(self):
        def cross_checker(category1: str, category2: str):
            pass

        self.factory.register_cross_checker(("category1", "category2"), cross_checker)
        self.assertEqual(
            self.factory.get_cross_checker(("category1", "category2")), cross_checker
        )


class TestCategoryValidation(unittest.TestCase):
    def setUp(self):
        self.factory = ValidatorFactory()
        self.category = Category(name="_database_2", validator_factory=self.factory)

    def test_validate(self):
        def validator(category_name: str):
            self.assertEqual(category_name, "_database_2")

        self.factory.register_validator("_database_2", validator)
        self.category.validate()

    def test_validate_against(self):
        other_category = Category(name="_database_1", validator_factory=self.factory)

        def cross_checker(category1: str, category2: str):
            self.assertEqual(category1, "_database_2")
            self.assertEqual(category2, "_database_1")

        self.factory.register_cross_checker(
            ("_database_2", "_database_1"), cross_checker
        )
        self.category.validate().against(other_category)


class TestItemAndCategory(unittest.TestCase):
    """Test Item and Category classes."""

    def test_item_creation(self):
        """Test Item class basic functionality."""
        item = Item("test_item", values=["value1", "value2", "value3"])

        self.assertEqual(item.name, "test_item")
        self.assertEqual(len(item), 3)
        self.assertEqual(list(item), ["value1", "value2", "value3"])
        self.assertEqual(item[0], "value1")
        self.assertEqual(item[1:3], ["value2", "value3"])

    def test_item_behavior(self):
        """Test Item behavior."""
        item = Item("test_item")

        # Initially empty
        self.assertEqual(len(item), 0)

        # Add values
        item.add_value("value1")
        item.add_value("value2")

        self.assertEqual(len(item), 2)
        self.assertEqual(list(item), ["value1", "value2"])

    def test_category_with_items(self):
        """Test Category class with items."""
        category = Category("test_category", None)

        # Add items
        category._add_item_value("item1", "value1")
        category._add_item_value("item1", "value2")
        category._add_item_value("item2", "valueA")
        # Commit batches to make data available
        category._commit_all_batches()

        # Check values
        self.assertEqual(category.item1, ["value1", "value2"])
        self.assertEqual(category.item2, ["valueA"])

    def test_row_access(self):
        """Test row-wise access to Category."""
        category = Category("test_category", None)

        # Add multiple rows of data
        category._add_item_value("name", "John")
        category._add_item_value("name", "Alice")
        category._add_item_value("name", "Bob")

        category._add_item_value("age", "25")
        category._add_item_value("age", "30")
        category._add_item_value("age", "22")

        category._add_item_value("city", "New York")
        category._add_item_value("city", "Boston")
        category._add_item_value("city", "Chicago")
        # Commit batches to make data available
        category._commit_all_batches()

        # Test single row access
        row0 = category[0]
        self.assertEqual(row0.name, "John")
        self.assertEqual(row0.age, "25")
        self.assertEqual(row0.city, "New York")

        # Test dictionary access to row
        self.assertEqual(row0["name"], "John")
        self.assertEqual(row0["age"], "25")
        self.assertEqual(row0["city"], "New York")

        # Test row.data property
        row_data = row0.data
        self.assertIsInstance(row_data, dict)
        self.assertEqual(row_data, {"name": "John", "age": "25", "city": "New York"})

        # Test negative index
        row_last = category[-1]
        self.assertEqual(row_last.name, "Bob")

        # Test invalid row index
        with self.assertRaises(IndexError):
            invalid_row = category[5]

        # Test invalid item name
        with self.assertRaises(KeyError):
            invalid_value = row0["invalid"]

        with self.assertRaises(AttributeError):
            invalid_value = row0.invalid

    def test_row_slicing(self):
        """Test row slicing of Category."""
        category = Category("test_category", None)

        # Add multiple rows of data
        for i in range(5):
            category._add_item_value("id", str(i))
            category._add_item_value("value", f"value_{i}")
        # Commit batches to make data available
        category._commit_all_batches()

        # Test slicing
        rows = category[1:4]
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].id, "1")
        self.assertEqual(rows[1].id, "2")
        self.assertEqual(rows[2].id, "3")

        # Test slice with step
        rows = category[0:5:2]
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].id, "0")
        self.assertEqual(rows[1].id, "2")
        self.assertEqual(rows[2].id, "4")

        # Test empty slice
        empty_rows = category[5:10]
        self.assertEqual(len(empty_rows), 0)

    def test_row_count_and_rows(self):
        """Test row_count and rows properties."""
        category = Category("test_category", None)

        # Empty category
        self.assertEqual(category.row_count, 0)
        self.assertEqual(len(category.rows), 0)

        # Add rows
        category._add_item_value("id", "1")
        category._add_item_value("id", "2")
        category._add_item_value("id", "3")
        # Commit batches to make data available
        category._commit_all_batches()

        self.assertEqual(category.row_count, 3)
        self.assertEqual(len(category.rows), 3)

        # Verify all rows are Row instances
        for row in category.rows:
            self.assertIsInstance(row, Row)

    def test_combined_column_row_access(self):
        """Test combination of column and row access."""
        category = Category("test_category", None)

        # Add data
        category._add_item_value("x", "1")
        category._add_item_value("x", "2")
        category._add_item_value("y", "10")
        category._add_item_value("y", "20")
        # Commit batches to make data available
        category._commit_all_batches()

        # Column access
        self.assertEqual(category["x"], ["1", "2"])

        # Row access
        self.assertEqual(category[0].x, "1")
        self.assertEqual(category[0].y, "10")
        self.assertEqual(category[1].x, "2")
        self.assertEqual(category[1].y, "20")

        # Mixed use - get a value using both approaches
        self.assertEqual(category["x"][0], category[0].x)
        self.assertEqual(category["y"][1], category[1].y)


class TestFileProcessing(unittest.TestCase):
    """Test file processing with real files."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_mmcif_content = """data_TEST
#
_entry.id TEST_ENTRY
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.label_atom_id
_atom_site.auth_seq_id
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
_atom_site.occupancy
_atom_site.B_iso_or_equiv
ATOM   1    N  N   20.154 6.718  22.746
ATOM   2    C  CA  21.618 6.756  22.530
ATOM   3    C  C   22.097 8.130  22.050
ATOM   4    O  O   21.346 8.963  21.523
#
"""
        # Create temporary file
        self.temp_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".cif", delete=False
        )
        self.temp_file.write(self.test_mmcif_content)
        self.temp_file.close()

    def tearDown(self):
        """Clean up test fixtures."""
        os.unlink(self.temp_file.name)

    def test_handler_parsing(self):
        """Test MMCIFHandler parsing."""
        handler = MMCIFHandler()

        # Parse the test file
        mmcif = handler.parse(self.temp_file.name)

        # Verify structure
        self.assertEqual(list(mmcif.blocks), ["data_TEST"])

        # Get test block
        block = mmcif.data[0]

        # Verify data was parsed correctly
        if "_atom_site" in block.categories:
            atom_site = block._atom_site
            # Should have parsed items
            self.assertGreater(len(atom_site.items), 0, "Should have parsed items")

    def test_data_consistency(self):
        """Test that parsing produces consistent results."""
        # Parse the same file multiple times
        handler = MMCIFHandler()
        data1 = handler.parse(self.temp_file.name)
        data2 = handler.parse(self.temp_file.name)

        # Should be consistent
        self.assertEqual(data1.blocks, data2.blocks)

        # Compare specific values
        for block_name in data1.blocks:
            block1 = data1[block_name]
            block2 = data2[block_name]

            for category_name in block1.categories:
                if category_name in block2.categories:
                    category1 = block1[category_name]
                    category2 = block2[category_name]

                    # Compare data
                    self.assertEqual(
                        category1.data,
                        category2.data,
                        f"Data mismatch in category {category_name}",
                    )

    def test_parsing_performance(self):
        """Test that the parser works efficiently."""
        import time

        # Create larger test content for performance testing
        large_content = self.test_mmcif_content
        for i in range(100):  # Add more atom records
            large_content += f"ATOM   {i+5}    C  CB  {20.0+i} {6.0+i}  {22.0+i}\n"
        large_content += "#\n"

        # Write to temp file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".cif", delete=False) as f:
            f.write(large_content)
            large_file = f.name

        try:
            # Time the parsing
            start_time = time.time()
            handler = MMCIFHandler()
            data = handler.parse(large_file)
            parse_time = time.time() - start_time

            print(f"Parsing time: {parse_time:.4f}s")

            # Verify data was parsed correctly
            block = data.data[0]
            if "_atom_site" in block.categories:
                atom_site = block._atom_site
                self.assertGreater(
                    len(atom_site.items), 0, "Should have parsed data correctly"
                )

        finally:
            os.unlink(large_file)


class TestPerformanceAndMemory(unittest.TestCase):
    """Test performance characteristics and memory efficiency."""

    def test_large_file_handling(self):
        """Test handling of larger files efficiently."""
        # Create a larger test file
        large_content = """data_LARGE_TEST
#
_entry.id LARGE_TEST
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
"""

        # Add many atom records
        for i in range(1000):
            large_content += (
                f"ATOM   {i+1}    C  {float(i)} {float(i+1)} {float(i+2)}\n"
            )

        large_content += "#\n"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".cif", delete=False) as f:
            f.write(large_content)
            large_file = f.name

        try:
            import time

            # Time the parsing
            start_time = time.time()
            handler = MMCIFHandler()
            data = handler.parse(large_file)
            parse_time = time.time() - start_time

            # Should be reasonably fast (less than 1 second for 1000 atoms)
            self.assertLess(parse_time, 1.0, "Should parse large files quickly")

            # Verify data was parsed correctly
            block = data.data[0]
            if "_atom_site" in block.categories:
                atom_site = block._atom_site
                self.assertEqual(
                    len(atom_site.Cartn_x), 1000, "Should have parsed all atoms"
                )

        finally:
            os.unlink(large_file)

    def test_memory_efficiency_with_lazy_loading(self):
        """Test memory efficiency with lazy loading."""
        # Create content with many values
        content = """data_MEMORY_TEST
#
_entry.id MEMORY_TEST
#
loop_
_test_data.index
_test_data.value
"""

        # Add many data rows
        for i in range(5000):
            content += f"{i} value_{i}\n"

        content += "#\n"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".cif", delete=False) as f:
            f.write(content)
            memory_file = f.name

        try:
            handler = MMCIFHandler()
            data = handler.parse(memory_file)

            block = data.data[0]

            # Should parse quickly without loading all data
            if "_test_data" in block.categories:
                test_data = block._test_data

                # Check that we can access specific values efficiently
                first_value = test_data.value[0]
                self.assertEqual(first_value, "value_0")

                # Check length without loading all data
                self.assertEqual(len(test_data.value), 5000)

        finally:
            os.unlink(memory_file)


class TestMMCIFExporter(unittest.TestCase):
    """Test case for the MMCIFExporter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

        # Create a simple mmCIF file for testing
        self.test_cif_path = os.path.join(self.temp_dir, "test.cif")
        with open(self.test_cif_path, "w") as f:
            f.write(
                """data_test
#
_entry.id test_structure
#
_database_2.database_id      TEST
_database_2.database_code    ABC123
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
ATOM   1    N  10.123 20.456 30.789
ATOM   2    C  11.234 21.567 31.890
ATOM   3    C  12.345 22.678 32.901
#
"""
            )

        # Parse the test file
        handler = MMCIFHandler()
        self.mmcif = handler.parse(self.test_cif_path)
        self.exporter = MMCIFExporter(self.mmcif)

    def tearDown(self):
        """Tear down test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_to_dict(self):
        """Test conversion to dictionary."""
        data_dict = self.exporter.to_dict()

        # Verify the structure
        self.assertIn("test", data_dict)
        self.assertIn("_entry", data_dict["test"])
        self.assertIn("_atom_site", data_dict["test"])

        # Verify specific values
        self.assertEqual(data_dict["test"]["_entry"]["id"], "test_structure")

        # Verify multi-row category
        atom_site = data_dict["test"]["_atom_site"]
        self.assertEqual(len(atom_site), 3)  # Three rows
        self.assertEqual(atom_site[0]["Cartn_x"], "10.123")

    def test_to_json(self):
        """Test JSON export."""
        json_path = os.path.join(self.temp_dir, "test.json")
        self.exporter.to_json(json_path)

        # Verify file exists
        self.assertTrue(os.path.exists(json_path))

        # Verify content
        with open(json_path) as f:
            data = json.load(f)

        self.assertIn("test", data)
        self.assertIn("_database_2", data["test"])
        self.assertEqual(data["test"]["_database_2"]["database_id"], "TEST")

        # Test without file path (string return)
        json_str = self.exporter.to_json()
        self.assertIsInstance(json_str, str)
        data_from_str = json.loads(json_str)
        self.assertIn("test", data_from_str)

    def test_handler_export_methods(self):
        """Test export methods in MMCIFHandler."""
        handler = MMCIFHandler()

        # Test JSON export
        json_path = os.path.join(self.temp_dir, "handler.json")
        handler.export_to_json(self.mmcif, json_path)
        self.assertTrue(os.path.exists(json_path))

        # Test XML export
        xml_path = os.path.join(self.temp_dir, "handler.xml")
        handler.export_to_xml(self.mmcif, xml_path)
        self.assertTrue(os.path.exists(xml_path))

        # Test pickle export
        pkl_path = os.path.join(self.temp_dir, "handler.pkl")
        handler.export_to_pickle(self.mmcif, pkl_path)
        self.assertTrue(os.path.exists(pkl_path))

    def test_to_yaml_pandas_availability(self):
        """Test YAML and pandas export availability."""
        # We don't actually test the functionality, just that the methods exist
        # and don't crash when libraries are not available

        # Test YAML export (should not crash whether PyYAML is installed or not)
        try:
            yaml_str = self.exporter.to_yaml()
            # If we get here, PyYAML is installed
            self.assertIsInstance(yaml_str, (str, type(None)))
        except ImportError:
            # This is fine, PyYAML might not be installed
            pass

        # Test pandas and CSV export (should not crash)
        try:
            # Try pandas export
            result = self.exporter.to_pandas()
            # If we get here, pandas is installed
            self.assertIsInstance(result, dict)
        except ImportError:
            # This is fine, pandas might not be installed
            pass


class TestMMCIFImporter(unittest.TestCase):
    """Test case for the MMCIFImporter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

        # Create a simple mmCIF file for testing
        self.test_cif_path = os.path.join(self.temp_dir, "test.cif")
        with open(self.test_cif_path, "w") as f:
            f.write(
                """data_test
#
_entry.id test_structure
#
_database_2.database_id      TEST
_database_2.database_code    ABC123
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
ATOM   1    N  10.123 20.456 30.789
ATOM   2    C  11.234 21.567 31.890
ATOM   3    C  12.345 22.678 32.901
#
"""
            )

        # Parse the test file
        handler = MMCIFHandler()
        self.mmcif = handler.parse(self.test_cif_path)
        self.exporter = MMCIFExporter(self.mmcif)

        # Export data to different formats for import testing
        self.json_path = os.path.join(self.temp_dir, "test.json")
        self.exporter.to_json(self.json_path)

        self.xml_path = os.path.join(self.temp_dir, "test.xml")
        self.exporter.to_xml(self.xml_path)

        self.pkl_path = os.path.join(self.temp_dir, "test.pkl")
        self.exporter.to_pickle(self.pkl_path)

        # Create YAML and CSV files if the dependencies are available
        try:
            self.yaml_path = os.path.join(self.temp_dir, "test.yaml")
            self.exporter.to_yaml(self.yaml_path)
            self.yaml_available = True
        except ImportError:
            self.yaml_available = False

        try:
            self.csv_dir = os.path.join(self.temp_dir, "csv_files")
            os.makedirs(self.csv_dir, exist_ok=True)
            self.exporter.to_csv(self.csv_dir)
            self.pandas_available = True
        except ImportError:
            self.pandas_available = False

        # Create the importer instance
        self.importer = MMCIFImporter()

    def tearDown(self):
        """Tear down test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_from_dict(self):
        """Test importing from dictionary."""
        # First export to dict
        data_dict = self.exporter.to_dict()

        # Now import from dict
        imported_container = MMCIFImporter.from_dict(data_dict)

        # Verify structure
        self.assertIn("data_test", imported_container.blocks)
        self.assertIn("_entry", imported_container["test"].categories)
        self.assertIn("_atom_site", imported_container["test"].categories)

        # Verify values
        self.assertEqual(
            imported_container["test"]["_entry"]["id"][0], "test_structure"
        )
        self.assertEqual(
            imported_container["test"]["_database_2"]["database_id"][0], "TEST"
        )
        self.assertEqual(
            imported_container["test"]["_database_2"]["database_code"][0], "ABC123"
        )

        # Verify multi-row category
        atom_site = imported_container["test"]["_atom_site"]
        self.assertEqual(len(atom_site["id"]), 3)  # Three rows
        self.assertEqual(atom_site["Cartn_x"][0], "10.123")
        self.assertEqual(atom_site["Cartn_y"][1], "21.567")
        self.assertEqual(atom_site["Cartn_z"][2], "32.901")

    def test_from_json(self):
        """Test importing from JSON."""
        # Test from file path
        imported_container = self.importer.from_json(self.json_path)

        # Verify structure and content
        self.assertIn("data_test", imported_container.blocks)
        self.assertEqual(
            imported_container["test"]["_entry"]["id"][0], "test_structure"
        )
        self.assertEqual(
            imported_container["test"]["_atom_site"]["Cartn_x"][0], "10.123"
        )

        # Test from JSON string
        with open(self.json_path, "r") as f:
            json_str = f.read()

        imported_container_from_str = self.importer.from_json(json_str)
        self.assertIn("data_test", imported_container_from_str.blocks)
        self.assertEqual(
            imported_container_from_str["test"]["_entry"]["id"][0], "test_structure"
        )

    def test_from_xml(self):
        """Test importing from XML."""
        # Test from file path
        imported_container = self.importer.from_xml(self.xml_path)

        # Verify structure and content
        self.assertIn("data_test", imported_container.blocks)
        self.assertEqual(
            imported_container["test"]["_entry"]["id"][0], "test_structure"
        )
        self.assertEqual(
            imported_container["test"]["_atom_site"]["Cartn_x"][0], "10.123"
        )

        # Test from XML string
        with open(self.xml_path, "r") as f:
            xml_str = f.read()

        imported_container_from_str = self.importer.from_xml(xml_str)
        self.assertIn("data_test", imported_container_from_str.blocks)
        self.assertEqual(
            imported_container_from_str["test"]["_entry"]["id"][0], "test_structure"
        )

    def test_from_pickle(self):
        """Test importing from Pickle."""
        imported_container = self.importer.from_pickle(self.pkl_path)

        # Verify structure and content
        self.assertIn("data_test", imported_container.blocks)
        self.assertEqual(
            imported_container["test"]["_entry"]["id"][0], "test_structure"
        )
        self.assertEqual(
            imported_container["test"]["_atom_site"]["Cartn_x"][0], "10.123"
        )

    def test_from_yaml(self):
        """Test importing from YAML if PyYAML is available."""
        if not self.yaml_available:
            self.skipTest("PyYAML is not installed")

        # Test from file path
        imported_container = self.importer.from_yaml(self.yaml_path)

        # Verify structure and content
        self.assertIn("data_test", imported_container.blocks)
        self.assertEqual(
            imported_container["test"]["_entry"]["id"][0], "test_structure"
        )
        self.assertEqual(
            imported_container["test"]["_atom_site"]["Cartn_x"][0], "10.123"
        )

        # Test from YAML string
        with open(self.yaml_path, "r") as f:
            yaml_str = f.read()

        imported_container_from_str = self.importer.from_yaml(yaml_str)
        self.assertIn("data_test", imported_container_from_str.blocks)
        self.assertEqual(
            imported_container_from_str["test"]["_entry"]["id"][0], "test_structure"
        )

    def test_from_csv_files(self):
        """Test importing from CSV files if pandas is available."""
        if not self.pandas_available:
            self.skipTest("pandas is not installed")

        imported_container = self.importer.from_csv_files(self.csv_dir)

        # Verify structure and content
        self.assertIn("data_test", imported_container.blocks)
        self.assertIn("_entry", imported_container["test"].categories)
        self.assertIn("_atom_site", imported_container["test"].categories)

        # Verify values (note: CSV might change data types)
        self.assertEqual(
            imported_container["test"]["_entry"]["id"][0], "test_structure"
        )

        # Verify multi-row category exists with correct number of rows
        atom_site = imported_container["test"]["_atom_site"]
        self.assertEqual(len(atom_site["id"]), 3)

    def test_auto_detect_format(self):
        """Test auto-detection of file formats."""
        # Test JSON auto-detection
        json_container = MMCIFImporter.auto_detect_format(self.json_path)
        self.assertIn("data_test", json_container.blocks)
        self.assertEqual(json_container["test"]["_entry"]["id"][0], "test_structure")

        # Test XML auto-detection
        xml_container = MMCIFImporter.auto_detect_format(self.xml_path)
        self.assertIn("data_test", xml_container.blocks)
        self.assertEqual(xml_container["test"]["_entry"]["id"][0], "test_structure")

        # Test Pickle auto-detection
        pkl_container = MMCIFImporter.auto_detect_format(self.pkl_path)
        self.assertIn("data_test", pkl_container.blocks)
        self.assertEqual(pkl_container["test"]["_entry"]["id"][0], "test_structure")

        # Test CIF auto-detection (original file)
        cif_container = MMCIFImporter.auto_detect_format(self.test_cif_path)
        self.assertIn("data_test", cif_container.blocks)
        self.assertEqual(cif_container["test"]["_entry"]["id"][0], "test_structure")

        # Test YAML auto-detection if available
        if self.yaml_available:
            yaml_container = MMCIFImporter.auto_detect_format(self.yaml_path)
            self.assertIn("data_test", yaml_container.blocks)
            self.assertEqual(
                yaml_container["test"]["_entry"]["id"][0], "test_structure"
            )

        # Test invalid extension
        with self.assertRaises(ValueError):
            MMCIFImporter.auto_detect_format(
                os.path.join(self.temp_dir, "test.unsupported")
            )

    def test_handler_import_methods(self):
        """Test import methods in MMCIFHandler."""
        handler = MMCIFHandler()

        # Test JSON import
        json_container = handler.import_from_json(self.json_path)
        self.assertIn("data_test", json_container.blocks)
        self.assertEqual(json_container["test"]["_entry"]["id"][0], "test_structure")

        # Test XML import
        xml_container = handler.import_from_xml(self.xml_path)
        self.assertIn("data_test", xml_container.blocks)
        self.assertEqual(xml_container["test"]["_entry"]["id"][0], "test_structure")

        # Test Pickle import
        pkl_container = handler.import_from_pickle(self.pkl_path)
        self.assertIn("data_test", pkl_container.blocks)
        self.assertEqual(pkl_container["test"]["_entry"]["id"][0], "test_structure")

        # Test auto-detection
        auto_container = handler.import_auto_detect(self.json_path)
        self.assertIn("data_test", auto_container.blocks)
        self.assertEqual(auto_container["test"]["_entry"]["id"][0], "test_structure")

    def test_round_trip_json(self):
        """Test round-trip export-import using JSON."""
        # Export to JSON
        json_path = os.path.join(self.temp_dir, "round_trip.json")
        self.exporter.to_json(json_path)

        # Import from JSON
        imported_container = self.importer.from_json(json_path)

        # Compare original and imported
        self.assertEqual(len(self.mmcif.blocks), len(imported_container.blocks))
        self.assertEqual(
            self.mmcif["test"]["_entry"]["id"][0],
            imported_container["test"]["_entry"]["id"][0],
        )
        self.assertEqual(
            self.mmcif["test"]["_atom_site"]["Cartn_x"][0],
            imported_container["test"]["_atom_site"]["Cartn_x"][0],
        )

    def test_round_trip_xml(self):
        """Test round-trip export-import using XML."""
        # Export to XML
        xml_path = os.path.join(self.temp_dir, "round_trip.xml")
        self.exporter.to_xml(xml_path)

        # Import from XML
        imported_container = self.importer.from_xml(xml_path)

        # Compare original and imported
        self.assertEqual(len(self.mmcif.blocks), len(imported_container.blocks))
        self.assertEqual(
            self.mmcif["test"]["_entry"]["id"][0],
            imported_container["test"]["_entry"]["id"][0],
        )
        self.assertEqual(
            self.mmcif["test"]["_atom_site"]["Cartn_x"][0],
            imported_container["test"]["_atom_site"]["Cartn_x"][0],
        )

    def test_round_trip_pickle(self):
        """Test round-trip export-import using Pickle."""
        # Export to Pickle
        pkl_path = os.path.join(self.temp_dir, "round_trip.pkl")
        self.exporter.to_pickle(pkl_path)

        # Import from Pickle
        imported_container = self.importer.from_pickle(pkl_path)

        # Compare original and imported
        self.assertEqual(len(self.mmcif.blocks), len(imported_container.blocks))
        self.assertEqual(
            self.mmcif["test"]["_entry"]["id"][0],
            imported_container["test"]["_entry"]["id"][0],
        )
        self.assertEqual(
            self.mmcif["test"]["_atom_site"]["Cartn_x"][0],
            imported_container["test"]["_atom_site"]["Cartn_x"][0],
        )

    def test_source_format_flag(self):
        """Test that the source format flag is correctly set."""
        # Test JSON source format
        json_container = self.importer.from_json(self.json_path)
        self.assertEqual(json_container.source_format, DataSourceFormat.JSON)

        # Test XML source format
        xml_container = self.importer.from_xml(self.xml_path)
        self.assertEqual(xml_container.source_format, DataSourceFormat.XML)

        # Test Pickle source format
        pickle_container = self.importer.from_pickle(self.pkl_path)
        self.assertEqual(pickle_container.source_format, DataSourceFormat.PICKLE)

        # Test CIF source format via auto-detect
        cif_container = MMCIFImporter.auto_detect_format(self.test_cif_path)
        self.assertEqual(cif_container.source_format, DataSourceFormat.MMCIF)

        # Test dictionary source format
        data_dict = self.exporter.to_dict()
        dict_container = MMCIFImporter.from_dict(data_dict)
        self.assertEqual(dict_container.source_format, DataSourceFormat.DICT)

        # Test YAML source format if available
        if self.yaml_available:
            yaml_container = self.importer.from_yaml(self.yaml_path)
            self.assertEqual(yaml_container.source_format, DataSourceFormat.YAML)

        # Test CSV source format if available
        if self.pandas_available:
            csv_container = self.importer.from_csv_files(self.csv_dir)
            self.assertEqual(csv_container.source_format, DataSourceFormat.CSV)


class TestDictToMMCIFConverter(unittest.TestCase):
    """Test the DictToMMCIFConverter class."""

    def setUp(self):
        self.validator_factory = ValidatorFactory()
        self.converter = DictToMMCIFConverter(self.validator_factory)

        # Sample dictionary structure
        self.sample_dict = {
            "block1": {
                "_category1": {"item1": "value1", "item2": ["value2"]},
                "_category2": [
                    {"itemA": "A1", "itemB": "B1"},
                    {"itemA": "A2", "itemB": "B2"},
                ],
            }
        }

    def test_convert_simple_structure(self):
        """Test conversion of a simple dictionary structure."""
        container = self.converter.convert(self.sample_dict)

        # Verify block structure
        self.assertEqual(len(container.blocks), 1)
        self.assertIn("data_block1", container.blocks)

        block = container["block1"]

        # Verify categories
        self.assertEqual(len(block.categories), 2)
        self.assertIn("_category1", block.categories)
        self.assertIn("_category2", block.categories)

        # Verify single-row category
        cat1 = block["_category1"]
        self.assertEqual(cat1["item1"], ["value1"])
        self.assertEqual(cat1["item2"], ["value2"])

        # Verify multi-row category
        cat2 = block["_category2"]
        self.assertEqual(len(cat2["itemA"]), 2)
        self.assertEqual(cat2["itemA"][0], "A1")
        self.assertEqual(cat2["itemA"][1], "A2")
        self.assertEqual(cat2["itemB"][0], "B1")
        self.assertEqual(cat2["itemB"][1], "B2")

    def test_convert_empty_dict(self):
        """Test conversion of an empty dictionary."""
        container = self.converter.convert({})
        self.assertEqual(len(container.blocks), 0)

    def test_convert_with_missing_values(self):
        """Test conversion with missing values in multi-row categories."""
        test_dict = {
            "block1": {
                "_category": [
                    {"item1": "A", "item2": "B"},
                    {"item1": "C"},  # Missing item2
                ]
            }
        }

        container = self.converter.convert(test_dict)
        cat = container["block1"]["_category"]

        # Should have both items with empty string for missing value
        self.assertEqual(cat["item1"], ["A", "C"])
        self.assertEqual(cat["item2"], ["B", ""])

    def test_source_format_setting(self):
        """Test that source format is correctly set to DICT."""
        container = self.converter.convert(self.sample_dict)
        self.assertEqual(container.source_format, DataSourceFormat.DICT)

    def test_validator_factory_propagation(self):
        """Test that validator factory is propagated to categories."""
        container = self.converter.convert(self.sample_dict)
        cat = container["block1"]["_category1"]
        self.assertEqual(cat.validator_factory, self.validator_factory)


class TestFormatLoaders(unittest.TestCase):
    """Base class for format loader tests."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.validator_factory = ValidatorFactory()

        # Create sample data dictionary
        self.sample_data = {
            "test_block": {"_test_category": {"item1": "value1", "item2": "value2"}}
        }

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


class TestJsonLoader(TestFormatLoaders):
    """Test the JsonLoader class."""

    def setUp(self):
        super().setUp()
        self.loader = JsonLoader(self.validator_factory)

        # Create JSON test files
        self.json_file = os.path.join(self.temp_dir, "test.json")
        with open(self.json_file, "w") as f:
            json.dump(self.sample_data, f)

        # Empty file
        self.empty_json_file = os.path.join(self.temp_dir, "empty.json")
        open(self.empty_json_file, "w").close()

    def test_load_from_file(self):
        """Test loading from a JSON file."""
        container = self.loader.load(self.json_file)

        # Verify structure
        self.assertEqual(len(container.blocks), 1)
        self.assertIn("data_test_block", container.blocks)
        self.assertEqual(
            container["test_block"]["_test_category"]["item1"][0], "value1"
        )

        # Verify source format
        self.assertEqual(container.source_format, DataSourceFormat.JSON)

    def test_load_from_string(self):
        """Test loading from a JSON string."""
        json_str = json.dumps(self.sample_data)
        container = self.loader.load(json_str)

        self.assertEqual(len(container.blocks), 1)
        self.assertEqual(
            container["test_block"]["_test_category"]["item2"][0], "value2"
        )

    def test_load_empty_file(self):
        """Test loading an empty JSON file."""
        container = self.loader.load(self.empty_json_file)
        self.assertEqual(len(container.blocks), 0)

    def test_load_from_file_object(self):
        """Test loading from a file object."""
        with open(self.json_file) as f:
            container = self.loader.load(f)

        self.assertEqual(len(container.blocks), 1)
        self.assertEqual(
            container["test_block"]["_test_category"]["item1"][0], "value1"
        )

    @patch("mmap.mmap")
    def test_memory_mapping(self, mock_mmap):
        """Test memory mapping for large files."""
        # Setup mock
        mock_mmap.return_value = (
            b'{"test_block": {"_test_category": {"item1": "mapped"}}}'
        )

        # Create a large file
        large_file = os.path.join(self.temp_dir, "large.json")
        with open(large_file, "wb") as f:
            f.write(b" " * 1024 * 1024)  # 1MB file

        container = self.loader.load(large_file)
        mock_mmap.assert_called_once()
        self.assertEqual(
            container["test_block"]["_test_category"]["item1"][0], "mapped"
        )


class TestXmlLoader(TestFormatLoaders):
    """Test the XmlLoader class."""

    def setUp(self):
        super().setUp()
        self.loader = XmlLoader(self.validator_factory)

        # Create XML test files
        self.xml_file = os.path.join(self.temp_dir, "test.xml")
        with open(self.xml_file, "w") as f:
            f.write(
                """<?xml version="1.0"?>
<mmcif>
    <data_block name="test_block">
        <category name="_test_category">
            <item name="item1">value1</item>
            <item name="item2">value2</item>
        </category>
    </data_block>
</mmcif>"""
            )

        # Multi-row XML
        self.multirow_xml_file = os.path.join(self.temp_dir, "multirow.xml")
        with open(self.multirow_xml_file, "w") as f:
            f.write(
                """<?xml version="1.0"?>
<mmcif>
    <data_block name="test_block">
        <category name="_test_category">
            <row>
                <item name="item1">A1</item>
                <item name="item2">B1</item>
            </row>
            <row>
                <item name="item1">A2</item>
                <item name="item2">B2</item>
            </row>
        </category>
    </data_block>
</mmcif>"""
            )

    def test_load_from_file(self):
        """Test loading from an XML file."""
        container = self.loader.load(self.xml_file)

        # Verify structure
        self.assertEqual(len(container.blocks), 1)
        self.assertIn("data_test_block", container.blocks)
        self.assertEqual(
            container["test_block"]["_test_category"]["item1"][0], "value1"
        )

        # Verify source format
        self.assertEqual(container.source_format, DataSourceFormat.XML)

    def test_load_from_string(self):
        """Test loading from an XML string."""
        xml_str = """<?xml version="1.0"?>
<mmcif>
    <data_block name="string_block">
        <category name="_test">
            <item name="item">string_value</item>
        </category>
    </data_block>
</mmcif>"""

        container = self.loader.load(xml_str)
        self.assertEqual(len(container.blocks), 1)
        self.assertEqual(container["string_block"]["_test"]["item"][0], "string_value")

    def test_load_multi_row(self):
        """Test loading XML with multi-row categories."""
        container = self.loader.load(self.multirow_xml_file)

        cat = container["test_block"]["_test_category"]
        self.assertEqual(len(cat["item1"]), 2)
        self.assertEqual(cat["item1"][0], "A1")
        self.assertEqual(cat["item1"][1], "A2")
        self.assertEqual(cat["item2"][0], "B1")
        self.assertEqual(cat["item2"][1], "B2")

    def test_load_from_file_object(self):
        """Test loading from a file object."""
        with open(self.xml_file) as f:
            container = self.loader.load(f)

        self.assertEqual(len(container.blocks), 1)
        self.assertEqual(
            container["test_block"]["_test_category"]["item2"][0], "value2"
        )


class TestPickleLoader(TestFormatLoaders):
    """Test the PickleLoader class."""

    def setUp(self):
        super().setUp()
        self.loader = PickleLoader(self.validator_factory)

        # Create pickle test files
        self.pickle_file = os.path.join(self.temp_dir, "test.pkl")
        with open(self.pickle_file, "wb") as f:
            import pickle

            pickle.dump(self.sample_data, f)

        # Empty file
        self.empty_pickle_file = os.path.join(self.temp_dir, "empty.pkl")
        open(self.empty_pickle_file, "wb").close()

    def test_load_from_file(self):
        """Test loading from a pickle file."""
        container = self.loader.load(self.pickle_file)

        # Verify structure
        self.assertEqual(len(container.blocks), 1)
        self.assertIn("data_test_block", container.blocks)
        self.assertEqual(
            container["test_block"]["_test_category"]["item1"][0], "value1"
        )

        # Verify source format
        self.assertEqual(container.source_format, DataSourceFormat.PICKLE)

    def test_load_empty_file(self):
        """Test loading an empty pickle file."""
        with self.assertRaises((EOFError, pickle.UnpicklingError)):
            self.loader.load(self.empty_pickle_file)

    @patch("mmap.mmap")
    def test_memory_mapping(self, mock_mmap):
        """Test memory mapping for large pickle files."""
        # Setup mock
        mock_mmap.return_value = pickle.dumps(
            {"test_block": {"_test_category": {"item1": "mapped"}}}
        )

        # Create a large file
        large_file = os.path.join(self.temp_dir, "large.pkl")
        with open(large_file, "wb") as f:
            f.write(b" " * 1024 * 1024)  # 1MB file

        container = self.loader.load(large_file)
        mock_mmap.assert_called_once()
        self.assertEqual(
            container["test_block"]["_test_category"]["item1"][0], "mapped"
        )

    def test_non_string_input(self):
        """Test that non-string input raises TypeError."""
        with self.assertRaises(TypeError):
            self.loader.load(StringIO("invalid"))


class TestYamlLoader(TestFormatLoaders):
    """Test the YamlLoader class."""

    def setUp(self):
        super().setUp()
        self.loader = YamlLoader(self.validator_factory)

        # Create YAML test files
        self.yaml_file = os.path.join(self.temp_dir, "test.yaml")
        with open(self.yaml_file, "w") as f:
            yaml.dump(self.sample_data, f)

        # Empty file
        self.empty_yaml_file = os.path.join(self.temp_dir, "empty.yaml")
        open(self.empty_yaml_file, "w").close()

    def test_load_from_file(self):
        """Test loading from a YAML file."""
        try:
            container = self.loader.load(self.yaml_file)

            # Verify structure
            self.assertEqual(len(container.blocks), 1)
            self.assertIn("data_test_block", container.blocks)
            self.assertEqual(
                container["test_block"]["_test_category"]["item1"][0], "value1"
            )

            # Verify source format
            self.assertEqual(container.source_format, DataSourceFormat.YAML)
        except ImportError:
            self.skipTest("PyYAML not installed")

    def test_load_from_string(self):
        """Test loading from a YAML string."""
        try:
            yaml_str = yaml.dump({"string_block": {"_test": {"item": "string_value"}}})
            container = self.loader.load(yaml_str)

            self.assertEqual(len(container.blocks), 1)
            self.assertEqual(
                container["string_block"]["_test"]["item"][0], "string_value"
            )
        except ImportError:
            self.skipTest("PyYAML not installed")

    def test_load_empty_file(self):
        """Test loading an empty YAML file."""
        try:
            container = self.loader.load(self.empty_yaml_file)
            self.assertEqual(len(container.blocks), 0)
        except ImportError:
            self.skipTest("PyYAML not installed")

    def test_load_from_file_object(self):
        """Test loading from a file object."""
        try:
            with open(self.yaml_file) as f:
                container = self.loader.load(f)

            self.assertEqual(len(container.blocks), 1)
            self.assertEqual(
                container["test_block"]["_test_category"]["item2"][0], "value2"
            )
        except ImportError:
            self.skipTest("PyYAML not installed")

    @patch("mmap.mmap")
    def test_memory_mapping(self, mock_mmap):
        """Test memory mapping for large YAML files."""
        try:
            # Setup mock
            mock_mmap.return_value = yaml.dump(
                {"test_block": {"_test_category": {"item1": "mapped"}}}
            ).encode("utf-8")

            # Create a large file
            large_file = os.path.join(self.temp_dir, "large.yaml")
            with open(large_file, "wb") as f:
                f.write(b" " * 1024 * 1024)  # 1MB file

            container = self.loader.load(large_file)
            mock_mmap.assert_called_once()
            self.assertEqual(
                container["test_block"]["_test_category"]["item1"][0], "mapped"
            )
        except ImportError:
            self.skipTest("PyYAML not installed")


class TestCsvLoader(TestFormatLoaders):
    """Test the CsvLoader class."""

    def setUp(self):
        super().setUp()
        self.loader = CsvLoader(self.validator_factory)

        # Create CSV test directory
        self.csv_dir = os.path.join(self.temp_dir, "csv_files")
        os.makedirs(self.csv_dir, exist_ok=True)

        # Single-row category
        with open(os.path.join(self.csv_dir, "block1_category1.csv"), "w") as f:
            f.write("item1,item2\nvalue1,value2\n")

        # Multi-row category
        with open(os.path.join(self.csv_dir, "block1_category2.csv"), "w") as f:
            f.write("itemA,itemB\nA1,B1\nA2,B2\n")

    def test_load_from_directory(self):
        """Test loading from a directory of CSV files."""
        try:
            container = self.loader.load(self.csv_dir)

            # Verify structure
            self.assertEqual(len(container.blocks), 1)
            self.assertIn("data_block1", container.blocks)

            # Get the category name with underscore prefix (core functionality doesn't add prefixes)
            category_names = list(container["block1"]._categories.keys())
            self.assertTrue(any("category1" in name for name in category_names))
            self.assertTrue(any("category2" in name for name in category_names))

            # Find the category that contains our test data
            for cat_name in category_names:
                if "category1" in cat_name:
                    cat1 = container["block1"][cat_name]
                    if "item1" in cat1.items:
                        self.assertEqual(cat1["item1"][0], "value1")
                        self.assertEqual(cat1["item2"][0], "value2")
                        break

            # Find the multi-row category
            for cat_name in category_names:
                if "category2" in cat_name:
                    cat2 = container["block1"][cat_name]
            self.assertEqual(len(cat2["itemA"]), 2)
            self.assertEqual(cat2["itemA"][0], "A1")
            self.assertEqual(cat2["itemA"][1], "A2")
            self.assertEqual(cat2["itemB"][0], "B1")
            self.assertEqual(cat2["itemB"][1], "B2")

            # Verify source format
            self.assertEqual(container.source_format, DataSourceFormat.CSV)
        except ImportError:
            self.skipTest("pandas not installed")

    def test_non_string_input(self):
        """Test that non-string input raises TypeError."""
        with self.assertRaises(TypeError):
            self.loader.load(StringIO("invalid"))

    def test_empty_directory(self):
        """Test loading from an empty directory."""
        empty_dir = os.path.join(self.temp_dir, "empty_csv")
        os.makedirs(empty_dir, exist_ok=True)

        try:
            container = self.loader.load(empty_dir)
            self.assertEqual(len(container.blocks), 0)
        except ImportError:
            self.skipTest("pandas not installed")

    def test_empty_and_invalid_csv_files(self):
        """Test that CSV loader skips empty and invalid CSV files."""
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a proper CSV file
            proper_csv = os.path.join(temp_dir, "1ABC__entry.csv")
            with open(proper_csv, "w") as f:
                f.write("id\nTEST_VALUE\n")

            # Create an empty CSV file
            empty_csv = os.path.join(temp_dir, "1ABC___empty.csv")
            with open(empty_csv, "w") as f:
                f.write("")

            # Create a CSV with only headers (no data)
            headers_only_csv = os.path.join(temp_dir, "1ABC___headers.csv")
            with open(headers_only_csv, "w") as f:
                f.write("id\n")

            # Load should succeed and only include the proper file
            loader = CsvLoader()
            container = loader.load(temp_dir)

            # Should have one block with one category
            self.assertEqual(len(container.blocks), 1)
            block = container.data[0]
            self.assertEqual(len(block.categories), 1)
            self.assertIn("_entry", block.categories)

            # Verify the data
            entry_cat = block._entry
            self.assertEqual(entry_cat.id, ["TEST_VALUE"])


class TestFormatLoaderIntegration(unittest.TestCase):
    """Integration tests for format loaders through MMCIFImporter."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

        # Create sample mmCIF data for testing
        sample_cif_content = """data_test
#
_entry.id test_structure
#
_database_2.database_id      PDB
_database_2.database_code    test
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
ATOM   1    N  10.123 20.456 30.789
ATOM   2    C  11.234 21.567 31.890
ATOM   3    C  12.345 22.678 32.901
#
"""

        # Create a test CIF file
        self.test_cif_path = os.path.join(self.temp_dir, "test.cif")
        with open(self.test_cif_path, "w") as f:
            f.write(sample_cif_content)

        # Parse the test file
        handler = MMCIFHandler()
        self.mmcif = handler.parse(self.test_cif_path)
        self.exporter = MMCIFExporter(self.mmcif)
        self.importer = MMCIFImporter()

        # Export data to different formats for import testing
        self.json_path = os.path.join(self.temp_dir, "exported.json")
        self.exporter.to_json(self.json_path)

        self.xml_path = os.path.join(self.temp_dir, "exported.xml")
        self.exporter.to_xml(self.xml_path)

        self.pkl_path = os.path.join(self.temp_dir, "exported.pkl")
        self.exporter.to_pickle(self.pkl_path)

        # Create sample files in all formats for legacy compatibility
        self.sample_data = {
            "test_block": {"_test_category": {"item1": "value1", "item2": "value2"}}
        }

        # JSON
        self.json_file = os.path.join(self.temp_dir, "test.json")
        with open(self.json_file, "w") as f:
            json.dump(self.sample_data, f)

        # XML
        self.xml_file = os.path.join(self.temp_dir, "test.xml")
        with open(self.xml_file, "w") as f:
            f.write(
                """<?xml version="1.0"?>
<mmcif>
    <data_block name="test_block">
        <category name="_test_category">
            <item name="item1">value1</item>
            <item name="item2">value2</item>
        </category>
    </data_block>
</mmcif>"""
            )

        # Pickle
        self.pickle_file = os.path.join(self.temp_dir, "test.pkl")
        with open(self.pickle_file, "wb") as f:
            pickle.dump(self.sample_data, f)

        # YAML (if available)
        try:
            self.yaml_path = os.path.join(self.temp_dir, "test.yaml")
            self.exporter.to_yaml(self.yaml_path)
            self.yaml_available = True
        except ImportError:
            self.yaml_available = False

        try:
            self.yaml_file = os.path.join(self.temp_dir, "test.yaml")
            with open(self.yaml_file, "w") as f:
                yaml.dump(self.sample_data, f)
            self.yaml_available = True
        except ImportError:
            self.yaml_available = False

        # CSV (if available)
        try:
            self.csv_path = os.path.join(self.temp_dir, "csv_files")
            self.exporter.to_csv(self.csv_path)
            self.csv_available = True
            self.pandas_available = True
        except ImportError:
            self.csv_available = False
            self.pandas_available = False

        try:
            import pandas as pd

            self.csv_dir = os.path.join(self.temp_dir, "csv_files")
            os.makedirs(self.csv_dir, exist_ok=True)

            df = pd.DataFrame({"item1": ["value1"], "item2": ["value2"]})
            df.to_csv(
                os.path.join(self.csv_dir, "test_block_test_category.csv"), index=False
            )
            self.csv_available = True
        except ImportError:
            self.csv_available = False

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_json_through_importer(self):
        """Test JSON loading through MMCIFImporter."""
        container = MMCIFImporter.from_json(self.json_file)
        self.assertEqual(len(container.blocks), 1)
        self.assertEqual(
            container["test_block"]["_test_category"]["item1"][0], "value1"
        )
        self.assertEqual(container.source_format, DataSourceFormat.JSON)

    def test_xml_through_importer(self):
        """Test XML loading through MMCIFImporter."""
        container = MMCIFImporter.from_xml(self.xml_file)
        self.assertEqual(len(container.blocks), 1)
        self.assertEqual(
            container["test_block"]["_test_category"]["item1"][0], "value1"
        )
        self.assertEqual(container.source_format, DataSourceFormat.XML)

    def test_pickle_through_importer(self):
        """Test Pickle loading through MMCIFImporter."""
        container = MMCIFImporter.from_pickle(self.pickle_file)
        self.assertEqual(len(container.blocks), 1)
        self.assertEqual(
            container["test_block"]["_test_category"]["item1"][0], "value1"
        )
        self.assertEqual(container.source_format, DataSourceFormat.PICKLE)

    def test_yaml_through_importer(self):
        """Test YAML loading through MMCIFImporter."""
        if not self.yaml_available:
            self.skipTest("PyYAML not installed")

        container = MMCIFImporter.from_yaml(self.yaml_file)
        self.assertEqual(len(container.blocks), 1)
        self.assertEqual(
            container["test_block"]["_test_category"]["item1"][0], "value1"
        )
        self.assertEqual(container.source_format, DataSourceFormat.YAML)

    def test_csv_through_importer(self):
        """Test CSV loading through MMCIFImporter."""
        if not self.csv_available:
            self.skipTest("pandas not installed")

        # Create a new CSV file with a name that matches the expected pattern
        os.makedirs(self.csv_dir, exist_ok=True)
        import pandas as pd

        df = pd.DataFrame({"item1": ["value1"], "item2": ["value2"]})
        # Use the exact file name pattern used in the test
        df.to_csv(
            os.path.join(self.csv_dir, "test_block_test_category.csv"), index=False
        )

        container = MMCIFImporter.from_csv_files(self.csv_dir)
        self.assertEqual(len(container.blocks), 1)

        # Get the block name that contains our data
        block_names = container.blocks
        self.assertTrue(len(block_names) > 0, "No blocks found in container")

        # Find the block and category that contain our item
        for block_name in block_names:
            data_block = container[block_name]
            for cat_name in data_block.categories:
                category = data_block[cat_name]
                if "item1" in category.items:
                    self.assertEqual(category["item1"][0], "value1")
                    break

        self.assertEqual(container.source_format, DataSourceFormat.CSV)

    def test_auto_detect_json(self):
        """Test auto-detection of JSON format."""
        container = MMCIFImporter.auto_detect_format(self.json_file)
        self.assertEqual(container.source_format, DataSourceFormat.JSON)

    def test_auto_detect_xml(self):
        """Test auto-detection of XML format."""
        container = MMCIFImporter.auto_detect_format(self.xml_file)
        self.assertEqual(container.source_format, DataSourceFormat.XML)

    def test_auto_detect_pickle(self):
        """Test auto-detection of Pickle format."""
        container = MMCIFImporter.auto_detect_format(self.pickle_file)
        self.assertEqual(container.source_format, DataSourceFormat.PICKLE)

    def test_auto_detect_yaml(self):
        """Test auto-detection of YAML format."""
        if not self.yaml_available:
            self.skipTest("PyYAML not installed")

        container = MMCIFImporter.auto_detect_format(self.yaml_file)
        self.assertEqual(container.source_format, DataSourceFormat.YAML)

    def test_auto_detect_csv(self):
        """Test auto-detection of CSV format."""
        if not self.csv_available:
            self.skipTest("pandas not installed")

        container = MMCIFImporter.auto_detect_format(self.csv_dir)
        self.assertEqual(container.source_format, DataSourceFormat.CSV)

    def test_auto_detect_unsupported(self):
        """Test auto-detection with unsupported format."""
        unsupported_file = os.path.join(self.temp_dir, "test.unsupported")
        open(unsupported_file, "w").close()

        with self.assertRaises(ValueError):
            MMCIFImporter.auto_detect_format(unsupported_file)

    def test_handler_import_methods(self):
        """Test import methods in MMCIFHandler."""
        handler = MMCIFHandler()

        # Test JSON import
        json_container = handler.import_from_json(self.json_path)
        self.assertEqual(len(json_container.blocks), 1)
        block_name = json_container.blocks[0]  # Get the actual block name
        block = json_container[block_name]
        
        # Find the entry category
        entry_category = None
        for cat_name in block.categories:
            if 'entry' in cat_name.lower():
                entry_category = cat_name
                break
        
        self.assertIsNotNone(entry_category, f"No entry category found in {block.categories}")
        self.assertEqual(block[entry_category]["id"][0], "test_structure")

        # Test XML import
        xml_container = handler.import_from_xml(self.xml_path)
        self.assertEqual(len(xml_container.blocks), 1)
        block_name = xml_container.blocks[0]  # Get the actual block name
        block = xml_container[block_name]
        
        # Find the entry category
        entry_category = None
        for cat_name in block.categories:
            if 'entry' in cat_name.lower():
                entry_category = cat_name
                break
        
        self.assertIsNotNone(entry_category, f"No entry category found in {block.categories}")
        self.assertEqual(block[entry_category]["id"][0], "test_structure")

        # Test Pickle import
        pkl_container = handler.import_from_pickle(self.pkl_path)
        self.assertEqual(len(pkl_container.blocks), 1)
        block_name = pkl_container.blocks[0]  # Get the actual block name
        block = pkl_container[block_name]
        
        # Find the entry category
        entry_category = None
        for cat_name in block.categories:
            if 'entry' in cat_name.lower():
                entry_category = cat_name
                break
        
        self.assertIsNotNone(entry_category, f"No entry category found in {block.categories}")
        self.assertEqual(block[entry_category]["id"][0], "test_structure")

        # Test auto-detection
        auto_container = handler.import_auto_detect(self.json_path)
        self.assertEqual(len(auto_container.blocks), 1)
        block_name = auto_container.blocks[0]  # Get the actual block name
        block = auto_container[block_name]
        
        # Find the entry category
        entry_category = None
        for cat_name in block.categories:
            if 'entry' in cat_name.lower():
                entry_category = cat_name
                break
        
        self.assertIsNotNone(entry_category, f"No entry category found in {block.categories}")
        self.assertEqual(block[entry_category]["id"][0], "test_structure")

    def test_round_trip_json(self):
        """Test round-trip export-import using JSON."""
        # Export to JSON
        json_path = os.path.join(self.temp_dir, "round_trip.json")
        self.exporter.to_json(json_path)

        # Import from JSON
        imported_container = self.importer.from_json(json_path)

        # Compare original and imported
        self.assertEqual(len(self.mmcif.blocks), len(imported_container.blocks))
        self.assertEqual(
            self.mmcif["test"]["_entry"]["id"][0],
            imported_container["test"]["_entry"]["id"][0],
        )
        self.assertEqual(
            self.mmcif["test"]["_atom_site"]["Cartn_x"][0],
            imported_container["test"]["_atom_site"]["Cartn_x"][0],
        )

    def test_round_trip_xml(self):
        """Test round-trip export-import using XML."""
        # Export to XML
        xml_path = os.path.join(self.temp_dir, "round_trip.xml")
        self.exporter.to_xml(xml_path)

        # Import from XML
        imported_container = self.importer.from_xml(xml_path)

        # Compare original and imported
        self.assertEqual(len(self.mmcif.blocks), len(imported_container.blocks))
        self.assertEqual(
            self.mmcif["test"]["_entry"]["id"][0],
            imported_container["test"]["_entry"]["id"][0],
        )
        self.assertEqual(
            self.mmcif["test"]["_atom_site"]["Cartn_x"][0],
            imported_container["test"]["_atom_site"]["Cartn_x"][0],
        )

    def test_round_trip_pickle(self):
        """Test round-trip export-import using Pickle."""
        # Export to Pickle
        pkl_path = os.path.join(self.temp_dir, "round_trip.pkl")
        self.exporter.to_pickle(pkl_path)

        # Import from Pickle
        imported_container = self.importer.from_pickle(pkl_path)

        # Compare original and imported
        self.assertEqual(len(self.mmcif.blocks), len(imported_container.blocks))
        self.assertEqual(
            self.mmcif["test"]["_entry"]["id"][0],
            imported_container["test"]["_entry"]["id"][0],
        )
        self.assertEqual(
            self.mmcif["test"]["_atom_site"]["Cartn_x"][0],
            imported_container["test"]["_atom_site"]["Cartn_x"][0],
        )

    def test_source_format_flag(self):
        """Test that the source format flag is correctly set."""
        # Test JSON source format
        json_container = self.importer.from_json(self.json_path)
        self.assertEqual(json_container.source_format, DataSourceFormat.JSON)

        # Test XML source format
        xml_container = self.importer.from_xml(self.xml_path)
        self.assertEqual(xml_container.source_format, DataSourceFormat.XML)

        # Test Pickle source format
        pickle_container = self.importer.from_pickle(self.pkl_path)
        self.assertEqual(pickle_container.source_format, DataSourceFormat.PICKLE)

        # Test CIF source format via auto-detect
        cif_container = MMCIFImporter.auto_detect_format(self.test_cif_path)
        self.assertEqual(cif_container.source_format, DataSourceFormat.MMCIF)

        # Test dictionary source format
        data_dict = self.exporter.to_dict()
        dict_container = MMCIFImporter.from_dict(data_dict)
        self.assertEqual(dict_container.source_format, DataSourceFormat.DICT)

        # Test YAML source format if available
        if self.yaml_available:
            yaml_container = self.importer.from_yaml(self.yaml_path)
            self.assertEqual(yaml_container.source_format, DataSourceFormat.YAML)

        # Test CSV source format if available
        if self.pandas_available:
            csv_container = self.importer.from_csv_files(self.csv_dir)
            self.assertEqual(csv_container.source_format, DataSourceFormat.CSV)


class TestGemmiWrapper(unittest.TestCase):
    """Test cases for GemmiWrapper functionality through MMCIFHandler"""

    def setUp(self):
        """Set up test data"""
        self.sample_content = """data_1ABC
_entry.id 1ABC_STRUCTURE
_database_2.database_id PDB
_database_2.database_code 1ABC
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
ATOM 1 N 10.123 20.456 30.789
ATOM 2 C 11.234 21.567 31.890
"""

    def test_gemmi_wrapper_import(self):
        """Test that GemmiWrapper can be imported"""
        try:
            from sloth import GemmiWrapper
            self.assertTrue(True)
        except ImportError:
            self.fail("GemmiWrapper could not be imported")

    def test_gemmi_handler_use_gemmi_param(self):
        """Test MMCIFHandler with use_gemmi=True parameter"""
        try:
            handler = MMCIFHandler(use_gemmi=True)
            self.assertTrue(handler.use_gemmi)
            self.assertIsNotNone(handler._gemmi_wrapper)
        except ImportError:
            self.skipTest("gemmi not available")

    def test_gemmi_vs_regular_parsing(self):
        """Test that use_gemmi=True produces same results as regular parsing"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as f:
            f.write(self.sample_content)
            temp_file = f.name

        try:
            # Parse with regular handler
            regular_handler = MMCIFHandler()
            regular_mmcif = regular_handler.parse(temp_file)

            try:
                # Parse with gemmi handler
                gemmi_handler = MMCIFHandler(use_gemmi=True)
                gemmi_mmcif = gemmi_handler.parse(temp_file)

                # Compare results
                self.assertEqual(len(regular_mmcif.data), len(gemmi_mmcif.data))
                
                # Compare first block
                regular_block = regular_mmcif.data[0]
                gemmi_block = gemmi_mmcif.data[0]
                
                self.assertEqual(regular_block.name, gemmi_block.name)
                self.assertEqual(len(regular_block.categories), len(gemmi_block.categories))
                
                # Compare specific data
                self.assertEqual(
                    regular_mmcif.data_1ABC._entry.id[0],
                    gemmi_mmcif.data_1ABC._entry.id[0]
                )
                
                self.assertEqual(
                    len(regular_mmcif.data_1ABC._atom_site.Cartn_x),
                    len(gemmi_mmcif.data_1ABC._atom_site.Cartn_x)
                )

            except ImportError:
                self.skipTest("gemmi not available")

        finally:
            os.unlink(temp_file)

    def test_gemmi_write_functionality(self):
        """Test MMCIFHandler write functionality with use_gemmi=True"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as f:
            f.write(self.sample_content)
            temp_file = f.name

        try:
            try:
                handler = MMCIFHandler(use_gemmi=True)
                mmcif = handler.parse(temp_file)

                # Test writing
                with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as f:
                    output_file = f.name

                with open(output_file, 'w') as f:
                    handler.file_obj = f
                    handler.write(mmcif)

                # Verify the written file can be parsed back
                mmcif_roundtrip = handler.parse(output_file)
                self.assertEqual(
                    mmcif.data_1ABC._entry.id[0],
                    mmcif_roundtrip.data_1ABC._entry.id[0]
                )

                os.unlink(output_file)

            except ImportError:
                self.skipTest("gemmi not available")

        finally:
            os.unlink(temp_file)

    def test_gemmi_export_methods(self):
        """Test that all export methods work with use_gemmi=True"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.cif', delete=False) as f:
            f.write(self.sample_content)
            temp_file = f.name

        try:
            try:
                handler = MMCIFHandler(use_gemmi=True)
                mmcif = handler.parse(temp_file)

                # Test JSON export
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json_file = f.name
                
                handler.export_to_json(mmcif, json_file)
                self.assertTrue(os.path.exists(json_file))
                
                # Test import back
                imported_mmcif = handler.import_from_json(json_file)
                self.assertEqual(
                    mmcif.data_1ABC._entry.id[0],
                    imported_mmcif.data_1ABC._entry.id[0]
                )

                os.unlink(json_file)

            except ImportError:
                self.skipTest("gemmi not available")

        finally:
            os.unlink(temp_file)

    def test_gemmi_disabled_by_default(self):
        """Test that use_gemmi=False is the default"""
        handler = MMCIFHandler()
        self.assertFalse(handler.use_gemmi)
        self.assertIsNone(handler._gemmi_wrapper)


if __name__ == "__main__":
    unittest.main()
