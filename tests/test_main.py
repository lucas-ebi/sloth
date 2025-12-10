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
from io import StringIO
from unittest.mock import mock_open, patch
from sloth.mmcif import (
    MMCIFHandler,
    MMCIFWriter,
    JSONExporter,
    JSONImporter,
    MMCIFDataContainer,
    DataBlock,
    Category,
    Row,
    Item,
    ValidatorFactory,
    DataSourceFormat,
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
            mmcif = self.handler.read(temp_file)
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
            mmcif = self.handler.read(temp_file, categories=["_database_2"])
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
        # Check that the complete content was written in one call
        expected_content = "data_7XJP\n_database_2.database_id PDB\n_database_2.database_code 7XJP\n"
        mock_file().write.assert_called_with(expected_content)


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
            mmcif = self.handler.read(temp_file, categories=["_database_2"])
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
        # Provide filename parameter to the write method
        self.handler.write(mmcif, "dummy.cif")
        # Check that the complete content was written in one call
        expected_content = "data_7XJP\n_database_2.database_id PDB\n_database_2.database_code 7XJP\n"
        mock_file().write.assert_called_with(expected_content)


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
ATOM   1    N  N   1  20.154 6.718  22.746  1.00  25.00
ATOM   2    C  CA  2  21.618 6.756  22.530  1.00  26.00
ATOM   3    C  C   3  22.097 8.130  22.050  1.00  27.00
ATOM   4    O  O   4  21.346 8.963  21.523  1.00  28.00
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
        mmcif = handler.read(self.temp_file.name)

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
        data1 = handler.read(self.temp_file.name)
        data2 = handler.read(self.temp_file.name)

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
            large_content += f"ATOM   {i+5}    C  CB  {i+5}  {20.0+i} {6.0+i}  {22.0+i}  1.00  {25.0+i}\n"
        large_content += "#\n"

        # Write to temp file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".cif", delete=False) as f:
            f.write(large_content)
            large_file = f.name

        try:
            # Time the parsing
            start_time = time.time()
            handler = MMCIFHandler()
            data = handler.read(large_file)
            parse_time = time.time() - start_time
            
            # Assert parsing completed in reasonable time (under 10 seconds for large files)
            self.assertLess(parse_time, 10.0, f"Parsing took too long: {parse_time:.4f}s")

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
            data = handler.read(large_file)
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
            data = handler.read(memory_file)

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
_database_2.database_id      PDB
_database_2.database_code    ABC123
#
loop_
_atom_type.symbol
N
C
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
_atom_site.B_iso_or_equiv
ATOM   1    N  10.123 20.456 30.789 25.0
ATOM   2    C  11.234 21.567 31.890 30.0
ATOM   3    C  12.345 22.678 32.901 35.0
#
"""
            )

        # Parse the test file
        self.handler = MMCIFHandler(validator_factory=None)  # Use permissive mode for exports
        self.mmcif = self.handler.read(self.test_cif_path)

    def tearDown(self):
        """Tear down test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_json_export_to_string(self):
        """Test JSON export to string."""
        json_str = self.handler.export(self.mmcif, format_type='json')
        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        
        # Verify the structure - JSON export uses external API naming with prefixes
        self.assertIn("data_test", data)  # Block name with data_ prefix
        block_data = data["data_test"]
        self.assertIn("_entry", block_data)  # Category name with _ prefix
        self.assertIn("_atom_site", block_data)
        
        # Verify specific values - _entry is now a list in nested structure
        self.assertIsInstance(block_data["_entry"], list)
        self.assertEqual(block_data["_entry"][0]["id"], "test_structure")
        
        # Verify multi-row category
        atom_site = block_data["_atom_site"]
        self.assertEqual(len(atom_site), 3)  # Three rows
        self.assertEqual(atom_site[0]["Cartn_x"], "10.123")

    def test_json_export_to_file(self):
        """Test JSON export to file."""
        json_path = os.path.join(self.temp_dir, "test.json")
        self.handler.export(self.mmcif, format_type='json', file_path=json_path)

        # Verify file exists
        self.assertTrue(os.path.exists(json_path))

        # Verify content
        with open(json_path) as f:
            data = json.load(f)

        self.assertIn("data_test", data)  # Block name with data_ prefix
        block_data = data["data_test"]
        self.assertIn("_database_2", block_data)  # Category name with _ prefix
        # In nested structure, _database_2 is a list of objects
        self.assertIsInstance(block_data["_database_2"], list)
        self.assertEqual(block_data["_database_2"][0]["database_id"], "PDB")

    def test_unsupported_format_error(self):
        """Test that unsupported formats raise appropriate errors."""
        with self.assertRaises(ValueError):
            self.handler.export(self.mmcif, format_type='unsupported')


class TestImportFunctionality(unittest.TestCase):
    """Test case for import functionality through JSONImporter and XMLImporter."""

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
_database_2.database_id      PDB
_database_2.database_code    ABC123
#
loop_
_atom_type.symbol
N
C
#
loop_
_atom_site.group_PDB
_atom_site.id
_atom_site.type_symbol
_atom_site.Cartn_x
_atom_site.Cartn_y
_atom_site.Cartn_z
_atom_site.B_iso_or_equiv
ATOM   1    N  10.123 20.456 30.789 25.0
ATOM   2    C  11.234 21.567 31.890 30.0
ATOM   3    C  12.345 22.678 32.901 35.0
#
"""
            )

        # Parse the test file and export to JSON for testing imports
        handler = MMCIFHandler()
        self.mmcif = handler.read(self.test_cif_path)

        # Export data to JSON for import testing
        self.json_path = os.path.join(self.temp_dir, "test.json")
        handler.export(self.mmcif, format_type='json', file_path=self.json_path)

    def tearDown(self):
        """Tear down test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_json_import_from_file(self):
        """Test importing from JSON file."""
        importer = JSONImporter()
        imported_container = importer.import_data(self.json_path)

        # Verify structure and content - the imported data should have block structure
        self.assertTrue(len(imported_container.blocks) > 0)
        
        # Get the first block by iterating over the container
        first_block = None
        for block in imported_container:
            first_block = block
            break
        
        self.assertIsNotNone(first_block)
        self.assertIn("_entry", first_block.categories)
        self.assertEqual(first_block["_entry"]["id"], ["test_structure"])
        
        self.assertIn("_atom_site", first_block.categories)
        atom_site = first_block["_atom_site"]
        self.assertEqual(atom_site["Cartn_x"][0], "10.123")

    def test_json_import_from_string(self):
        """Test importing from JSON string."""
        with open(self.json_path, "r") as f:
            json_str = f.read()

        importer = JSONImporter()
        imported_container = importer.import_data(json_str)
        
        # Get the first block by iterating over the container
        self.assertTrue(len(imported_container.blocks) > 0)
        
        first_block = None
        for block in imported_container:
            first_block = block
            break
        
        self.assertIsNotNone(first_block)
        self.assertEqual(first_block["_entry"]["id"], ["test_structure"])

    def test_round_trip_json(self):
        """Test round-trip conversion: mmCIF -> JSON -> mmCIF."""
        handler = MMCIFHandler()
        
        # Export to JSON string
        json_str = handler.export(self.mmcif, format_type='json')
        
        # Import back from JSON
        importer = JSONImporter()
        imported_mmcif = importer.import_data(json_str)
        
        # Verify the data matches - use correct block iteration
        # Get first block from original by iterating over container
        original_block = None
        for block in self.mmcif:
            original_block = block
            break
        
        # Get first block from imported by iterating over container
        imported_block = None
        for block in imported_mmcif:
            imported_block = block
            break
        
        self.assertIsNotNone(original_block)
        self.assertIsNotNone(imported_block)
        self.assertEqual(original_block["_entry"]["id"], imported_block["_entry"]["id"])


class TestValidationModes(unittest.TestCase):
    """Test permissive vs compliant validation modes."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

        # Create test mmCIF content with potentially invalid data
        self.test_cif_path = os.path.join(self.temp_dir, "test.cif")
        with open(self.test_cif_path, "w") as f:
            f.write(
                """data_test
#
_entry.id test_structure
#
_database_2.database_id      PDB
_database_2.database_code    ABC123
#
_custom_category.custom_item custom_value
#
"""
            )

    def tearDown(self):
        """Tear down test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_permissive_mode(self):
        """Test that permissive mode allows flexible parsing."""
        # Handler without validator should be permissive
        handler = MMCIFHandler(validator_factory=None)
        mmcif = handler.read(self.test_cif_path)
        
        # Should parse successfully
        self.assertIn("data_test", mmcif.blocks)
        self.assertIn("_custom_category", mmcif["data_test"].categories)
        self.assertEqual(mmcif["data_test"]["_custom_category"]["custom_item"], ["custom_value"])

    def test_compliant_mode_with_validator(self):
        """Test that compliant mode uses validation when available."""
        # Handler with validator factory should be compliant
        validator_factory = ValidatorFactory()
        handler = MMCIFHandler(validator_factory=validator_factory)
        mmcif = handler.read(self.test_cif_path)
        
        # Should still parse but might validate
        self.assertIn("data_test", mmcif.blocks)
        self.assertIn("_custom_category", mmcif["data_test"].categories)

    def test_export_validation_modes(self):
        """Test export works in both validation modes."""
        # Test permissive export
        handler_permissive = MMCIFHandler(validator_factory=None)
        mmcif = handler_permissive.read(self.test_cif_path)
        
        json_str = handler_permissive.export(mmcif, format_type='json')
        self.assertIsInstance(json_str, str)
        self.assertIn('"_entry"', json_str)
        
        # Test compliant export with permissive flag
        handler_compliant = MMCIFHandler(validator_factory=ValidatorFactory())
        mmcif = handler_compliant.read(self.test_cif_path)
        
        json_str = handler_compliant.export(mmcif, format_type='json')
        self.assertIsInstance(json_str, str)
        self.assertIn('"_entry"', json_str)

    def test_strict_vs_permissive_export(self):
        """Test export works with standard data."""
        # Create handler with valid data
        test_cif_path = os.path.join(self.temp_dir, "valid.cif")
        with open(test_cif_path, "w") as f:
            f.write(
                """data_test
#
_entry.id test_structure
#
_custom_category.custom_item custom_value
#
"""
            )
        
        handler = MMCIFHandler()
        mmcif = handler.read(test_cif_path)
        
        # Export should work
        json_str = handler.export(mmcif, format_type='json')
        self.assertIsInstance(json_str, str)


class TestDataStructureIntegrity(unittest.TestCase):
    """Test data structure integrity and edge cases."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_empty_categories(self):
        """Test handling of empty categories."""
        cif_content = """data_test
#
_empty_category.item1 ?
_empty_category.item2 .
#
"""
        cif_path = os.path.join(self.temp_dir, "empty.cif")
        with open(cif_path, "w") as f:
            f.write(cif_content)
            
        handler = MMCIFHandler()
        mmcif = handler.read(cif_path)
        
        self.assertIn("data_test", mmcif.blocks)
        self.assertIn("_empty_category", mmcif["data_test"].categories)
        
        # Check that missing values are handled properly
        category = mmcif["data_test"]["_empty_category"]
        self.assertEqual(category["item1"], ["?"])
        self.assertEqual(category["item2"], ["."])

    def test_special_characters(self):
        """Test handling of special characters in data."""
        cif_content = """data_test
#
_special.quoted_item 'This is a quoted string'
_special.multiline_item
;This is a
multiline string
;
#
"""
        cif_path = os.path.join(self.temp_dir, "special.cif")
        with open(cif_path, "w") as f:
            f.write(cif_content)
            
        handler = MMCIFHandler()
        mmcif = handler.read(cif_path)
        
        category = mmcif["data_test"]["_special"]
        # The quotes are preserved in the current implementation
        self.assertEqual(category["quoted_item"], ["'This is a quoted string'"])
        self.assertIn("This is a\nmultiline string", category["multiline_item"][0])

    def test_large_datasets(self):
        """Test handling of moderately large datasets."""
        # Create a CIF with many rows
        cif_content = "data_test\n#\nloop_\n_atom_site.id\n_atom_site.x\n_atom_site.y\n_atom_site.z\n"
        for i in range(1000):
            cif_content += f"{i} {i*0.1:.3f} {i*0.2:.3f} {i*0.3:.3f}\n"
        cif_content += "#\n"
        
        cif_path = os.path.join(self.temp_dir, "large.cif")
        with open(cif_path, "w") as f:
            f.write(cif_content)
            
        handler = MMCIFHandler()
        mmcif = handler.read(cif_path)
        
        atom_site = mmcif["data_test"]["_atom_site"]
        self.assertEqual(len(atom_site["id"]), 1000)
        self.assertEqual(atom_site["id"][0], "0")
        self.assertEqual(atom_site["id"][999], "999")


class TestDefaultBackend(unittest.TestCase):
    """
    Test the default backend functionality.

    The default backend should parse mmCIF files using the new
    optimized API while maintaining backwards compatibility.
    """

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Tear down test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_default_handler_creation(self):
        """Test that the default handler can be created without parameters."""
        handler = MMCIFHandler()
        self.assertIsInstance(handler, MMCIFHandler)
        # Default should be permissive (no validator)
        self.assertIsNone(handler.validator_factory)

    def test_default_parsing_behavior(self):
        """Test default parsing behavior is optimized and permissive."""
        cif_content = """data_test
#
_entry.id test_structure
_database_2.database_id PDB
_database_2.database_code 1ABC
#
"""
        cif_path = os.path.join(self.temp_dir, "test.cif")
        with open(cif_path, "w") as f:
            f.write(cif_content)

        handler = MMCIFHandler()
        mmcif = handler.read(cif_path)

        # Verify structure
        self.assertIn("data_test", mmcif.blocks)
        data_block = mmcif["data_test"]

        # Verify data
        self.assertEqual(data_block["_entry"]["id"], ["test_structure"])
        self.assertEqual(data_block["_database_2"]["database_id"], ["PDB"])
        self.assertEqual(data_block["_database_2"]["database_code"], ["1ABC"])

    def test_default_writing_behavior(self):
        """Test default writing behavior."""
        # Create a simple data structure
        data_block = DataBlock(
            name="test",
            categories={
                "_entry": Category(name="_entry", validator_factory=None)
            },
        )
        data_block["_entry"]._add_item_value("id", "test_structure")
        data_block["_entry"]._commit_all_batches()
        mmcif = MMCIFDataContainer(data_blocks={"test": data_block})

        # Write using default handler
        handler = MMCIFHandler()
        output_path = os.path.join(self.temp_dir, "output.cif")
        
        # Use the filename parameter for convenience
        handler.write(mmcif, filename=output_path)

        # Verify file was created and contains expected content
        self.assertTrue(os.path.exists(output_path))
        with open(output_path, "r") as f:
            content = f.read()
        
        self.assertIn("data_test", content)
        self.assertIn("_entry.id test_structure", content)

    def test_backwards_compatibility(self):
        """Test that the new API maintains backwards compatibility concepts."""
        handler = MMCIFHandler()
        
        # Should be able to handle typical use cases
        cif_content = """data_test
#
_audit_conform.dict_name mmcif_pdbx.dic
_audit_conform.dict_version 5.281
#
_entry.id TEST
#
"""
        cif_path = os.path.join(self.temp_dir, "compat.cif")
        with open(cif_path, "w") as f:
            f.write(cif_content)

        mmcif = handler.read(cif_path)
        self.assertIn("data_test", mmcif.blocks)
        self.assertIn("_audit_conform", mmcif["data_test"].categories)
        self.assertIn("_entry", mmcif["data_test"].categories)


if __name__ == "__main__":
    unittest.main()
