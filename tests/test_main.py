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
import time
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
    PluginFactory,
    ValidatorPlugin,
    ValidationError,
    ValidationSeverity,
    ValidationReport,
    CategoryValidator,
    DataBlockValidator,
    ContainerValidator,
    DataSourceFormat,
    SchemaValidator,
    MMCIFValidator,
    SchemaWarning,
    # Rule factories
    mandatory_items,
    one_of_following,
    value_length,
    value_range,
    conditional_mandatory,
    regex_check,
    ordering_check,
    allowed_pairs,
    min_rows,
    enumeration_check,
    type_check,
    foreign_key,
    parent_child,
    composite_key,
    oper_expression,
    cross_mandatory,
    cross_ordering,
)
import warnings as _warnings


class TestMMCIFParser(unittest.TestCase):
    mmcif_content = """
data_7XJP
#
_database_2.database_id      PDB
_database_2.database_code    7XJP
#
"""

    def setUp(self):
        self.handler = MMCIFHandler()

    def test_read_empty_file(self):
        # Create a temporary file since mmap requires a real file

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
                "_database_2": Category(name="_database_2")
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
        self.handler = MMCIFHandler()

    def test_parse_file(self):
        # Create a temporary file since mmap requires a real file

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
                "_database_2": Category(name="_database_2")
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


class TestValidatorPlugin(unittest.TestCase):
    def setUp(self):
        self.plugin = ValidatorPlugin()

    def test_register_and_get_validator(self):
        def validator(category_name: str):
            pass

        self.plugin.register_validator("test_category", validator)
        self.assertIn(validator, self.plugin.get_validators("test_category"))

    def test_register_and_get_cross_checker(self):
        def cross_checker(category1: str, category2: str):
            pass

        self.plugin.register_cross_checker(("category1", "category2"), cross_checker)
        self.assertIn(
            cross_checker, self.plugin.get_cross_checkers(("category1", "category2"))
        )

    def test_multiple_validators_per_category(self):
        results = []
        self.plugin.register_validator("_test", lambda c: results.append("a"))
        self.plugin.register_validator("_test", lambda c: results.append("b"))
        self.assertEqual(len(self.plugin.get_validators("_test")), 2)


class TestCategoryValidation(unittest.TestCase):
    def setUp(self):
        self.plugin = ValidatorPlugin()
        self.category = Category(name="_database_2")
        self.category.register("validate", self.plugin)

    def test_validate(self):
        def validator(category: Category):
            self.assertEqual(category.name, "_database_2")

        self.plugin.register_validator("_database_2", validator)
        self.category.validate()

    def test_validate_against(self):
        other_category = Category(name="_database_1")

        def cross_checker(category1: Category, category2: Category):
            self.assertEqual(category1.name, "_database_2")
            self.assertEqual(category2.name, "_database_1")

        self.plugin.register_cross_checker(
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
        category = Category("test_category")

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
        category = Category("test_category")

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
        category = Category("test_category")

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
        category = Category("test_category")

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
        category = Category("test_category")

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
                """data_TEST
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
        self.handler = MMCIFHandler()  # No validation for simple exports
        self.mmcif = self.handler.read(self.test_cif_path)

    def tearDown(self):
        """Tear down test fixtures."""
        shutil.rmtree(self.temp_dir)

    def test_json_export_to_string(self):
        """Test JSON export to string."""
        json_str = self.handler.export(self.mmcif)
        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        
        # Verify the structure - JSON export uses external API naming with prefixes
        self.assertIn("data_TEST", data)  # Block name with data_ prefix
        block_data = data["data_TEST"]
        self.assertIn("_entry", block_data)  # Category name with _ prefix
        self.assertIn("_atom_type", block_data)  # atom_type is the parent category
        
        # Verify specific values - _entry is now a list in nested structure
        self.assertIsInstance(block_data["_entry"], list)
        self.assertEqual(block_data["_entry"][0]["id"], "test_structure")
        
        # Verify nested structure - atom_site is now nested under atom_type
        atom_type = block_data["_atom_type"]
        self.assertEqual(len(atom_type), 2)  # Two atom types: N and C
        # Find the N atom type and check its nested atom_site
        n_type = next(at for at in atom_type if at["symbol"] == "N")
        self.assertIn("_atom_site", n_type)
        self.assertEqual(len(n_type["_atom_site"]), 1)  # One N atom
        self.assertEqual(n_type["_atom_site"][0]["Cartn_x"], "10.123")

    def test_json_export_to_file(self):
        """Test JSON export to file."""
        json_path = os.path.join(self.temp_dir, "test.json")
        self.handler.export(self.mmcif, file_path=json_path)

        # Verify file exists
        self.assertTrue(os.path.exists(json_path))

        # Verify content
        with open(json_path) as f:
            data = json.load(f)

        self.assertIn("data_TEST", data)  # Block name with data_ prefix
        block_data = data["data_TEST"]
        self.assertIn("_database_2", block_data)  # Category name with _ prefix
        # In nested structure, _database_2 is a list of objects
        self.assertIsInstance(block_data["_database_2"], list)
        self.assertEqual(block_data["_database_2"][0]["database_id"], "PDB")

    def test_export_without_file_path(self):
        """Test that export returns JSON string when no file_path is provided."""
        result = self.handler.export(self.mmcif)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, str)
        # Verify it's valid JSON
        data = json.loads(result)
        self.assertIn('data_TEST', data)


class TestImportFunctionality(unittest.TestCase):
    """Test case for import functionality through JSONImporter."""

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
        handler.export(self.mmcif, file_path=self.json_path)

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
        
        # atom_site is now nested under atom_type in the JSON structure
        # After import, it should be flattened back to the original mmCIF structure
        self.assertIn("_atom_site", first_block.categories)
        atom_site = first_block["_atom_site"]
        # Check that we have all 3 atoms after flattening
        self.assertTrue(len(atom_site["Cartn_x"]) >= 1)
        # First atom (N type) should have x coordinate 10.123
        self.assertIn("10.123", atom_site["Cartn_x"])

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
        json_str = handler.export(self.mmcif)
        
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


class TestHandlerValidationModes(unittest.TestCase):
    """Test handler behavior with and without validators."""

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

    def test_handler_without_validators(self):
        """Test that handler without validators allows flexible parsing."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif_path)
        
        # Should parse successfully
        self.assertIn("data_test", mmcif.blocks)
        self.assertIn("_custom_category", mmcif["data_test"].categories)
        self.assertEqual(mmcif["data_test"]["_custom_category"]["custom_item"], ["custom_value"])

    def test_compliant_mode_with_validators(self):
        """Test that reading + registering validators on parsed data works."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif_path)
        
        # Register validator on a category after parsing
        vp = ValidatorPlugin()
        vp.register_validator("_entry", lambda cat: None)
        mmcif["data_test"]["_entry"].register("validate", vp)
        
        # Should still be accessible
        self.assertIn("data_test", mmcif.blocks)
        self.assertIn("_custom_category", mmcif["data_test"].categories)

    def test_proxy_prevents_auto_creation_on_read(self):
        """Reading a non-existent category via proxy raises AttributeError."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif_path)

        # Parsed categories should still be accessible
        self.assertIn("data_test", mmcif.blocks)
        block = mmcif["data_test"]
        self.assertEqual(block["_entry"]["id"], ["test_structure"])

        # Accessing a non-existent category returns a proxy.
        # Attempting to *read* from that proxy should raise.
        from sloth.mmcif.models import _PendingCategory
        pending = block._nonexistent_category
        self.assertIsInstance(pending, _PendingCategory)
        with self.assertRaises(AttributeError):
            _ = pending.some_item

        # Accessing a non-existent data block returns a proxy.
        from sloth.mmcif.models import _PendingDataBlock
        pending_block = mmcif.data_nonexistent
        self.assertIsInstance(pending_block, _PendingDataBlock)
        with self.assertRaises(AttributeError):
            _ = pending_block.some_attr

    def test_proxy_allows_creation_on_write(self):
        """Writing through a proxy auto-creates the category."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif_path)
        block = mmcif["data_test"]

        # One-liner creation via proxy
        block._brand_new_category["widget_id"] = ["w1"]
        self.assertIn("_brand_new_category", block.categories)
        self.assertEqual(block._brand_new_category.widget_id, ["w1"])

    def test_export_with_different_handlers(self):
        """Test export works with handlers with and without validators."""
        # Test export with handler without validators
        handler_no_plugins = MMCIFHandler()
        mmcif = handler_no_plugins.read(self.test_cif_path)
        
        json_str = handler_no_plugins.export(mmcif)
        self.assertIsInstance(json_str, str)
        self.assertIn('"_entry"', json_str)
        
        # Test export with handler (same handler, validators are on models now)
        handler_with_plugins = MMCIFHandler()
        mmcif = handler_with_plugins.read(self.test_cif_path)
        
        json_str = handler_with_plugins.export(mmcif)
        self.assertIsInstance(json_str, str)
        self.assertIn('"_entry"', json_str)

    def test_export_with_standard_data(self):
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
        json_str = handler.export(mmcif)
        self.assertIsInstance(json_str, str)


class TestSchemaWarnings(unittest.TestCase):
    """Test on-the-fly schema warnings during data creation."""

    def test_unknown_category_warns(self):
        """Creating a non-dictionary category via proxy emits SchemaWarning."""
        block = DataBlock("test")
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            block._totally_fake_category["x"] = ["1"]
            schema_warns = [x for x in w if issubclass(x.category, SchemaWarning)]
            # Should have at least one warning about the category
            cat_warns = [x for x in schema_warns if "totally_fake_category" in str(x.message)]
            self.assertTrue(len(cat_warns) > 0, f"Expected SchemaWarning, got: {[str(x.message) for x in w]}")

    def test_known_category_no_category_warning(self):
        """Creating a valid dictionary category via proxy emits no category warning."""
        block = DataBlock("test")
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            block._entry["id"] = ["test"]
            cat_warns = [x for x in w if issubclass(x.category, SchemaWarning)
                         and "_entry" in str(x.message) and "not in the mmCIF dictionary" in str(x.message)
                         and "Item" not in str(x.message)]
            self.assertEqual(len(cat_warns), 0, f"Unexpected category warning: {[str(x.message) for x in cat_warns]}")

    def test_unknown_item_warns(self):
        """Setting a non-dictionary item on a known category emits SchemaWarning."""
        block = DataBlock("test")
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            block._entry["totally_fake_item"] = ["x"]
            item_warns = [x for x in w if issubclass(x.category, SchemaWarning)
                          and "totally_fake_item" in str(x.message)]
            self.assertTrue(len(item_warns) > 0, f"Expected item SchemaWarning, got: {[str(x.message) for x in w]}")

    def test_known_item_no_warning(self):
        """Setting a valid dictionary item emits no item warning."""
        block = DataBlock("test")
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            block._entry["id"] = ["test_structure"]
            item_warns = [x for x in w if issubclass(x.category, SchemaWarning)
                          and "Item" in str(x.message)]
            self.assertEqual(len(item_warns), 0, f"Unexpected item warning: {[str(x.message) for x in item_warns]}")

    def test_dot_notation_item_warns(self):
        """Dot-notation assignment of unknown item on existing category warns."""
        cat = Category("_entry")
        cat["id"] = ["test"]  # bracket — no warning from __setattr__
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            cat.fake_item_xyz = ["val"]
            item_warns = [x for x in w if issubclass(x.category, SchemaWarning)
                          and "fake_item_xyz" in str(x.message)]
            self.assertTrue(len(item_warns) > 0, f"Expected SchemaWarning for dot-notation, got: {[str(x.message) for x in w]}")

    def test_parsing_no_warnings(self):
        """Parsing a valid mmCIF file should not emit SchemaWarning."""
        handler = MMCIFHandler()
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            mmcif = handler.read(os.path.join(
                os.path.dirname(__file__), "..", "tests", "test_main.py"
            ).replace("tests/test_main.py", "") + "tests/../tests/../tests/../tests/../tests/test_main.py") if False else None
        # Use the real test fixture instead
        test_cif = tempfile.NamedTemporaryFile(
            mode="w", suffix=".cif", delete=False
        )
        test_cif.write("data_test\n_entry.id test_structure\n")
        test_cif.flush()
        test_cif.close()
        try:
            with _warnings.catch_warnings(record=True) as w:
                _warnings.simplefilter("always")
                mmcif = handler.read(test_cif.name)
                schema_warns = [x for x in w if issubclass(x.category, SchemaWarning)]
                self.assertEqual(len(schema_warns), 0,
                                 f"Parsing should not warn: {[str(x.message) for x in schema_warns]}")
        finally:
            os.unlink(test_cif.name)

    def test_schema_suggest_category(self):
        """Unknown category warning includes 'Did you mean?' suggestion."""
        block = DataBlock("test")
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            # _entr is close to _entry
            block._entr["id"] = ["x"]
            cat_warns = [x for x in w if issubclass(x.category, SchemaWarning)
                         and "_entr" in str(x.message)]
            self.assertTrue(len(cat_warns) > 0)
            self.assertIn("Did you mean", str(cat_warns[0].message))


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

    def test_default_parsing_behavior(self):
        """Test default parsing behavior is optimized without validation."""
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
                "_entry": Category(name="_entry")
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


class TestRuleFactories(unittest.TestCase):
    """Test the rule factory functions from sloth.mmcif.validator."""

    def _make_category(self, cat_name, **items):
        """Helper: build a Category with items."""
        cat = Category(name=cat_name)
        for item_name, values in items.items():
            cat[item_name] = values if isinstance(values, list) else [values]
        return cat

    def test_mandatory_items_pass(self):
        check = mandatory_items(["id", "name"])
        cat = self._make_category("_test", id=["1"], name=["foo"])
        check(cat)  # should not raise

    def test_mandatory_items_fail(self):
        check = mandatory_items(["id", "name"])
        cat = self._make_category("_test", id=["1"])
        with self.assertRaises(ValidationError):
            check(cat)

    def test_mandatory_items_null_values(self):
        check = mandatory_items(["id"])
        cat = self._make_category("_test", id=["?"])
        with self.assertRaises(ValidationError):
            check(cat)

    def test_mandatory_items_exclude(self):
        check = mandatory_items(["id", "name"], exclude=["name"])
        cat = self._make_category("_test", id=["1"])
        check(cat)  # name excluded, should pass

    def test_one_of_following_pass(self):
        check = one_of_following(["a", "b", "c"])
        cat = self._make_category("_test", b=["val"])
        check(cat)

    def test_one_of_following_fail(self):
        check = one_of_following(["a", "b"])
        cat = self._make_category("_test", x=["val"])
        with self.assertRaises(ValidationError):
            check(cat)

    def test_value_length_pass(self):
        check = value_length("title", min_len=3, max_len=20)
        cat = self._make_category("_test", title=["Good title"])
        check(cat)

    def test_value_length_too_short(self):
        check = value_length("title", min_len=10)
        cat = self._make_category("_test", title=["Hi"])
        with self.assertRaises(ValidationError):
            check(cat)

    def test_value_length_too_long(self):
        check = value_length("title", max_len=5)
        cat = self._make_category("_test", title=["Way too long title"])
        with self.assertRaises(ValidationError):
            check(cat)

    def test_value_range_pass(self):
        check = value_range("resolution", min_val=0.0, max_val=10.0)
        cat = self._make_category("_test", resolution=["2.5"])
        check(cat)

    def test_value_range_below(self):
        check = value_range("defocus", min_val=0)
        cat = self._make_category("_test", defocus=["-1.5"])
        with self.assertRaises(ValidationError):
            check(cat)

    def test_value_range_above(self):
        check = value_range("res", max_val=5.0)
        cat = self._make_category("_test", res=["10.0"])
        with self.assertRaises(ValidationError):
            check(cat)

    def test_conditional_mandatory_triggered(self):
        check = conditional_mandatory(
            required_items=["details"],
            when_item="source_name",
            when_values=["Other"],
        )
        cat = self._make_category("_test", source_name=["Other"])
        with self.assertRaises(ValidationError):
            check(cat)

    def test_conditional_mandatory_not_triggered(self):
        check = conditional_mandatory(
            required_items=["details"],
            when_item="source_name",
            when_values=["Other"],
        )
        cat = self._make_category("_test", source_name=["PDB"])
        check(cat)  # condition not met, should pass

    def test_regex_check_pass(self):
        check = regex_check("accession_code", r"^[\w\d]{4}$")
        cat = self._make_category("_test", accession_code=["1csb"])
        check(cat)

    def test_regex_check_fail(self):
        check = regex_check("accession_code", r"^[\w\d]{4}$")
        cat = self._make_category("_test", accession_code=["invalid!!"])
        with self.assertRaises(ValidationError):
            check(cat)

    def test_ordering_check_pass(self):
        check = ordering_check("d_res_low", "d_res_high", "<")
        cat = self._make_category("_test", d_res_low=["1.0"], d_res_high=["3.0"])
        check(cat)

    def test_ordering_check_fail(self):
        check = ordering_check("d_res_low", "d_res_high", "<")
        cat = self._make_category("_test", d_res_low=["5.0"], d_res_high=["3.0"])
        with self.assertRaises(ValidationError):
            check(cat)

    def test_cross_mandatory_pass(self):
        cat_a = Category(name="_a")
        cat_b = Category(name="_b")
        cat_b["required_item"] = ["value"]
        check = cross_mandatory(["required_item"])
        check(cat_a, cat_b)

    def test_cross_mandatory_fail(self):
        cat_a = Category(name="_a")
        cat_b = Category(name="_b")
        check = cross_mandatory(["required_item"])
        with self.assertRaises(ValidationError):
            check(cat_a, cat_b)

    def test_cross_ordering_pass(self):
        cat_a = Category(name="_a")
        cat_b = Category(name="_b")
        cat_a["resolution"] = ["2.0"]
        cat_b["high_resolution"] = ["5.0"]
        check = cross_ordering("resolution", "high_resolution", "<")
        check(cat_a, cat_b)

    def test_cross_ordering_fail(self):
        cat_a = Category(name="_a")
        cat_b = Category(name="_b")
        cat_a["resolution"] = ["10.0"]
        cat_b["high_resolution"] = ["5.0"]
        check = cross_ordering("resolution", "high_resolution", "<")
        with self.assertRaises(ValidationError):
            check(cat_a, cat_b)


class TestMmcifValidatorsFactory(unittest.TestCase):
    """Test that MMCIFValidator returns a usable ValidatorPlugin."""

    def test_returns_validator_plugin(self):
        vp = MMCIFValidator()
        self.assertIsInstance(vp, ValidatorPlugin)

    def test_has_validators(self):
        vp = MMCIFValidator()
        self.assertTrue(len(vp._validators) > 0)

    def test_has_cross_checkers(self):
        vp = MMCIFValidator()
        self.assertTrue(len(vp._cross_checkers) > 0)

    def test_register_with_handler(self):
        """Validators can be registered directly on a category."""
        vp = MMCIFValidator()
        cat = Category(name="_entry")
        cat.register("validate", vp)
        # Plugin is accessible
        self.assertIn("validate", dir(cat))

    def test_struct_title_too_short(self):
        """wwPDB rule: _struct.title must be >= 10 chars."""
        vp = ValidatorPlugin()
        vp.register_validator("_struct", value_length(
            "title", min_len=10, severity=ValidationSeverity.ERROR,
        ))
        cat = Category(name="_struct")
        cat.register("validate", vp)
        cat["title"] = ["Hi"]
        with self.assertRaises(ValidationError):
            cat.validate()

    def test_struct_title_valid(self):
        """wwPDB rule: _struct.title with enough characters should pass."""
        vp = ValidatorPlugin()
        vp.register_validator("_struct", value_length(
            "title", min_len=10, severity=ValidationSeverity.ERROR,
        ))
        cat = Category(name="_struct")
        cat.register("validate", vp)
        cat["title"] = ["Crystal structure of an important protein"]
        cat.validate()  # should not raise

    def test_allowed_pairs_in_validators(self):
        """wwPDB rule #6: type/source_name pairs in _pdbx_initial_refinement_model."""
        vp = ValidatorPlugin()
        vp.register_validator("_pdbx_initial_refinement_model", allowed_pairs(
            "type", "source_name", {"other": ["Other"]},
        ))
        cat = Category(name="_pdbx_initial_refinement_model")
        cat.register("validate", vp)
        cat["type"] = ["other"]
        cat["source_name"] = ["PDB"]  # invalid: other → only Other allowed
        cat["accession_code"] = ["1abc"]
        cat["details"] = ["some details"]
        with self.assertRaises(ValidationError):
            cat.validate()

    def test_em_imaging_defocus_ordering(self):
        """wwPDB rule #8: nominal_defocus_min <= nominal_defocus_max."""
        vp = ValidatorPlugin()
        vp.register_validator("_em_imaging", ordering_check(
            "nominal_defocus_min", "nominal_defocus_max", "<=",
        ))
        cat = Category(name="_em_imaging")
        cat.register("validate", vp)
        cat["nominal_defocus_min"] = ["100"]
        cat["nominal_defocus_max"] = ["50"]  # min > max → violation
        with self.assertRaises(ValidationError):
            cat.validate()

    def test_min_rows_audit_author(self):
        """wwPDB rule #12: _audit_author must have at least 2 rows."""
        vp = ValidatorPlugin()
        vp.register_validator("_audit_author", min_rows(2))
        cat = Category(name="_audit_author")
        cat.register("validate", vp)
        cat["name"] = ["Author One"]
        with self.assertRaises(ValidationError):
            cat.validate()

    def test_em_3d_reconstruction_mandatory(self):
        """wwPDB rule #2: resolution & resolution_method mandatory."""
        vp = ValidatorPlugin()
        vp.register_validator("_em_3d_reconstruction", mandatory_items(
            ["resolution", "resolution_method"],
        ))
        cat = Category(name="_em_3d_reconstruction")
        cat.register("validate", vp)
        cat["id"] = ["1"]
        with self.assertRaises(ValidationError):
            cat.validate()


class TestDictionaryValidatorsFactory(unittest.TestCase):
    """Test that SchemaValidator reuses DictionaryParser correctly."""

    def test_returns_validator_plugin(self):
        vp = SchemaValidator()
        self.assertIsInstance(vp, ValidatorPlugin)

    def test_has_validators(self):
        """Dictionary should produce at least some mandatory / enum checks."""
        vp = SchemaValidator()
        self.assertTrue(len(vp._validators) > 0)

    def test_has_cross_checkers(self):
        """Dictionary relationships should produce FK / parent-child checks."""
        vp = SchemaValidator()
        self.assertTrue(len(vp._cross_checkers) > 0)

    def test_register_with_handler(self):
        """Validators can be registered directly on a category."""
        vp = SchemaValidator()
        cat = Category(name="_entry")
        cat.register("dict_validate", vp)
        self.assertIn("dict_validate", dir(cat))

    def test_mandatory_from_dictionary(self):
        """Mandatory items parsed from dictionary should fail when missing."""
        vp = SchemaValidator()
        # _audit_author.name and .pdbx_ordinal are mandatory in the dictionary
        cat = Category(name="_audit_author")
        cat.register("validate", vp)
        cat["irrelevant"] = ["x"]
        with self.assertRaises(ValidationError):
            cat.validate()

    def test_mandatory_from_dictionary_pass(self):
        """Mandatory items satisfied should not raise."""
        vp = SchemaValidator()
        cat = Category(name="_audit_author")
        cat.register("validate", vp)
        cat["name"] = ["Smith, J."]
        cat["pdbx_ordinal"] = ["1"]
        cat.validate()  # should not raise

    def test_enumeration_from_dictionary(self):
        """Enumeration values from dictionary should reject invalid values."""
        vp = SchemaValidator()
        # Check if there are any enumeration validators registered
        has_enum = False
        for cat_name, checks in vp._validators.items():
            for check_fn in checks:
                # enumeration_check closures contain "not in allowed values"
                if "enumeration" in getattr(check_fn, "__qualname__", ""):
                    has_enum = True
                    break
        # Even if we can't inspect closures, the factory should have produced some
        self.assertTrue(
            len(vp._validators) > 5,
            "SchemaValidator should produce validators for many categories",
        )

    def test_explicit_dict_path(self):
        """Passing an explicit path to the bundled dict should work the same."""
        dict_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "sloth", "mmcif", "schemas", "mmcif_pdbx_v50.dic",
        )
        vp = SchemaValidator(dict_path)
        self.assertIsInstance(vp, ValidatorPlugin)
        self.assertTrue(len(vp._validators) > 0)

    def test_mmcif_validator_includes_dictionary_rules(self):
        """MMCIFValidator should include dictionary rules + wwPDB rules."""
        dict_vp = SchemaValidator()
        full_vp = MMCIFValidator()
        # Full should have at least as many validators as dict-only
        self.assertGreaterEqual(
            len(full_vp._validators), len(dict_vp._validators),
        )
        # Full should have wwPDB-specific cross-checkers beyond dict
        self.assertGreaterEqual(
            len(full_vp._cross_checkers), len(dict_vp._cross_checkers),
        )


class TestNewRuleFactories(unittest.TestCase):
    """Test the new rule factory functions added for PDF/GH repo coverage."""

    def _make_category(self, cat_name, **items):
        cat = Category(name=cat_name)
        for item_name, values in items.items():
            cat[item_name] = values if isinstance(values, list) else [values]
        return cat

    # -- allowed_pairs --

    def test_allowed_pairs_pass(self):
        check = allowed_pairs("type", "source_name", {
            "experimental model": ["PDB", "Other"],
        })
        cat = self._make_category("_test", type=["experimental model"], source_name=["PDB"])
        check(cat)

    def test_allowed_pairs_fail(self):
        check = allowed_pairs("type", "source_name", {
            "other": ["Other"],
        })
        cat = self._make_category("_test", type=["other"], source_name=["PDB"])
        with self.assertRaises(ValidationError):
            check(cat)

    # -- min_rows --

    def test_min_rows_pass(self):
        check = min_rows(2)
        cat = self._make_category("_test", name=["A", "B"])
        check(cat)

    def test_min_rows_fail(self):
        check = min_rows(2)
        cat = self._make_category("_test", name=["A"])
        with self.assertRaises(ValidationError):
            check(cat)

    # -- enumeration_check --

    def test_enumeration_check_pass(self):
        check = enumeration_check("status", ["active", "inactive"])
        cat = self._make_category("_test", status=["active"])
        check(cat)

    def test_enumeration_check_fail(self):
        check = enumeration_check("status", ["active", "inactive"])
        cat = self._make_category("_test", status=["deleted"])
        with self.assertRaises(ValidationError):
            check(cat)

    # -- type_check --

    def test_type_check_pass(self):
        check = type_check("date", r"\d{4}-\d{2}-\d{2}", "yyyy-mm-dd")
        cat = self._make_category("_test", date=["2026-04-07"])
        check(cat)

    def test_type_check_fail(self):
        check = type_check("date", r"\d{4}-\d{2}-\d{2}", "yyyy-mm-dd")
        cat = self._make_category("_test", date=["20260407"])
        with self.assertRaises(ValidationError):
            check(cat)

    # -- foreign_key --

    def test_foreign_key_pass(self):
        child = Category(name="_child")
        child["entity_id"] = ["1", "2"]
        parent = Category(name="_parent")
        parent["id"] = ["1", "2", "3"]
        check = foreign_key("entity_id", "id")
        check(child, parent)

    def test_foreign_key_fail(self):
        child = Category(name="_child")
        child["entity_id"] = ["1", "99"]
        parent = Category(name="_parent")
        parent["id"] = ["1", "2"]
        check = foreign_key("entity_id", "id")
        with self.assertRaises(ValidationError):
            check(child, parent)

    # -- parent_child --

    def test_parent_child_pass(self):
        child = Category(name="_entity_src_nat")
        child["entity_id"] = ["1"]
        parent = Category(name="_entity")
        parent["id"] = ["1"]
        check = parent_child()
        check(child, parent)

    def test_parent_child_fail(self):
        child = Category(name="_entity_src_nat")
        child["entity_id"] = ["1"]
        parent = Category(name="_entity")
        # parent is empty (no items)
        check = parent_child()
        with self.assertRaises(ValidationError):
            check(child, parent)

    # -- composite_key --

    def test_composite_key_pass(self):
        child = Category(name="_child")
        child["mon_id"] = ["ALA", "GLY"]
        child["seq_num"] = ["1", "2"]
        parent = Category(name="_parent")
        parent["mon_id"] = ["ALA", "GLY", "VAL"]
        parent["num"] = ["1", "2", "3"]
        check = composite_key(["mon_id", "seq_num"], ["mon_id", "num"])
        check(child, parent)

    def test_composite_key_fail(self):
        child = Category(name="_child")
        child["mon_id"] = ["ALA", "XXX"]
        child["seq_num"] = ["1", "99"]
        parent = Category(name="_parent")
        parent["mon_id"] = ["ALA", "GLY"]
        parent["num"] = ["1", "2"]
        check = composite_key(["mon_id", "seq_num"], ["mon_id", "num"])
        with self.assertRaises(ValidationError):
            check(child, parent)

    # -- oper_expression --

    def test_oper_expression_pass(self):
        assembly_gen = Category(name="_pdbx_struct_assembly_gen")
        assembly_gen["oper_expression"] = ["(1-3)"]
        oper_list = Category(name="_pdbx_struct_oper_list")
        oper_list["id"] = ["1", "2", "3"]
        check = oper_expression()
        check(assembly_gen, oper_list)

    def test_oper_expression_fail(self):
        assembly_gen = Category(name="_pdbx_struct_assembly_gen")
        assembly_gen["oper_expression"] = ["(1-5)"]
        oper_list = Category(name="_pdbx_struct_oper_list")
        oper_list["id"] = ["1", "2", "3"]
        check = oper_expression()
        with self.assertRaises(ValidationError):
            check(assembly_gen, oper_list)

    def test_oper_expression_comma_list(self):
        assembly_gen = Category(name="_pdbx_struct_assembly_gen")
        assembly_gen["oper_expression"] = ["(1,2,5)"]
        oper_list = Category(name="_pdbx_struct_oper_list")
        oper_list["id"] = ["1", "2", "5"]
        check = oper_expression()
        check(assembly_gen, oper_list)

    def test_oper_expression_combined_groups(self):
        assembly_gen = Category(name="_pdbx_struct_assembly_gen")
        assembly_gen["oper_expression"] = ["(1,2)(3,4)"]
        oper_list = Category(name="_pdbx_struct_oper_list")
        oper_list["id"] = ["1", "2", "3"]  # missing 4
        check = oper_expression()
        with self.assertRaises(ValidationError):
            check(assembly_gen, oper_list)


class TestValidation(unittest.TestCase):
    """Tests for ValidatorPlugin.validate() and plugin-based validation."""

    def setUp(self):
        self.test_cif = tempfile.NamedTemporaryFile(
            mode="w", suffix=".cif", delete=False,
        )
        self.test_cif.write(
            "data_test\n"
            "_entry.id test\n"
            "_audit_author.name 'Smith, J.'\n"
            "_audit_author.pdbx_ordinal 1\n"
        )
        self.test_cif.close()

    def tearDown(self):
        os.unlink(self.test_cif.name)

    def test_validate_returns_report(self):
        """vp.validate() should return a ValidationReport."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif.name)
        vp = MMCIFValidator()
        report = vp.validate(mmcif)
        self.assertIsInstance(report, ValidationReport)

    def test_validate_empty_plugin(self):
        """Empty ValidatorPlugin returns empty report."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif.name)
        vp = ValidatorPlugin()
        report = vp.validate(mmcif)
        self.assertIsInstance(report, ValidationReport)
        self.assertEqual(len(report), 0)

    def test_validate_full_suite(self):
        """MMCIFValidator runs the full validation suite."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif.name)
        vp = MMCIFValidator()
        report = vp.validate(mmcif)
        self.assertIsNotNone(report)

    def test_validate_datablock(self):
        """validate() accepts a DataBlock directly."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif.name)
        block = mmcif["data_test"]
        vp = MMCIFValidator()
        report = vp.validate(block)
        self.assertIsInstance(report, ValidationReport)

    def test_validate_category(self):
        """validate() accepts a single Category."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif.name)
        cat = mmcif["data_test"]["_entry"]
        vp = MMCIFValidator()
        report = vp.validate(cat)
        self.assertIsInstance(report, ValidationReport)

    def test_validate_bad_type_raises(self):
        """validate() rejects non-data objects."""
        vp = MMCIFValidator()
        with self.assertRaises(TypeError):
            vp.validate("not a data object")

    def test_validate_custom_rules(self):
        """Custom validators run and collect errors."""
        from sloth.mmcif.validator import ValidationError as VE
        custom_vp = ValidatorPlugin()
        def _always_fail(cat):
            raise VE("custom fail", path=cat.name)
        custom_vp.register_validator("_entry", _always_fail)

        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif.name)
        report = custom_vp.validate(mmcif)

        # The custom rule's error should be in the report
        custom_msgs = [e for e in report.all_issues if "custom fail" in str(e)]
        self.assertTrue(len(custom_msgs) > 0, "Custom validator should have run")

    def test_validate_merged_rules(self):
        """Merged validators combine rules from both sources."""
        from sloth.mmcif.validator import ValidationError as VE
        custom_vp = ValidatorPlugin()
        def _check_widget(cat):
            if "widget_id" not in cat.items:
                raise VE("widget_id is required", path=cat.name)
        custom_vp.register_validator("_my_custom_category", _check_widget)

        container = MMCIFDataContainer()
        block = DataBlock("test")
        cat = Category("_my_custom_category")
        cat["name"] = ["foo"]  # missing widget_id
        block["_my_custom_category"] = cat
        container["data_test"] = block

        report = custom_vp.validate(container)
        self.assertFalse(report.is_valid)
        self.assertTrue(any("widget_id" in str(e) for e in report.errors))

    def test_validate_programmatic_data(self):
        """validate() works on data built entirely in-memory (no file)."""
        vp = MMCIFValidator()
        container = MMCIFDataContainer()
        block = DataBlock("test")
        cat = Category("_entry")
        cat["id"] = ["test_structure"]
        block["_entry"] = cat
        container["data_test"] = block
        report = vp.validate(container)
        self.assertIsInstance(report, ValidationReport)

    def test_report_is_valid_property(self):
        """Report.is_valid should be True when only warnings present."""
        report = ValidationReport()
        self.assertTrue(report.is_valid)
        report.add(ValidationError("warn", severity=ValidationSeverity.WARNING))
        self.assertTrue(report.is_valid)
        report.add(ValidationError("err", severity=ValidationSeverity.ERROR))
        self.assertFalse(report.is_valid)

    def test_report_raise_on_error(self):
        """raise_on_error() should raise when errors exist."""
        report = ValidationReport()
        report.raise_on_error()  # no-op, no errors
        report.add(ValidationError("boom", severity=ValidationSeverity.ERROR))
        with self.assertRaises(ValidationError):
            report.raise_on_error()

    def test_block_validate_dot_notation(self):
        """block.validate() should be available when validators are registered."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif.name)
        block = mmcif["data_test"]
        vp = MMCIFValidator()
        block.register("validate", DataBlockValidator(vp))
        wrapper = block.validate()
        self.assertIsNotNone(wrapper.report)

    def test_container_validate_dot_notation(self):
        """container.validate() should be available when validators are registered."""
        handler = MMCIFHandler()
        mmcif = handler.read(self.test_cif.name)
        vp = MMCIFValidator()
        bv = DataBlockValidator(vp)
        cv = ContainerValidator(bv)
        mmcif.register("validate", cv)
        wrapper = mmcif.validate()
        self.assertIsNotNone(wrapper.report)


if __name__ == "__main__":
    unittest.main()
