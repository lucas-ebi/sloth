import unittest
from unittest.mock import mock_open, patch
from mmcif_tools import MMCIFHandler, MMCIFParser, MMCIFWriter, MMCIFDataContainer, DataBlock, Category, ValidatorFactory

class TestMMCIFParser(unittest.TestCase):
    mmcif_content = """
data_7XJP
#
_database_2.database_id      PDB
_database_2.database_code    7XJP
#
"""

    def setUp(self):
        self.reader = MMCIFParser(atoms=False, validator_factory=None, categories=['_database_2'])

    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_read_empty_file(self, mock_file):
        with open("dummy.cif", "r") as f:
            data_container = self.reader.read(f)
        self.assertEqual(len(data_container), 0)

    @patch("builtins.open", new_callable=mock_open, read_data=mmcif_content)
    def test_read_file_with_data(self, mock_file):
        with open("dummy.cif", "r") as f:
            data_container = self.reader.read(f)
        self.assertIn("7XJP", data_container.blocks)
        data_block = data_container["7XJP"]
        self.assertIn("_database_2", data_block.categories)
        category = data_block["_database_2"]
        self.assertEqual(category["database_id"], ["PDB"])
        self.assertEqual(category["database_code"], ["7XJP"])


class TestMMCIFWriter(unittest.TestCase):
    def setUp(self):
        self.data_block = DataBlock(name="7XJP", categories={
            "_database_2": Category(name="_database_2", validator_factory=None)
        })
        self.data_block["_database_2"].add_item_value("database_id", "PDB")
        self.data_block["_database_2"].add_item_value("database_code", "7XJP")
        self.data_container = MMCIFDataContainer(data_blocks={"7XJP": self.data_block})
        self.writer = MMCIFWriter()

    @patch("builtins.open", new_callable=mock_open)
    def test_write_file(self, mock_file):
        with open("dummy.cif", "w") as f:
            self.writer.write(f, self.data_container)
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
        self.handler = MMCIFHandler(atoms=False, validator_factory=None)

    @patch("builtins.open", new_callable=mock_open, read_data=mmcif_content)
    def test_parse_file(self, mock_file):
        data_container = self.handler.parse("dummy.cif", categories=['_database_2'])
        self.assertIn("7XJP", data_container.blocks)
        data_block = data_container["7XJP"]
        self.assertIn("_database_2", data_block.categories)
        category = data_block["_database_2"]
        self.assertEqual(category["database_id"], ["PDB"])
        self.assertEqual(category["database_code"], ["7XJP"])

    @patch("builtins.open", new_callable=mock_open)
    def test_write_file(self, mock_file):
        data_block = DataBlock(name="7XJP", categories={
            "_database_2": Category(name="_database_2", validator_factory=None)
        })
        data_block["_database_2"].add_item_value("database_id", "PDB")
        data_block["_database_2"].add_item_value("database_code", "7XJP")
        data_container = MMCIFDataContainer(data_blocks={"7XJP": data_block})
        with open("dummy.cif", "w") as f:
            self.handler.file_obj = f
            self.handler.write(data_container)
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
        self.assertEqual(self.factory.get_cross_checker(("category1", "category2")), cross_checker)


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

        self.factory.register_cross_checker(("_database_2", "_database_1"), cross_checker)
        self.category.validate().against(other_category)


if __name__ == "__main__":
    unittest.main()
