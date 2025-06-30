from typing import IO
from .models import MMCIFDataContainer, Category

class MMCIFWriter:
    """A class to write an mmCIF data container to a file."""
    def write(self, file_obj: IO, mmcif_data_container: MMCIFDataContainer) -> None:
        try:
            for data_block in mmcif_data_container:
                file_obj.write(f"data_{data_block.name}\n")
                file_obj.write("#\n")
                for category_name in data_block.categories:
                    category = data_block.data[category_name]
                    if isinstance(category, Category):
                        self._write_category(file_obj, category_name, category)
                        file_obj.write("#\n")
        except IOError as e:
            print(f"Error writing to file: {e}")

    def _write_category(self, file_obj: IO, category_name: str, category: Category) -> None:
        """
        Writes a category to a file.

        :param file_obj: The file object to write to.
        :type file_obj: IO
        :param category_name: The name of the category.
        :type category_name: str
        :param category: The category to write.
        :type category: Category
        :return: None
        """
        # Get all data (this will force loading of lazy items)
        items = category.data
        
        if any(len(values) > 1 for values in items.values()):
            file_obj.write("loop_\n")
            for item_name in items.keys():
                file_obj.write(f"{category_name}.{item_name}\n")
            for row in zip(*items.values()):
                formatted_row = [self._format_value(value) for value in row]
                file_obj.write(f"{''.join(formatted_row)}\n".replace('\n\n', '\n'))
        else:
            for item_name, values in items.items():
                for value in values:
                    formatted_value = self._format_value(value)
                    file_obj.write(f"{category_name}.{item_name} {formatted_value}\n")

    @staticmethod
    def _format_value(value: str) -> str:
        """
        Formats a value for writing to a file.

        :param value: The value to format.
        :type value: str
        :return: The formatted value.
        :rtype: str
        """
        if '\n' in value or value.startswith(' ') or value.startswith(';'):
            return f"\n;{value.strip()}\n;\n"
        if ' ' in value or value.startswith('_') or value.startswith("'") or value.startswith('"'):
            return f"'{value}' "
        return f"{value} "
