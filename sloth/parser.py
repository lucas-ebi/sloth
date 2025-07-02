import os
from typing import Optional, List, Union
from pathlib import Path
from .models import Category, DataBlock, MMCIFDataContainer, DataSourceFormat
from .validator import ValidatorFactory
from .common import BaseParser
import shlex


def fast_mmcif_split(line: str) -> List[str]:
    """Optimized mmCIF line splitting - avoid shlex overhead when possible."""
    # Fast path: if no quotes, use simple split (90%+ of cases)
    if '"' not in line and "'" not in line:
        return line.split()

    # Medium path: try simple approach with basic quote handling
    parts = line.split()
    needs_shlex = False
    for part in parts:
        if (part.startswith('"') and not part.endswith('"')) or (
            part.startswith("'") and not part.endswith("'")
        ):
            needs_shlex = True
            break

    if not needs_shlex:
        # Simple quotes - remove them
        return [
            p.strip("\"'") if p.startswith(("\"", "'")) and p.endswith(("\"", "'"))
            else p for p in parts
        ]

    # Complex path: use shlex only when absolutely needed
    try:
        return shlex.split(line)
    except ValueError:
        # Fallback to simple split if shlex fails
        return line.split()


class MMCIFParser(BaseParser):
    """mmCIF parser with lazy loading for optimal performance."""

    def __init__(
        self,
        validator_factory: Optional[ValidatorFactory],
        categories: Optional[List[str]] = None,
    ):
        super().__init__(validator_factory, categories)
        self._data_blocks = {}
        self._current_block = None
        self._current_category = None
        self._current_data = None
        self._loop_items = []
        self._in_loop = False
        self._multi_line_value = False
        self._multi_line_item_name = ""
        self._multi_line_value_buffer = []
        self._current_row_values = []
        self._value_counter = 0
        self._file_path: Optional[str] = None

    def parse_file(self, file_path: Union[str, Path]) -> MMCIFDataContainer:
        """Parse a file using memory mapping with lazy loading."""
        # Convert Path to string if needed
        file_path_str = str(file_path)
        self._file_path = file_path_str

        # Check file size first
        file_size = os.path.getsize(file_path_str)

        # Handle empty files
        if file_size == 0:
            return MMCIFDataContainer({})
        # Use regular file I/O for all files
        # Read entire file at once (faster than line-by-line for medium files)
        with open(file_path_str, "r", encoding="utf-8") as f:
            content = f.read()

        # Split into lines once (faster than readlines())
        lines = content.split("\n")

        # Process lines without memory mapping overhead
        line_count = len(lines)
        for i in range(line_count):
            line = lines[i].rstrip()
            if line:  # Skip empty lines early
                self._process_line(line)

        # Commit any remaining batched values
        self._commit_all_category_batches()

        return MMCIFDataContainer(
            self._data_blocks, source_format=DataSourceFormat.MMCIF
        )

    def _commit_all_category_batches(self) -> None:
        """Commit all batched values across all categories."""
        for block in self._data_blocks.values():
            for category in block._categories.values():
                if hasattr(category, "_commit_all_batches"):
                    category._commit_all_batches()

    def _process_line(self, line: str) -> None:
        """Process a line for small files without memory mapping overhead."""
        if not line:  # Early exit for empty lines
            return

        first_char = line[0]
        # Ultra-fast character-based dispatch
        if first_char == "#":
            return
        elif first_char == "d":  # data_
            if line.startswith("data_"):
                self._handle_data_block(line)
        elif first_char == "l":  # loop_
            if line.startswith("loop_"):
                self._start_loop()
        elif first_char == "_":
            self._handle_item_line(line)
        elif self._in_loop and first_char not in ["#", "d", "l", "_"]:
            self._handle_loop_value_line(line)
        elif self._multi_line_value:
            self._handle_non_loop_multiline(line)

    def _handle_data_block(self, line: str):
        self._current_block = line.split("_", 1)[1]
        self._data_blocks[self._current_block] = DataBlock(
            self._current_block, {}
        )
        self._current_category = None
        self._in_loop = False

    def _start_loop(self):
        self._in_loop = True
        self._loop_items = []

    def _handle_loop_item(self, item_full: str, value: str):
        # Handle malformed item names gracefully
        if "." not in item_full:
            return  # Skip malformed items
        category, item = item_full.split(".", 1)
        if not self._should_include_category(category):
            return
        if self._in_loop:
            self._loop_items.append(item_full)
            self._ensure_current_data(category)
        else:
            self._ensure_current_data(category)
            self._current_data._add_item_value(
                item, value
            )

    def _handle_loop_value_line(self, line: str) -> None:
        """Optimized loop value line handling for small files."""
        if not self._multi_line_value:
            # Fast tokenization optimized for common cases
            values = self._fast_tokenize_loop_line(line)

            # Process values in batch
            num_items = len(self._loop_items)
            values_processed = 0

            for value in values:
                if len(self._current_row_values) >= num_items:
                    break

                if value.startswith(";"):
                    # Handle multi-line value
                    item_idx = len(self._current_row_values)
                    self._multi_line_value = True
                    self._multi_line_item_name = self._loop_items[item_idx].split(
                        ".", 1
                    )[1]
                    self._multi_line_value_buffer.append(value[1:])
                    self._current_row_values.append(None)
                    break
                else:
                    self._current_row_values.append(value)
                    values_processed += 1

            self._value_counter += values_processed
            self._maybe_commit_loop_row()
        else:
            if line == ";":
                self._multi_line_value = False
                full_value = "\n".join(self._multi_line_value_buffer)
                self._current_row_values[-1] = full_value
                self._multi_line_value_buffer = []
                self._value_counter += 1
                self._maybe_commit_loop_row()
            else:
                self._multi_line_value_buffer.append(line)

    def _maybe_commit_loop_row(self):
        """Optimized loop row commit for small files."""
        if self._value_counter == len(self._loop_items):
            # Batch process all values for this row at once
            item_names = [item.split(".", 1)[1] for item in self._loop_items]

            for i, value in enumerate(self._current_row_values):
                if value is not None:
                    self._current_data._add_item_value(
                        item_names[i], value
                    )

            # Reset row state (reuse lists instead of creating new ones)
            self._current_row_values.clear()
            self._value_counter = 0

    def _handle_non_loop_multiline(self, line: str):
        if line == ";":
            self._multi_line_value = False
            full_value = "\n".join(self._multi_line_value_buffer)
            self._current_data._add_item_value(self._multi_line_item_name, full_value)
            self._multi_line_value_buffer = []
        else:
            self._multi_line_value_buffer.append(line)

    def _should_include_category(self, category: str) -> bool:
        return not self.categories or category in self.categories

    def _ensure_current_data(self, category: str):
        if self._current_category != category:
            self._current_category = category
            if category not in self._data_blocks[self._current_block]._categories:
                self._data_blocks[self._current_block]._categories[category] = Category(
                    category, self.validator_factory
                )
            self._current_data = self._data_blocks[self._current_block]._categories[
                category
            ]

    def _handle_item_line(self, line: str) -> None:
        """Handle item lines for small files without offset tracking."""
        parts = fast_mmcif_split(line)
        if len(parts) == 2:
            self._handle_simple_item(parts[0], parts[1])
        else:
            self._handle_loop_item(parts[0], line[len(parts[0]) :].strip())

    def _handle_simple_item(self, item_full: str, value: str) -> None:
        """Handle simple items for small files without memory mapping."""
        category, item = item_full.split(".", 1)
        if not self._should_include_category(category):
            return
        self._ensure_current_data(category)

        if value.startswith(";"):
            self._multi_line_value = True
            self._multi_line_item_name = item
            self._multi_line_value_buffer = []
        else:
            # Direct value storage with batch optimization
            self._current_data._add_item_value(item, value.strip())

    def _fast_tokenize_loop_line(self, line: str) -> List[str]:
        """Ultra-fast tokenization optimized for loop value lines."""
        # Fast path: no quotes means simple split works
        if '"' not in line and "'" not in line:
            return line.split()

        # Complex path: manual tokenization with quote handling
        tokens = []
        i = 0
        line_len = len(line)

        while i < line_len:
            # Skip whitespace
            while i < line_len and line[i].isspace():
                i += 1
            if i >= line_len:
                break

            start = i
            if line[i] in ['"', "'"]:
                # Quoted token
                quote = line[i]
                i += 1
                while i < line_len and line[i] != quote:
                    if line[i] == "\\":
                        i += 2  # Skip escaped char
                    else:
                        i += 1
                if i < line_len:
                    i += 1  # Skip closing quote
                # Extract without quotes
                if i > start + 1:
                    tokens.append(line[start + 1:i - 1])
            else:
                # Unquoted token
                while i < line_len and not line[i].isspace():
                    i += 1
                tokens.append(line[start:i])

        return tokens
