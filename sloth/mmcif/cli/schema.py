"""
Schema hint provider for the SLOTH CLI.

Wraps _DictionarySchema to provide autocomplete suggestions,
category/item metadata, and enumeration values for the editor.
"""

from typing import Dict, FrozenSet, List, Optional, Tuple

from ..models import _DictionarySchema


class SchemaHints:
    """Provides schema-driven hints for the interactive editor."""

    def __init__(self) -> None:
        self._schema: Optional[_DictionarySchema] = _DictionarySchema.get()
        self._enumerations: Dict[str, List[str]] = {}
        self._loaded_enums = False

    # -- availability -------------------------------------------------------

    @property
    def available(self) -> bool:
        return self._schema is not None

    # -- category helpers ---------------------------------------------------

    def all_categories(self) -> List[str]:
        if not self._schema:
            return []
        return sorted(self._schema.all_categories())

    def match_categories(self, prefix: str) -> List[str]:
        prefix_lower = prefix.lower()
        return [c for c in self.all_categories() if c.lower().startswith(prefix_lower)]

    def is_known_category(self, name: str) -> bool:
        if not self._schema:
            return False
        return self._schema.known_category(name)

    # -- item helpers -------------------------------------------------------

    def items_for_category(self, category: str) -> List[str]:
        if not self._schema:
            return []
        return sorted(self._schema.category_items(category))

    def match_items(self, category: str, prefix: str) -> List[str]:
        prefix_lower = prefix.lower()
        return [
            i
            for i in self.items_for_category(category)
            if i.lower().startswith(prefix_lower)
        ]

    def is_known_item(self, category: str, item: str) -> bool:
        if not self._schema:
            return False
        return self._schema.known_item(category, item)

    # -- enumerations (lazy-loaded from full dictionary parse) ---------------

    def _ensure_enumerations(self) -> None:
        if self._loaded_enums:
            return
        self._loaded_enums = True
        try:
            from pathlib import Path
            from ..serializer import DictionaryParser, get_cache_manager
            from ..defaults import DictDataType

            dict_path = str(
                Path(__file__).parent.parent / "schemas" / "mmcif_pdbx_v50.dic"
            )
            dp = DictionaryParser(get_cache_manager(), quiet=True)
            meta = dp.parse(dict_path)
            self._enumerations = meta.get(DictDataType.ENUMERATIONS.value, {})
        except Exception:
            self._enumerations = {}

    def enumerations_for(self, category: str, item: str) -> List[str]:
        self._ensure_enumerations()
        full_name = f"{category}.{item}"
        return list(self._enumerations.get(full_name, []))

    # -- summary for status display -----------------------------------------

    def category_summary(self, category: str) -> str:
        items = self.items_for_category(category)
        if not items:
            return "Custom category (not in mmCIF dictionary)"
        return f"{len(items)} items defined in mmCIF dictionary"
