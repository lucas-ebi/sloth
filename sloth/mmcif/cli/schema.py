"""
Schema hint provider for the SLOTH CLI.

Wraps _DictionarySchema to provide autocomplete suggestions,
category/item metadata, and enumeration values for the editor.
"""

from typing import Dict, FrozenSet, List, Optional, Set, Tuple

from ..models import _DictionarySchema, MMCIFDataContainer


class SchemaHints:
    """Provides schema-driven hints for the interactive editor."""

    def __init__(self) -> None:
        self._schema: Optional[_DictionarySchema] = _DictionarySchema.get()
        self._enumerations: Dict[str, List[str]] = {}
        self._loaded_enums = False
        self._ownership_children: Optional[Dict[str, List[str]]] = None
        self._loaded_hierarchy = False
        self._child_to_parents: Optional[Dict[str, List[str]]] = None
        self._loaded_relationships = False

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

    def suggested_categories(
        self,
        prefix: str,
        present_categories: Set[str],
        limit: int = 10,
    ) -> Tuple[List[str], List[str]]:
        """Return suggestion lists: (missing_required, other_matches).

        Both lists are underscore-prefixed and exclude categories already present
        in *present_categories*.
        """
        present_stripped = {c.lstrip("_") for c in present_categories}
        all_matches = [
            c
            for c in self.match_categories(prefix or "_")
            if c.lstrip("_") not in present_stripped
        ]

        missing_required = [
            f"_{cat}"
            for cat in self.missing_required_parents(present_stripped)
            if f"_{cat}".lower().startswith((prefix or "_").lower())
        ]

        # Keep order deterministic and avoid duplicates in the secondary list.
        seen = set(missing_required)
        others: List[str] = []
        for name in all_matches:
            if name in seen:
                continue
            others.append(name)
            if len(missing_required) + len(others) >= limit:
                break

        return missing_required[:limit], others

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

    # -- category hierarchy (computed from the full serializer pipeline) ----

    def _ensure_hierarchy(self, container: Optional[MMCIFDataContainer] = None) -> None:
        if self._loaded_hierarchy:
            return
        self._loaded_hierarchy = True
        try:
            from pathlib import Path
            from ..serializer import (
                DictionaryParser,
                MappingGenerator,
                NestingBuilder,
                OwnershipAnalyzer,
                RelationshipResolver,
                get_cache_manager,
            )
            from ..defaults import DictDataType, MappingDataKey

            dict_path = str(
                Path(__file__).parent.parent / "schemas" / "mmcif_pdbx_v50.dic"
            )
            cache_mgr = get_cache_manager()
            dp = DictionaryParser(cache_mgr, quiet=True)
            dp.source = dict_path
            mg = MappingGenerator(dp, cache_mgr, quiet=True)

            resolver = RelationshipResolver(mg)

            if container is not None:
                # Run the full pipeline with real data: flatten → ownership
                # → usability filter → primary parent selection.
                flat = resolver._flatten_mmcif(container)
                mapping = resolver.mapping_rules
                fk_map = mapping[MappingDataKey.FK_MAP.value]
                primary_keys = mapping.get(DictDataType.PRIMARY_KEYS.value, {})
                present_categories: Set[str] = {
                    cat.lstrip("_")
                    for block in container
                    for cat in block.categories
                }

                ownership_fk, _ = resolver.ownership_analyzer.filter_ownership_relationships(
                    fk_map, flat
                )

                # Filter to usable relationships (parent exists, child has FK
                # field) — mirrors NestingBuilder._filter_usable_relationships
                # but works directly on the flat dict so we don't need the
                # indexed structure (which can choke on composite PKs).
                usable: dict = {}
                for (cc, cf), (pc, pf) in ownership_fk.items():
                    if pc not in flat:
                        continue
                    if any(cf in row for row in flat.get(cc, [])):
                        usable[(cc, cf)] = (pc, pf)

                # Select a single primary nesting parent per child
                nb = NestingBuilder()
                nesting_fk = nb._select_primary_nesting_parents(usable, primary_keys)

                # If a category is present but currently unresolved by the
                # data-driven pass (common for empty categories), anchor it
                # using schema-only primary parent selection.
                schema_ownership_fk, _ = OwnershipAnalyzer(mg).filter_ownership_relationships(
                    fk_map, {}
                )
                schema_primary_fk = nb._select_primary_nesting_parents(
                    schema_ownership_fk,
                    primary_keys,
                )
                schema_parent_for_child: Dict[str, str] = {
                    child_cat: parent_cat
                    for (child_cat, _), (parent_cat, _) in schema_primary_fk.items()
                }
                data_assigned_children = {child_cat for (child_cat, _) in nesting_fk.keys()}

                for child_cat in present_categories:
                    if child_cat in data_assigned_children:
                        continue
                    parent_cat = schema_parent_for_child.get(child_cat)
                    if not parent_cat or parent_cat not in present_categories:
                        continue
                    # Synthetic field names are fine here because we only
                    # consume category-level parent/child relationships.
                    nesting_fk[(child_cat, "__schema_anchor__")] = (
                        parent_cat,
                        "__schema_anchor__",
                    )
            else:
                # Fallback: schema-only (no data filtering / parent selection)
                mapping = mg.get_mapping_rules()
                fk_map = mapping[MappingDataKey.FK_MAP.value]
                oa = OwnershipAnalyzer(mg)
                nesting_fk, _ = oa.filter_ownership_relationships(fk_map, {})

            # Build parent → [children] from the nesting FK map
            children: Dict[str, Set[str]] = {}
            for (child_cat, _), (parent_cat, _) in nesting_fk.items():
                children.setdefault(parent_cat, set()).add(child_cat)

            self._ownership_children = {
                k: sorted(v) for k, v in children.items()
            }
        except Exception:
            self._ownership_children = {}

    def build_hierarchy(self, container: MMCIFDataContainer) -> None:
        """(Re)compute hierarchy using actual data from *container*."""
        self._loaded_hierarchy = False
        self._ownership_children = None
        self._ensure_hierarchy(container)

    def children_of(self, category: str) -> List[str]:
        """Return owned child categories of *category* (underscore-stripped)."""
        self._ensure_hierarchy()
        cat = category.lstrip("_")
        return list(self._ownership_children.get(cat, []))

    def parent_of(self, category: str) -> Optional[str]:
        """Return the owning parent category, or None."""
        self._ensure_hierarchy()
        cat = category.lstrip("_")
        for parent, kids in (self._ownership_children or {}).items():
            if cat in kids:
                return parent
        return None

    def root_categories(self, present: Set[str]) -> List[str]:
        """Return categories from *present* that are not owned by another present category."""
        self._ensure_hierarchy()
        owned: Set[str] = set()
        stripped = [c.lstrip("_") for c in present]
        present_set = set(stripped)
        for cat in stripped:
            for child in self.children_of(cat):
                if child in present_set:
                    owned.add(child)
        return [c for c in stripped if c not in owned]

    # -- required parent category helpers -----------------------------------

    def _ensure_relationships(self) -> None:
        if self._loaded_relationships:
            return
        self._loaded_relationships = True
        try:
            from pathlib import Path
            from ..serializer import DictionaryParser, MappingGenerator, get_cache_manager
            from ..defaults import MappingDataKey

            dict_path = str(
                Path(__file__).parent.parent / "schemas" / "mmcif_pdbx_v50.dic"
            )
            cache_mgr = get_cache_manager()
            dp = DictionaryParser(cache_mgr, quiet=True)
            dp.source = dict_path
            mg = MappingGenerator(dp, cache_mgr, quiet=True)
            mapping = mg.get_mapping_rules()
            fk_map = mapping.get(MappingDataKey.FK_MAP.value, {})

            child_to_parents: Dict[str, Set[str]] = {}
            for (child_cat, _), (parent_cat, _) in fk_map.items():
                child_to_parents.setdefault(child_cat, set()).add(parent_cat)

            self._child_to_parents = {
                child: sorted(parents) for child, parents in child_to_parents.items()
            }
        except Exception:
            self._child_to_parents = {}

    def missing_required_parents(self, present_categories: Set[str]) -> List[str]:
        """Return missing parent categories required by currently present children.

        Categories are returned without underscore prefix.
        """
        self._ensure_relationships()
        present = {c.lstrip("_") for c in present_categories}
        missing: Set[str] = set()

        for child in present:
            for parent in (self._child_to_parents or {}).get(child, []):
                if parent not in present:
                    missing.add(parent)

        return sorted(missing)
