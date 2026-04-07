"""
SLOTH Validation Rules

Two validator classes and a library of composable rule factories for
mmCIF validation.

**Validator classes** (ready-to-use):

* :class:`DictionaryValidator` — auto-generated from an mmCIF dictionary
  via SLOTH's :class:`~sloth.mmcif.serializer.DictionaryParser` (mandatory,
  enumeration, type-regex, FK, composite-key, parent/child).
* :class:`MmcifValidator` — extends ``DictionaryValidator`` with wwPDB
  deposition business rules expressed as declarative data tables.
  Derived from the wwPDB private codebase.

**Rule factories** (for user composition):
generic building-blocks that can be used independently or combined into
a custom :class:`~sloth.mmcif.validator.ValidatorPlugin`.

Usage::

    from sloth import MMCIFHandler
    from sloth.mmcif.rules import MmcifValidator

    handler = MMCIFHandler(strict=True)   # auto-registers MmcifValidator
    mmcif = handler.read("model.cif")
    mmcif.data_1ABC._refine.validate()
"""

import re
from typing import (
    Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple,
    TYPE_CHECKING,
)

from .validator import ValidatorPlugin, ValidationError, ValidationSeverity
from .defaults import DataValue

if TYPE_CHECKING:
    from .models import Category


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_null(value: str) -> bool:
    """Return True if *value* is an mmCIF null (``?``, ``.``, empty)."""
    return DataValue.is_null(value)


def _has_value(category: "Category", item_name: str) -> bool:
    """Return True if *item_name* exists in *category* and has ≥1 non-null value."""
    try:
        values = category[item_name]
    except (KeyError, AttributeError):
        return False
    return any(not _is_null(v) for v in values)


def _row_count(category: "Category") -> int:
    """Return the number of rows in *category*."""
    return getattr(category, "row_count", 0)


# ===================================================================
# Single-category rule factories
# ===================================================================

def mandatory_items(
    items: Sequence[str],
    exclude: Sequence[str] = (),
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category"], None]:
    """Items that must be non-null when the category is present."""
    _exclude = set(exclude)

    def check(category: "Category") -> None:
        for item in items:
            if item in _exclude:
                continue
            if not _has_value(category, item):
                raise ValidationError(
                    f"Mandatory item '{item}' is missing or null",
                    path=category.name,
                    severity=severity,
                )
    return check


def one_of_following(
    items: Sequence[str],
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category"], None]:
    """At least one of *items* must be non-null."""
    def check(category: "Category") -> None:
        if not any(_has_value(category, item) for item in items):
            raise ValidationError(
                f"At least one of {list(items)} must be set",
                path=category.name,
                severity=severity,
            )
    return check


def value_length(
    item: str,
    min_len: Optional[int] = None,
    max_len: Optional[int] = None,
    severity: ValidationSeverity = ValidationSeverity.WARNING,
) -> Callable[["Category"], None]:
    """String length bounds for an item."""
    def check(category: "Category") -> None:
        if not _has_value(category, item):
            return
        for val in category[item]:
            if _is_null(val):
                continue
            length = len(val)
            if min_len is not None and length < min_len:
                raise ValidationError(
                    f"'{item}' length {length} < minimum {min_len}",
                    path=category.name,
                    severity=severity,
                )
            if max_len is not None and length > max_len:
                raise ValidationError(
                    f"'{item}' length {length} > maximum {max_len}",
                    path=category.name,
                    severity=severity,
                )
    return check


def value_range(
    item: str,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    severity: ValidationSeverity = ValidationSeverity.WARNING,
) -> Callable[["Category"], None]:
    """Numeric bounds for an item."""
    def check(category: "Category") -> None:
        if not _has_value(category, item):
            return
        for val in category[item]:
            if _is_null(val):
                continue
            try:
                num = float(val)
            except ValueError:
                continue
            if min_val is not None and num < min_val:
                raise ValidationError(
                    f"'{item}' value {num} < minimum {min_val}",
                    path=category.name,
                    severity=severity,
                )
            if max_val is not None and num > max_val:
                raise ValidationError(
                    f"'{item}' value {num} > maximum {max_val}",
                    path=category.name,
                    severity=severity,
                )
    return check


def conditional_mandatory(
    required_items: Sequence[str],
    when_item: str,
    when_values: Sequence[str],
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category"], None]:
    """Items that must be non-null when *when_item* has one of *when_values*."""
    _when_values = set(when_values)

    def check(category: "Category") -> None:
        if not _has_value(category, when_item):
            return
        values = category[when_item]
        if not any(v in _when_values for v in values if not _is_null(v)):
            return
        for item in required_items:
            if not _has_value(category, item):
                raise ValidationError(
                    f"'{item}' is required when '{when_item}' "
                    f"is one of {sorted(_when_values)}",
                    path=category.name,
                    severity=severity,
                )
    return check


def regex_check(
    item: str,
    pattern: str,
    error_text: str = "",
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category"], None]:
    """Values of *item* must match *pattern*."""
    compiled = re.compile(pattern)

    def check(category: "Category") -> None:
        if not _has_value(category, item):
            return
        for val in category[item]:
            if _is_null(val):
                continue
            if not compiled.match(val):
                msg = error_text or f"'{item}' value '{val}' does not match {pattern}"
                raise ValidationError(msg, path=category.name, severity=severity)
    return check


def ordering_check(
    item_a: str,
    item_b: str,
    op: str = "<",
    severity: ValidationSeverity = ValidationSeverity.WARNING,
) -> Callable[["Category"], None]:
    """Numeric ordering: *item_a* ``op`` *item_b* (per row).

    :param op: One of ``"<"``, ``"<="``, ``">"``, ``">="``.
    """
    ops = {
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
    }
    if op not in ops:
        raise ValueError(f"Unsupported operator: {op}")
    cmp = ops[op]

    def check(category: "Category") -> None:
        if not _has_value(category, item_a) or not _has_value(category, item_b):
            return
        vals_a = category[item_a]
        vals_b = category[item_b]
        for va, vb in zip(vals_a, vals_b):
            if _is_null(va) or _is_null(vb):
                continue
            try:
                na, nb = float(va), float(vb)
            except ValueError:
                continue
            if not cmp(na, nb):
                raise ValidationError(
                    f"'{item_a}' ({na}) must be {op} '{item_b}' ({nb})",
                    path=category.name,
                    severity=severity,
                )
    return check


def allowed_pairs(
    item_a: str,
    item_b: str,
    valid_mapping: Mapping[str, Sequence[str]],
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category"], None]:
    """Restrict allowed (item_a, item_b) value combinations per row.

    *valid_mapping* maps each value of *item_a* to the sequence of allowed
    values in *item_b*.
    """
    def check(category: "Category") -> None:
        if not _has_value(category, item_a) or not _has_value(category, item_b):
            return
        for va, vb in zip(category[item_a], category[item_b]):
            if _is_null(va) or _is_null(vb):
                continue
            allowed = valid_mapping.get(va)
            if allowed is not None and vb not in allowed:
                raise ValidationError(
                    f"'{item_b}' value '{vb}' is not allowed when "
                    f"'{item_a}' is '{va}' (allowed: {sorted(allowed)})",
                    path=category.name,
                    severity=severity,
                )
    return check


def min_rows(
    n: int,
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category"], None]:
    """Category must contain at least *n* rows."""
    def check(category: "Category") -> None:
        count = _row_count(category)
        if count < n:
            raise ValidationError(
                f"Expected at least {n} rows, found {count}",
                path=category.name,
                severity=severity,
            )
    return check


# --- Dictionary-pattern factories (PDBeurope/mmcif-validator) ------

def enumeration_check(
    item: str,
    allowed_values: Sequence[str],
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category"], None]:
    """Values of *item* must be in *allowed_values*.

    Mirrors the dictionary ``_item_enumeration`` validation from the
    PDBeurope/mmcif-validator.
    """
    _allowed = set(allowed_values)

    def check(category: "Category") -> None:
        if not _has_value(category, item):
            return
        for val in category[item]:
            if _is_null(val):
                continue
            if val not in _allowed:
                raise ValidationError(
                    f"'{item}' value '{val}' is not in enumeration: "
                    f"{sorted(_allowed)}",
                    path=category.name,
                    severity=severity,
                )
    return check


def type_check(
    item: str,
    type_pattern: str,
    type_name: str = "",
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category"], None]:
    """Values of *item* must match the data-type regex *type_pattern*.

    Mirrors the dictionary ``_item_type_list.construct`` validation from the
    PDBeurope/mmcif-validator.
    """
    compiled = re.compile(type_pattern)

    def check(category: "Category") -> None:
        if not _has_value(category, item):
            return
        for val in category[item]:
            if _is_null(val):
                continue
            if not compiled.fullmatch(val):
                label = type_name or type_pattern
                raise ValidationError(
                    f"'{item}' value '{val}' does not match expected type '{label}'",
                    path=category.name,
                    severity=severity,
                )
    return check


def foreign_key(
    child_item: str,
    parent_item: str,
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category", "Category"], None]:
    """Cross-checker: every non-null value of *child_item* in cat_a must
    exist in *parent_item* of cat_b.

    Mirrors the FK integrity check from the PDBeurope/mmcif-validator.
    """
    def check(cat_a: "Category", cat_b: "Category") -> None:
        if not _has_value(cat_a, child_item):
            return
        try:
            parent_values = set(
                v for v in cat_b[parent_item] if not _is_null(v)
            )
        except (KeyError, AttributeError):
            parent_values = set()
        for val in cat_a[child_item]:
            if _is_null(val):
                continue
            if val not in parent_values:
                raise ValidationError(
                    f"Foreign key value '{val}' in '{child_item}' does not "
                    f"exist in parent item '{parent_item}' "
                    f"(parent category '{cat_b.name}')",
                    path=cat_a.name,
                    severity=severity,
                )
    return check


def parent_child(
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category", "Category"], None]:
    """Cross-checker: if child category (cat_a) is present, parent category
    (cat_b) must also be present and non-empty.

    Mirrors the parent/child category validation from the
    PDBeurope/mmcif-validator.
    """
    def check(cat_a: "Category", cat_b: "Category") -> None:
        if _row_count(cat_b) == 0:
            raise ValidationError(
                f"Child category '{cat_a.name}' is present but parent "
                f"category '{cat_b.name}' is missing",
                path=cat_a.name,
                severity=severity,
            )
    return check


def composite_key(
    child_items: Sequence[str],
    parent_items: Sequence[str],
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category", "Category"], None]:
    """Cross-checker: each combination of *child_items* in cat_a must exist
    as a matching combination of *parent_items* in cat_b.

    Mirrors the composite key validation from the PDBeurope/mmcif-validator.
    """
    def check(cat_a: "Category", cat_b: "Category") -> None:
        # Build parent index
        try:
            parent_cols = [cat_b[p] for p in parent_items]
        except (KeyError, AttributeError):
            return
        parent_index: Set[Tuple[str, ...]] = set()
        for row_vals in zip(*parent_cols):
            if any(_is_null(v) for v in row_vals):
                continue
            parent_index.add(row_vals)
        if not parent_index:
            return
        # Check child rows
        try:
            child_cols = [cat_a[c] for c in child_items]
        except (KeyError, AttributeError):
            return
        for row_vals in zip(*child_cols):
            if any(_is_null(v) for v in row_vals):
                continue
            if row_vals not in parent_index:
                pairs = ", ".join(
                    f"{child_items[i]}='{row_vals[i]}'"
                    for i in range(len(child_items))
                )
                raise ValidationError(
                    f"Composite key ({pairs}) does not exist in parent "
                    f"category '{cat_b.name}'",
                    path=cat_a.name,
                    severity=severity,
                )
    return check


def oper_expression(
    expression_item: str = "oper_expression",
    oper_list_item: str = "id",
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category", "Category"], None]:
    """Cross-checker: validate that operation expression references in cat_a
    all resolve to valid IDs in cat_b (``_pdbx_struct_oper_list``).

    Parses expressions like ``(1-60)``, ``(1,2,5)``, ``(X0)(1-5,11-15)``.

    Mirrors the oper_expression validation from the PDBeurope/mmcif-validator.
    """
    _group_re = re.compile(r"\(([^)]+)\)")

    def _parse_ids(expr: str) -> Set[str]:
        ids: Set[str] = set()
        expr = expr.strip()
        if not expr.startswith("("):
            if expr:
                ids.add(expr)
            return ids
        for group in _group_re.findall(expr):
            for part in group.split(","):
                part = part.strip()
                if "-" in part:
                    rng = part.split("-", 1)
                    try:
                        for i in range(int(rng[0]), int(rng[1]) + 1):
                            ids.add(str(i))
                    except ValueError:
                        ids.add(part)
                else:
                    ids.add(part)
        return ids

    def check(cat_a: "Category", cat_b: "Category") -> None:
        if not _has_value(cat_a, expression_item):
            return
        try:
            valid_ids = set(
                v for v in cat_b[oper_list_item] if not _is_null(v)
            )
        except (KeyError, AttributeError):
            return
        for val in cat_a[expression_item]:
            if _is_null(val):
                continue
            for ref_id in _parse_ids(val):
                if ref_id not in valid_ids:
                    raise ValidationError(
                        f"Operation expression '{val}' references ID "
                        f"'{ref_id}' which does not exist in "
                        f"'{cat_b.name}'",
                        path=cat_a.name,
                        severity=severity,
                    )
    return check


# ===================================================================
# Cross-category rule factories
# ===================================================================

def cross_mandatory(
    required_items: Sequence[str],
    severity: ValidationSeverity = ValidationSeverity.ERROR,
) -> Callable[["Category", "Category"], None]:
    """Cross-checker: *required_items* must exist in the second category."""
    def check(cat_a: "Category", cat_b: "Category") -> None:
        for item in required_items:
            if not _has_value(cat_b, item):
                raise ValidationError(
                    f"'{item}' is required in '{cat_b.name}' "
                    f"when '{cat_a.name}' is present",
                    path=cat_b.name,
                    severity=severity,
                )
    return check


def cross_ordering(
    item_a: str,
    item_b: str,
    op: str = "<",
    severity: ValidationSeverity = ValidationSeverity.WARNING,
) -> Callable[["Category", "Category"], None]:
    """Cross-checker: compare a value in cat_a against a value in cat_b."""
    ops = {
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
    }
    cmp = ops[op]

    def check(cat_a: "Category", cat_b: "Category") -> None:
        if not _has_value(cat_a, item_a) or not _has_value(cat_b, item_b):
            return
        try:
            na = float(cat_a[item_a][0])
            nb = float(cat_b[item_b][0])
        except (ValueError, IndexError):
            return
        if not cmp(na, nb):
            raise ValidationError(
                f"'{cat_a.name}.{item_a}' ({na}) must be {op} "
                f"'{cat_b.name}.{item_b}' ({nb})",
                path=cat_a.name,
                severity=severity,
            )
    return check


# ===================================================================
# Dictionary-driven validator
# ===================================================================

_E = ValidationSeverity.ERROR
_W = ValidationSeverity.WARNING


class DictionaryValidator(ValidatorPlugin):
    """Validator auto-generated from an mmCIF dictionary.

    Parses a ``.dic`` file via SLOTH's
    :class:`~sloth.mmcif.serializer.DictionaryParser`, retains the schema
    metadata, and registers validators from it:

    * **mandatory items** — from ``_item.mandatory_code``
    * **enumeration** — from ``_item_enumeration.value``
    * **type regex** — from ``_item_type_list.construct``
    * **foreign-key integrity** — single-key relationships
    * **composite-key integrity** — multi-key relationships
    * **parent/child category presence** — parent must exist when child does

    Parameters
    ----------
    dict_path : str, optional
        Path to an mmCIF dictionary.  Defaults to the bundled
        ``mmcif_pdbx_v50.dic``.
    quiet : bool
        Suppress progress messages from the dictionary parser (default True).

    Usage::

        from sloth.mmcif.rules import DictionaryValidator

        v = DictionaryValidator()          # schema-only rules
        handler.register("validate", v)
    """

    def __init__(self, dict_path: Optional[str] = None, *, quiet: bool = True):
        super().__init__()
        self._schema = self._load_schema(dict_path, quiet)
        self._register_schema_rules()

    # ------------------------------------------------------------------
    # Schema loading (via serializer.py)
    # ------------------------------------------------------------------

    @staticmethod
    def _load_schema(
        dict_path: Optional[str], quiet: bool,
    ) -> Dict:
        from pathlib import Path as _Path
        from .serializer import DictionaryParser, get_cache_manager
        from .defaults import DictDataType

        if dict_path is None:
            dict_path = str(
                _Path(__file__).parent / "schemas" / "mmcif_pdbx_v50.dic"
            )

        dp = DictionaryParser(get_cache_manager(), quiet=quiet)
        meta = dp.parse(dict_path)

        return {
            "dict_path": dict_path,
            "categories": meta.get(DictDataType.CATEGORIES.value, {}),
            "items": meta.get(DictDataType.ITEMS.value, {}),
            "enumerations": meta.get(DictDataType.ENUMERATIONS.value, {}),
            "relationships": meta.get(DictDataType.RELATIONSHIPS.value, []),
            "item_types": meta.get(DictDataType.ITEM_TYPES.value, {}),
        }

    # ------------------------------------------------------------------
    # Schema → validator registration
    # ------------------------------------------------------------------

    def _register_schema_rules(self) -> None:
        from .defaults import DictItemKey, RelationshipKey

        items = self._schema["items"]
        enumerations = self._schema["enumerations"]
        item_types_map = self._schema["item_types"]
        relationships = self._schema["relationships"]

        self._register_mandatory(items, DictItemKey)
        self._register_enumerations(enumerations)
        self._register_type_checks(items, item_types_map, DictItemKey)
        self._register_relationships(relationships, RelationshipKey)

    def _register_mandatory(self, items: Dict, DictItemKey: type) -> None:
        cat_mandatory: Dict[str, List[str]] = {}
        for full_name, item_data in items.items():
            code = item_data.get(DictItemKey.ITEM_MANDATORY_CODE.value, "")
            if code.strip().lower() in ("yes", "y", "true"):
                parts = full_name.lstrip("_").split(".", 1)
                if len(parts) == 2:
                    cat_mandatory.setdefault(parts[0], []).append(parts[1])
        for cat_name, fields in cat_mandatory.items():
            self.register_validator(f"_{cat_name}", mandatory_items(fields))

    def _register_enumerations(self, enumerations: Dict) -> None:
        for full_name, allowed in enumerations.items():
            parts = full_name.lstrip("_").split(".", 1)
            if len(parts) == 2 and isinstance(allowed, list) and allowed:
                self.register_validator(
                    f"_{parts[0]}",
                    enumeration_check(parts[1], allowed),
                )

    def _register_type_checks(
        self, items: Dict, item_types_map: Dict, DictItemKey: type,
    ) -> None:
        type_regexes: Dict[str, str] = {}
        for code, row in item_types_map.items():
            construct = row.get("construct") or row.get("regex")
            if not construct:
                continue
            raw = construct.strip()
            if raw.startswith(";"):
                continue
            if (raw.startswith('"') and raw.endswith('"')) or \
               (raw.startswith("'") and raw.endswith("'")):
                raw = raw[1:-1]
            if not raw:
                continue
            try:
                re.compile(raw)
            except re.error:
                continue
            type_regexes[code] = raw

        for full_name, item_data in items.items():
            type_code = item_data.get(DictItemKey.ITEM_TYPE_CODE.value, "")
            regex = type_regexes.get(type_code)
            if regex:
                parts = full_name.lstrip("_").split(".", 1)
                if len(parts) == 2:
                    self.register_validator(
                        f"_{parts[0]}",
                        type_check(parts[1], regex, type_code),
                    )

    def _register_relationships(
        self, relationships: List, RelationshipKey: type,
    ) -> None:
        rel_groups: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        for rel in relationships:
            child_name = (
                rel.get(RelationshipKey.ITEM_LINKED_CHILD_NAME.value)
                or rel.get(RelationshipKey.CHILD_NAME.value)
            )
            parent_name = (
                rel.get(RelationshipKey.ITEM_LINKED_PARENT_NAME.value)
                or rel.get(RelationshipKey.PARENT_NAME.value)
            )
            child_cat = rel.get(RelationshipKey.CHILD_CATEGORY.value)
            parent_cat = rel.get(RelationshipKey.PARENT_CATEGORY.value)
            if not child_name or not parent_name:
                continue
            if child_cat and parent_cat:
                c_field = child_name.lstrip("_").split(".")[-1] if "." in child_name else child_name
                p_field = parent_name.lstrip("_").split(".")[-1] if "." in parent_name else parent_name
            else:
                c_parts = child_name.strip("_").split(".")
                p_parts = parent_name.strip("_").split(".")
                if len(c_parts) != 2 or len(p_parts) != 2:
                    continue
                child_cat, c_field = c_parts
                parent_cat, p_field = p_parts
            rel_groups.setdefault((child_cat, parent_cat), []).append(
                (c_field, p_field)
            )

        for key in rel_groups:
            rel_groups[key] = list(dict.fromkeys(rel_groups[key]))

        seen_pc: Set[Tuple[str, str]] = set()
        for (child_cat, parent_cat), pairs in rel_groups.items():
            pc_key = (child_cat, parent_cat)
            if pc_key not in seen_pc:
                seen_pc.add(pc_key)
                self.register_cross_checker(
                    (f"_{child_cat}", f"_{parent_cat}"),
                    parent_child(),
                )
            if len(pairs) == 1:
                c_field, p_field = pairs[0]
                self.register_cross_checker(
                    (f"_{child_cat}", f"_{parent_cat}"),
                    foreign_key(c_field, p_field),
                )
            else:
                c_fields = [p[0] for p in pairs]
                p_fields = [p[1] for p in pairs]
                self.register_cross_checker(
                    (f"_{child_cat}", f"_{parent_cat}"),
                    composite_key(c_fields, p_fields),
                )


# ===================================================================
# wwPDB deposition validator
# ===================================================================

class MmcifValidator(DictionaryValidator):
    """Full wwPDB validator = dictionary schema + deposition business rules.

    Inherits all schema-level checks from :class:`DictionaryValidator`, then
    registers wwPDB-specific rules from the declarative tables below.

    Parameters
    ----------
    dict_path : str, optional
        Path to an mmCIF dictionary.  Defaults to the bundled
        ``mmcif_pdbx_v50.dic``.
    quiet : bool
        Suppress dictionary parser progress messages (default True).

    Usage::

        from sloth.mmcif.rules import MmcifValidator

        v = MmcifValidator()               # full wwPDB + dictionary rules
        handler.register("validate", v)
    """

    # ------------------------------------------------------------------
    # Declarative wwPDB rule tables
    # ------------------------------------------------------------------
    # Items required by wwPDB deposition beyond dictionary mandatory_code.
    # Format: (category, items [, exclude])
    _MANDATORY = [
        ("_refine_ls_shell", ["d_res_low"], ["pdbx_refine_id", "pdbx_ordinal"]),
        ("_pdbx_depui_status_flags", ["has_accepted_assemblies"]),
        ("_em_imaging", ["nominal_defocus_min", "nominal_defocus_max"]),
        ("_em_image_recording", ["avg_electron_dose_per_subtomogram"]),
        ("_em_3d_reconstruction", ["resolution", "resolution_method"]),
        ("_entity_poly", ["pdbx_seq_one_letter_code"]),
        ("_diffrn_source", ["pdbx_wavelength_list"]),
        ("_pdbx_initial_refinement_model", ["source_name", "accession_code"]),
    ]

    # At least one of these items must be non-null.
    # Format: (category, items)
    _ONE_OF = [
        ("_reflns", [
            "pdbx_CC_half", "pdbx_Rmerge_I_obs", "pdbx_Rsym_value",
            "pdbx_Rpim_I_all", "pdbx_Rrim_I_all", "pdbx_R_split",
        ]),
        ("_reflns_shell", [
            "pdbx_CC_half", "Rmerge_I_obs", "pdbx_Rsym_value",
            "pdbx_Rpim_I_all", "pdbx_Rrim_I_all", "pdbx_R_split",
        ]),
        ("_refine", [
            "ls_R_factor_obs", "ls_R_factor_R_work", "ls_R_factor_R_free",
        ]),
    ]

    # Items required when a trigger item has certain values.
    # Format: (category, required_items, when_item, when_values)
    _CONDITIONAL = [
        ("_pdbx_initial_refinement_model", ["details"], "source_name", ["Other"]),
        ("_pdbx_initial_refinement_model", ["details"], "type", ["other"]),
        ("_software", ["version"], "name", ["PHENIX", "REFMAC"]),
        ("_refine", ["pdbx_starting_model"],
         "pdbx_method_to_determine_struct", ["MOLECULAR REPLACEMENT"]),
        ("_em_entity_assembly_molwt", ["units", "value"],
         "experimental_flag", ["YES"]),
        ("_em_start_model", ["emdb_id"], "type", ["EMDB MAP"]),
        ("_em_start_model", ["pdb_id"], "type", ["PDB ENTRY"]),
        ("_em_start_model",
         ["orthogonal_tilt_num_images", "orthogonal_tilt_angle1",
          "orthogonal_tilt_angle2"],
         "type", ["ORTHOGONAL TILT"]),
        ("_em_start_model",
         ["random_conical_tilt_num_images", "random_conical_tilt_angle"],
         "type", ["RANDOM CONICAL TILT"]),
        ("_em_3d_reconstruction", ["fsc_type"], "resolution_method",
         ["FSC 0.5", "FSC 0.33", "FSC 0.143", "3 SIGMA", "1/2 BIT CUT-OFF"]),
        ("_em_3d_fitting_list", ["details"], "type", ["other"]),
        ("_em_3d_fitting_list", ["details"], "source_name", ["Other"]),
        ("_em_3d_fitting_list", ["accession_code"], "source_name", ["PDB"]),
        ("_em_software", ["name"], "category",
         ["RECONSTRUCTION", "PARTICLE SELECTION",
          "VOLUME SELECTION", "SERIES ALIGNMENT"]),
        ("_em_software", ["version"], "name", ["RELION"]),
        ("_pdbx_audit_support", ["country", "details"],
         "funding_organization", ["Other government", "Other private"]),
        ("_pdbx_struct_ref_seq_depositor_info", ["db_accession"],
         "db_name", ["GB", "UNP"]),
    ]

    # Restrict allowed (item_a, item_b) value combinations.
    # Format: (category, item_a, item_b, mapping)
    _ALLOWED_PAIRS = [
        ("_pdbx_initial_refinement_model", "type", "source_name", {
            "experimental model": ["PDB", "Other"],
            "other": ["Other"],
        }),
        ("_pdbx_initial_refinement_model", "source_name", "type", {
            "PDB": ["experimental model"],
        }),
        ("_em_3d_fitting_list", "type", "source_name", {
            "experimental model": ["PDB", "Other"],
            "integrative model": ["Other"],
            "other": ["Other"],
        }),
        ("_em_3d_fitting_list", "source_name", "type", {
            "PDB": ["experimental model"],
        }),
    ]

    # Values must match a regex.
    # Format: (category, item, pattern, error_text)
    _REGEX = [
        ("_pdbx_database_related", "db_id",
         r"^(D_\d{10}|[0-9][A-Za-z0-9]{3}|pdb_\d{13})$",
         "db_id must be a PDB ID (e.g. 1csb), extended PDB ID "
         "(e.g. pdb_0000000001csb), or deposition ID (e.g. D_1000000001)"),
        ("_em_3d_fitting_list", "accession_code",
         r"^(pdb_0000)?[\w\d]{4}$",
         "Please provide a valid PDB accession code "
         "(e.g. 1csb, pdb_00001csb)"),
    ]

    # Numeric ordering between two items in the same category.
    # Format: (category, item_a, item_b, op, severity)
    _ORDERING = [
        ("_em_imaging", "nominal_defocus_min", "nominal_defocus_max", "<=", _W),
        ("_em_imaging", "calibrated_defocus_min", "calibrated_defocus_max", "<=", _W),
        ("_em_imaging", "recording_temperature_min", "recording_temperature_max", "<=", _W),
        ("_refine", "ls_R_factor_R_work", "ls_R_factor_R_free", "<", _W),
        ("_em_focused_ion_beam", "initial_thickness", "final_thickness", "<", _W),
        ("_reflns_shell", "d_res_low", "d_res_high", "<", _W),
        ("_refine_ls_shell", "d_res_low", "d_res_high", "<", _W),
        ("_refine_ls_shell", "R_factor_R_work", "R_factor_R_free", "<", _W),
        ("_pdbx_nmr_ensemble", "conformers_submitted_total_number",
         "conformers_calculated_total_number", "<=", _W),
    ]

    # Numeric ordering across two categories.
    # Format: (cat_a, cat_b, item_a, item_b, op, severity)
    _CROSS_ORDERING = [
        ("_em_3d_reconstruction", "_em_diffraction_stats",
         "resolution", "high_resolution", "<", _W),
        ("_em_diffraction_shell", "_em_diffraction_stats",
         "high_resolution", "high_resolution", "<", _W),
        ("_em_diffraction_shell", "_em_diffraction_stats",
         "num_structure_factors", "num_structure_factors", "<=", _W),
        ("_pdbx_nmr_representative", "_pdbx_nmr_ensemble",
         "conformer_id", "conformers_submitted_total_number", "<=", _W),
    ]

    # String length bounds.
    # Format: (category, item, min_len, max_len, severity)
    _VALUE_LENGTH = [
        ("_struct", "title", 10, 300, _E),
        ("_struct", "title", 20, 200, _W),
        ("_em_admin", "title", 10, None, _E),
        ("_em_admin", "title", 20, None, _W),
        ("_citation", "title", 10, None, _E),
        ("_citation", "title", 20, None, _W),
    ]

    # Numeric bounds.
    # Format: (category, item, min_val, max_val, severity)
    _VALUE_RANGE = [
        ("_em_imaging", "nominal_defocus_min", 0, None, _E),
        ("_em_imaging", "nominal_defocus_max", 0, None, _E),
        ("_em_imaging", "nominal_defocus_min", 0, 200, _W),
        ("_em_imaging", "nominal_defocus_max", 0, 200, _W),
        ("_em_imaging", "calibrated_defocus_min", 0, None, _E),
        ("_em_imaging", "calibrated_defocus_max", 0, None, _E),
        ("_em_imaging", "calibrated_defocus_min", 0, 200, _W),
        ("_em_imaging", "calibrated_defocus_max", 0, 200, _W),
    ]

    # Minimum row count per category.
    # Format: (category, n)
    _MIN_ROWS = [
        ("_audit_author", 2),
        ("_citation_author", 2),
    ]

    # Items required in cat_b when cat_a is present.
    # Format: (cat_a, cat_b, required_items)
    _CROSS_MANDATORY = [
        ("_refine", "_pdbx_initial_refinement_model", ["type", "source_name"]),
    ]

    # ------------------------------------------------------------------

    def __init__(self, dict_path: Optional[str] = None, *, quiet: bool = True):
        super().__init__(dict_path, quiet=quiet)
        self._register_wwpdb_rules()

    def _register_wwpdb_rules(self) -> None:
        for rule in self._MANDATORY:
            cat, items_ = rule[0], rule[1]
            exclude = rule[2] if len(rule) > 2 else ()
            self.register_validator(cat, mandatory_items(items_, exclude=exclude))

        for cat, items_ in self._ONE_OF:
            self.register_validator(cat, one_of_following(items_))

        for cat, req, when, vals in self._CONDITIONAL:
            self.register_validator(
                cat, conditional_mandatory(req, when, vals),
            )

        for cat, ia, ib, mapping in self._ALLOWED_PAIRS:
            self.register_validator(cat, allowed_pairs(ia, ib, mapping))

        for cat, item, pattern, err in self._REGEX:
            self.register_validator(
                cat, regex_check(item, pattern, error_text=err),
            )

        for cat, ia, ib, op, sev in self._ORDERING:
            self.register_validator(cat, ordering_check(ia, ib, op, severity=sev))

        for ca, cb, ia, ib, op, sev in self._CROSS_ORDERING:
            self.register_cross_checker(
                (ca, cb), cross_ordering(ia, ib, op, severity=sev),
            )

        for cat, item, mn, mx, sev in self._VALUE_LENGTH:
            self.register_validator(
                cat, value_length(item, min_len=mn, max_len=mx, severity=sev),
            )

        for cat, item, mn, mx, sev in self._VALUE_RANGE:
            self.register_validator(
                cat, value_range(item, min_val=mn, max_val=mx, severity=sev),
            )

        for cat, n in self._MIN_ROWS:
            self.register_validator(cat, min_rows(n))

        for ca, cb, items_ in self._CROSS_MANDATORY:
            self.register_cross_checker((ca, cb), cross_mandatory(items_))

