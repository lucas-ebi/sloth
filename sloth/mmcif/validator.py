"""
SLOTH Validation

Validation exception classes, the :class:`ValidatorPlugin` that powers
per-category and cross-category checks, :class:`CategoryValidator` (the
chainable wrapper), higher-level :class:`DataBlockValidator` /
:class:`ContainerValidator` plugins that collect errors into a
:class:`ValidationReport`, and a library of composable rule factories plus
ready-to-use validator classes (:class:`SchemaValidator`,
:class:`MMCIFValidator`).
"""

import re
from typing import (
    Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple,
    Union, TYPE_CHECKING,
)

from .plugins import Plugin, PluginWrapper
from .defaults import DataValue, DictItemKey, RelationshipKey, ValidationSeverity

if TYPE_CHECKING:
    from .models import Category, DataBlock, MMCIFDataContainer


class ValidationError(Exception):
    """Exception raised for validation errors."""

    def __init__(
        self,
        message: str,
        path: str = "",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ):
        """
        Initialize validation error.

        Args:
            message: Error message
            path: Path where the error occurred (e.g., JSON path, category name)
            severity: Validation error severity
        """
        self.message = message
        self.path = path
        self.severity = severity
        if path:
            super().__init__(f"{path}: {message}")
        else:
            super().__init__(message)


# ---------------------------------------------------------------------------
# Validation plugin
# ---------------------------------------------------------------------------

class ValidatorPlugin(Plugin):
    """Plugin for per-category validation with cross-checker support.

    Multiple validators can be registered for the same category — they
    will all run in registration order.

    Use :meth:`validate` for a standalone convenience entry-point that
    accepts any level of the data hierarchy and returns a
    :class:`ValidationReport`.
    """

    def __init__(self):
        self._validators: Dict[str, List[Callable]] = {}
        self._cross_checkers: Dict[Tuple[str, str], List[Callable]] = {}

    # -- registration helpers -----------------------------------------------

    def register_validator(
        self, category_name: str, validator_function: Callable
    ) -> None:
        """Register a validator callable for a category name.

        Multiple validators for the same category are allowed.
        """
        self._validators.setdefault(category_name, []).append(validator_function)

    def register_cross_checker(
        self,
        category_pair: Tuple[str, str],
        cross_checker_function: Callable,
    ) -> None:
        """Register a cross-checker callable for a pair of category names."""
        self._cross_checkers.setdefault(category_pair, []).append(cross_checker_function)

    def merge(self, other: "ValidatorPlugin") -> "ValidatorPlugin":
        """Return a **new** ValidatorPlugin whose rules are *self* + *other*.

        Rules from *other* run **after** rules from *self* for each
        category / cross-checker pair.  Neither the receiver nor the
        argument is mutated.
        """
        merged = ValidatorPlugin()
        # Copy self
        for cat, fns in self._validators.items():
            merged._validators.setdefault(cat, []).extend(fns)
        for pair, fns in self._cross_checkers.items():
            merged._cross_checkers.setdefault(pair, []).extend(fns)
        # Append other
        for cat, fns in other._validators.items():
            merged._validators.setdefault(cat, []).extend(fns)
        for pair, fns in other._cross_checkers.items():
            merged._cross_checkers.setdefault(pair, []).extend(fns)
        return merged

    # -- lookup helpers -----------------------------------------------------

    def get_validators(self, category_name: str) -> List[Callable]:
        """Return all validators for *category_name*."""
        return self._validators.get(category_name, [])

    def get_cross_checkers(
        self, category_pair: Tuple[str, str]
    ) -> List[Callable]:
        """Return all cross-checkers for *category_pair*."""
        return self._cross_checkers.get(category_pair, [])

    # -- Plugin interface ---------------------------------------------------

    def create_wrapper(self, target) -> "CategoryValidator":
        return CategoryValidator(target, self)

    def execute(self, target, *args, **kwargs) -> Any:
        results = []
        for validator in self._validators.get(target.name, []):
            result = validator(target)
            if result is not None:
                results.append(result)
        return results or None

    # -- standalone convenience ---------------------------------------------

    def validate(
        self,
        data: Union["Category", "DataBlock", "MMCIFDataContainer"],
    ) -> "ValidationReport":
        """Validate *data* and return a :class:`ValidationReport`.

        Accepts any level of the hierarchy:

        * :class:`Category` — runs per-category validators only.
        * :class:`DataBlock` — runs per-category validators **and**
          cross-checkers for all categories in the block.
        * :class:`MMCIFDataContainer` — validates each block in turn.

        :param data: The data object to validate.
        :return: A :class:`ValidationReport` with all collected issues.
        """
        from .models import Category, DataBlock, MMCIFDataContainer

        if isinstance(data, Category):
            return self._validate_category(data)
        if isinstance(data, DataBlock):
            return self._validate_block(data)
        if isinstance(data, MMCIFDataContainer):
            report = ValidationReport()
            for block_name in data.blocks:
                report.extend(self._validate_block(data[block_name]))
            return report
        raise TypeError(
            f"Expected Category, DataBlock, or MMCIFDataContainer, "
            f"got {type(data).__name__}"
        )

    def _validate_category(self, category: "Category") -> "ValidationReport":
        report = ValidationReport()
        for validator_fn in self._validators.get(category.name, []):
            try:
                validator_fn(category)
            except ValidationError as exc:
                report.add(exc)
        return report

    def _validate_block(self, block: "DataBlock") -> "ValidationReport":
        report = ValidationReport()
        # Per-category validators
        for cat_name in block.categories:
            category = block[cat_name]
            for validator_fn in self._validators.get(cat_name, []):
                try:
                    validator_fn(category)
                except ValidationError as exc:
                    report.add(exc)
        # Cross-checkers
        for (cat_a, cat_b), checkers in self._cross_checkers.items():
            if cat_a not in block.categories or cat_b not in block.categories:
                continue
            try:
                a, b = block[cat_a], block[cat_b]
            except (KeyError, AttributeError):
                continue
            for checker in checkers:
                try:
                    checker(a, b)
                except ValidationError as exc:
                    report.add(exc)
        return report


class CategoryValidator(PluginWrapper):
    """Chainable wrapper for category validation with cross-checking."""

    _plugin: "ValidatorPlugin"

    def __call__(self) -> "CategoryValidator":
        """Execute the registered validator for this category."""
        super().__call__()
        return self

    def against(self, other_category: "Category") -> "CategoryValidator":
        """Execute cross-validation against *other_category*."""
        for cross_checker in self._plugin.get_cross_checkers(
            (self._target.name, other_category.name)
        ):
            cross_checker(self._target, other_category)
        return self


# ---------------------------------------------------------------------------
# Validation report
# ---------------------------------------------------------------------------

class ValidationReport:
    """Collects validation errors from a recursive validation pass.

    Returned by :meth:`DataBlockValidator.execute`,
    :meth:`ContainerValidator.execute`, and
    :meth:`ValidatorPlugin.validate`.
    """

    def __init__(self):
        self._issues: List[ValidationError] = []

    # -- mutators -----------------------------------------------------------

    def add(self, error: ValidationError) -> None:
        """Append a single :class:`ValidationError`."""
        self._issues.append(error)

    def extend(self, other: "ValidationReport") -> None:
        """Merge all issues from *other* into this report."""
        self._issues.extend(other._issues)

    # -- queries ------------------------------------------------------------

    @property
    def all_issues(self) -> List[ValidationError]:
        """Every collected issue regardless of severity."""
        return list(self._issues)

    @property
    def errors(self) -> List[ValidationError]:
        """Only :attr:`ValidationSeverity.ERROR` issues."""
        return [e for e in self._issues if e.severity == ValidationSeverity.ERROR]

    @property
    def warnings(self) -> List[ValidationError]:
        """Only :attr:`ValidationSeverity.WARNING` issues."""
        return [e for e in self._issues if e.severity == ValidationSeverity.WARNING]

    @property
    def is_valid(self) -> bool:
        """``True`` when no ERROR-level issues are present."""
        return all(e.severity != ValidationSeverity.ERROR for e in self._issues)

    def raise_on_error(self) -> None:
        """Raise :class:`ValidationError` if any errors are present."""
        errs = self.errors
        if errs:
            summary = "; ".join(str(e) for e in errs[:5])
            if len(errs) > 5:
                summary += f" ... and {len(errs) - 5} more"
            raise ValidationError(
                f"{len(errs)} validation error(s): {summary}"
            )

    # -- dunder -------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._issues)

    def __repr__(self) -> str:
        return (
            f"ValidationReport(errors={len(self.errors)}, "
            f"warnings={len(self.warnings)})"
        )

    def __str__(self) -> str:
        lines: List[str] = []
        if self.is_valid:
            lines.append("Validation passed")
            if self.warnings:
                lines.append(f"  ({len(self.warnings)} warning(s))")
        else:
            lines.append(
                f"Validation failed: {len(self.errors)} error(s), "
                f"{len(self.warnings)} warning(s)"
            )
        for e in self.errors:
            lines.append(f"  ERROR   {e}")
        for w in self.warnings:
            lines.append(f"  WARNING {w}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Block-level validator
# ---------------------------------------------------------------------------

class DataBlockValidator(Plugin):
    """Validates every category in a :class:`DataBlock`.

    Runs all per-category validators **and** cross-checkers registered on the
    wrapped :class:`ValidatorPlugin`, collecting errors into a
    :class:`ValidationReport` rather than raising on the first failure.
    """

    def __init__(self, category_validator: ValidatorPlugin):
        self._category_validator = category_validator

    def create_wrapper(self, target: "DataBlock") -> "BlockValidationWrapper":
        return BlockValidationWrapper(target, self)

    def execute(self, target: "DataBlock", *args, **kwargs) -> ValidationReport:
        report = ValidationReport()

        # Per-category validators
        for cat_name in target.categories:
            category = target[cat_name]
            for validator_fn in self._category_validator.get_validators(cat_name):
                try:
                    validator_fn(category)
                except ValidationError as exc:
                    report.add(exc)

        # Cross-checkers
        for (cat_a, cat_b), checkers in self._category_validator._cross_checkers.items():
            if cat_a not in target.categories or cat_b not in target.categories:
                continue
            try:
                a, b = target[cat_a], target[cat_b]
            except (KeyError, AttributeError):
                continue
            for checker in checkers:
                try:
                    checker(a, b)
                except ValidationError as exc:
                    report.add(exc)

        return report


class BlockValidationWrapper(PluginWrapper):
    """Chainable wrapper for block-level validation."""

    @property
    def report(self) -> Optional[ValidationReport]:
        """Shortcut for :attr:`result` — the :class:`ValidationReport`."""
        return self._result

    @property
    def is_valid(self) -> Optional[bool]:
        """``True`` when validation passed (no errors). ``None`` if not yet run."""
        return self._result.is_valid if self._result else None


# ---------------------------------------------------------------------------
# Container-level validator
# ---------------------------------------------------------------------------

class ContainerValidator(Plugin):
    """Validates every block in an :class:`MMCIFDataContainer`.

    Delegates to a :class:`DataBlockValidator` for each block and merges all
    results into a single :class:`ValidationReport`.
    """

    def __init__(self, block_validator: DataBlockValidator):
        self._block_validator = block_validator

    def create_wrapper(self, target: "MMCIFDataContainer") -> "ContainerValidationWrapper":
        return ContainerValidationWrapper(target, self)

    def execute(self, target: "MMCIFDataContainer", *args, **kwargs) -> ValidationReport:
        report = ValidationReport()
        for block_name in target.blocks:
            block_report = self._block_validator.execute(target[block_name])
            report.extend(block_report)
        return report


class ContainerValidationWrapper(PluginWrapper):
    """Chainable wrapper for container-level validation."""

    @property
    def report(self) -> Optional[ValidationReport]:
        """Shortcut for :attr:`result` — the :class:`ValidationReport`."""
        return self._result

    @property
    def is_valid(self) -> Optional[bool]:
        """``True`` when validation passed (no errors). ``None`` if not yet run."""
        return self._result.is_valid if self._result else None


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


class SchemaValidator(ValidatorPlugin):
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

        from sloth.mmcif.validator import SchemaValidator

        v = SchemaValidator()          # schema-only rules
        report = v.validate(block)     # validate a DataBlock
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

class MMCIFValidator(SchemaValidator):
    """Full wwPDB validator = dictionary schema + deposition business rules.

    Inherits all schema-level checks from :class:`SchemaValidator`, then
    registers wwPDB-specific rules from the declarative tables below.

    Parameters
    ----------
    dict_path : str, optional
        Path to an mmCIF dictionary.  Defaults to the bundled
        ``mmcif_pdbx_v50.dic``.
    quiet : bool
        Suppress dictionary parser progress messages (default True).

    Usage::

        from sloth.mmcif.validator import MMCIFValidator

        v = MMCIFValidator()               # full wwPDB + dictionary rules
        report = v.validate(container)     # validate an entire container
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

