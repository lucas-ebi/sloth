"""
SLOTH Validation

Validation exception classes, the :class:`ValidatorPlugin` that powers
per-category and cross-category checks, :class:`CategoryValidator` (the
chainable wrapper), and higher-level :class:`BlockValidator` /
:class:`ContainerValidator` plugins that collect errors into a
:class:`ValidationReport`.
"""

from enum import Enum, auto
from typing import Any, Callable, Dict, List, Tuple, Optional, TYPE_CHECKING

from .plugins import Plugin, PluginWrapper

if TYPE_CHECKING:
    from .models import Category, DataBlock, MMCIFDataContainer


class ValidationSeverity(Enum):
    """Severity levels for validation errors."""

    ERROR = auto()  # Validation failures that should prevent processing
    WARNING = auto()  # Issues that should be flagged but don't prevent processing
    INFO = auto()  # Informational notices


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

    Returned by :meth:`BlockValidator.execute`,
    :meth:`ContainerValidator.execute`, and
    :meth:`~sloth.mmcif.handler.MMCIFHandler.validate`.
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

class BlockValidator(Plugin):
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

    Delegates to a :class:`BlockValidator` for each block and merges all
    results into a single :class:`ValidationReport`.
    """

    def __init__(self, block_validator: BlockValidator):
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
