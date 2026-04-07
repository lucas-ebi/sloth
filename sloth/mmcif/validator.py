"""
SLOTH Validation

Validation exception classes, the :class:`ValidatorPlugin` that powers
per-category and cross-category checks, and :class:`CategoryValidator` (the
chainable wrapper).
"""

from enum import Enum, auto
from typing import Any, Callable, Dict, Tuple, Optional, TYPE_CHECKING

from .plugins import Plugin, PluginWrapper

if TYPE_CHECKING:
    from .models import Category


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
    """Plugin for per-category validation with cross-checker support."""

    def __init__(self):
        self._validators: Dict[str, Callable] = {}
        self._cross_checkers: Dict[Tuple[str, str], Callable] = {}

    # -- registration helpers -----------------------------------------------

    def register_validator(
        self, category_name: str, validator_function: Callable
    ) -> None:
        """Register a validator callable for a category name."""
        self._validators[category_name] = validator_function

    def register_cross_checker(
        self,
        category_pair: Tuple[str, str],
        cross_checker_function: Callable,
    ) -> None:
        """Register a cross-checker callable for a pair of category names."""
        self._cross_checkers[category_pair] = cross_checker_function

    # -- lookup helpers -----------------------------------------------------

    def get_validator(self, category_name: str) -> Optional[Callable]:
        return self._validators.get(category_name)

    def get_cross_checker(
        self, category_pair: Tuple[str, str]
    ) -> Optional[Callable]:
        return self._cross_checkers.get(category_pair)

    # -- Plugin interface ---------------------------------------------------

    def create_wrapper(self, target) -> "CategoryValidator":
        return CategoryValidator(target, self)

    def execute(self, target, *args, **kwargs) -> Any:
        validator = self._validators.get(target.name)
        if validator:
            return validator(target)
        return None


class CategoryValidator(PluginWrapper):
    """Chainable wrapper for category validation with cross-checking."""

    _plugin: "ValidatorPlugin"

    def __call__(self) -> "CategoryValidator":
        """Execute the registered validator for this category."""
        super().__call__()
        return self

    def against(self, other_category: "Category") -> "CategoryValidator":
        """Execute cross-validation against *other_category*."""
        cross_checker = self._plugin.get_cross_checker(
            (self._target.name, other_category.name)
        )
        if cross_checker:
            cross_checker(self._target, other_category)
        return self
