"""
SLOTH Validation Exceptions

This module provides validation exception classes used throughout SLOTH.
"""

from enum import Enum, auto


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
