from typing import Callable, Dict, Tuple, Optional


class ValidatorFactory:
    """A factory class for creating validators and cross-checkers."""

    def __init__(self):
        self.validators: Dict[str, Callable[[str], None]] = {}
        self.cross_checkers: Dict[Tuple[str, str], Callable[[str, str], None]] = {}

    def register_validator(
        self, category_name: str, validator_function: Callable[[str], None]
    ) -> None:
        """
        Registers a validator function for a category.

        :param category_name: The name of the category.
        :type category_name: str
        :param validator_function: The validator function.
        :type validator_function: Callable[[str], None]
        :return: None
        """
        self.validators[category_name] = validator_function

    def register_cross_checker(
        self,
        category_pair: Tuple[str, str],
        cross_checker_function: Callable[[str, str], None],
    ) -> None:
        """
        Registers a cross-checker function for a pair of categories.

        :param category_pair: The pair of category names.
        :type category_pair: Tuple[str, str]
        :param cross_checker_function: The cross-checker function.
        :type cross_checker_function: Callable[[str, str], None]
        :return: None
        """
        self.cross_checkers[category_pair] = cross_checker_function

    def get_validator(self, category_name: str) -> Optional[Callable[[str], None]]:
        """
        Retrieves a validator function for a category.

        :param category_name: The name of the category.
        :type category_name: str
        :return: The validator function.
        :rtype: Optional[Callable[[str], None]]
        """
        return self.validators.get(category_name)

    def get_cross_checker(
        self, category_pair: Tuple[str, str]
    ) -> Optional[Callable[[str, str], None]]:
        """
        Retrieves a cross-checker function for a pair of categories.

        :param category_pair: The pair of category names.
        :type category_pair: Tuple[str, str]
        :return: The cross-checker function.
        :rtype: Optional[Callable[[str, str], None]]
        """
        return self.cross_checkers.get(category_pair)


class CategoryValidator:
    """A class to validate a category - extracted from Category.Validator"""

    def __init__(self, category_name: str, factory: ValidatorFactory):
        self._category_name = category_name
        self._factory = factory
        self._other_category_name: Optional[str] = None

    def __call__(self) -> "CategoryValidator":
        """Execute validation for the category"""
        validator = self._factory.get_validator(self._category_name)
        if validator:
            validator(self._category_name)
        return self

    def against(self, other_category_name: str) -> "CategoryValidator":
        """Execute cross-validation against another category"""
        self._other_category_name = other_category_name
        cross_checker = self._factory.get_cross_checker(
            (self._category_name, other_category_name)
        )
        if cross_checker:
            cross_checker(self._category_name, other_category_name)
        return self
