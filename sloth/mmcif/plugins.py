from typing import Callable, Dict, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Category


class ValidatorFactory:
    """A factory class for creating validators and cross-checkers."""

    def __init__(self):
        self.validators: Dict[str, Callable[["Category"], None]] = {}
        self.cross_checkers: Dict[Tuple[str, str], Callable[["Category", "Category"], None]] = {}

    def register_validator(
        self, category_name: str, validator_function: Callable[["Category"], None]
    ) -> None:
        """
        Registers a validator function for a category.

        :param category_name: The name of the category.
        :type category_name: str
        :param validator_function: The validator function that receives a Category object.
        :type validator_function: Callable[[Category], None]
        :return: None
        """
        self.validators[category_name] = validator_function

    def register_cross_checker(
        self,
        category_pair: Tuple[str, str],
        cross_checker_function: Callable[["Category", "Category"], None],
    ) -> None:
        """
        Registers a cross-checker function for a pair of categories.

        :param category_pair: The pair of category names.
        :type category_pair: Tuple[str, str]
        :param cross_checker_function: The cross-checker function that receives two Category objects.
        :type cross_checker_function: Callable[[Category, Category], None]
        :return: None
        """
        self.cross_checkers[category_pair] = cross_checker_function

    def get_validator(self, category_name: str) -> Optional[Callable[["Category"], None]]:
        """
        Retrieves a validator function for a category.

        :param category_name: The name of the category.
        :type category_name: str
        :return: The validator function that receives a Category object.
        :rtype: Optional[Callable[[Category], None]]
        """
        return self.validators.get(category_name)

    def get_cross_checker(
        self, category_pair: Tuple[str, str]
    ) -> Optional[Callable[["Category", "Category"], None]]:
        """
        Retrieves a cross-checker function for a pair of categories.

        :param category_pair: The pair of category names.
        :type category_pair: Tuple[str, str]
        :return: The cross-checker function that receives two Category objects.
        :rtype: Optional[Callable[[Category, Category], None]]
        """
        return self.cross_checkers.get(category_pair)


class CategoryValidator:
    """A class to validate a category - extracted from Category.Validator"""

    def __init__(self, category: "Category", factory: ValidatorFactory):
        self._category = category
        self._factory = factory
        self._other_category: Optional["Category"] = None

    def __call__(self) -> "CategoryValidator":
        """Execute validation for the category"""
        validator = self._factory.get_validator(self._category.name)
        if validator:
            validator(self._category)
        return self

    def against(self, other_category: "Category") -> "CategoryValidator":
        """Execute cross-validation against another category"""
        self._other_category = other_category
        cross_checker = self._factory.get_cross_checker(
            (self._category.name, other_category.name)
        )
        if cross_checker:
            cross_checker(self._category, other_category)
        return self
