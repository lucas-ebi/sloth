Validation Rules
================

.. module:: sloth.mmcif.rules
   :synopsis: Validator classes and composable rule factory functions

Validator Classes
-----------------

.. autoclass:: DictionaryValidator
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: MmcifValidator
   :members:
   :undoc-members:
   :show-inheritance:

Single-Category Rule Factories
------------------------------

.. autofunction:: mandatory_items
.. autofunction:: one_of_following
.. autofunction:: value_length
.. autofunction:: value_range
.. autofunction:: conditional_mandatory
.. autofunction:: regex_check
.. autofunction:: ordering_check
.. autofunction:: allowed_pairs
.. autofunction:: min_rows
.. autofunction:: enumeration_check
.. autofunction:: type_check

Cross-Category Rule Factories
------------------------------

.. autofunction:: foreign_key
.. autofunction:: parent_child
.. autofunction:: composite_key
.. autofunction:: oper_expression
.. autofunction:: cross_mandatory
.. autofunction:: cross_ordering
