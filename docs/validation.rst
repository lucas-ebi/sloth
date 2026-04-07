Validation
==========

SLOTH provides a pluggable validation system that supports per-category validators
and cross-category checkers.

Validator Factory
-----------------

Register validators using :class:`~sloth.mmcif.plugins.ValidatorFactory`:

.. code-block:: python

   from sloth import MMCIFHandler, ValidatorFactory

   vf = ValidatorFactory()

   # Register a category validator
   vf.register_validator(
       "_atom_site",
       lambda cat: print(f"Validating {cat.name} ({cat.row_count} rows)")
   )

   handler = MMCIFHandler(validator_factory=vf)
   mmcif = handler.read("1abc.cif")

Single Category Validation
--------------------------

.. code-block:: python

   mmcif.data_1ABC._atom_site.validate()

Cross-Category Validation
-------------------------

Register a cross-checker for related categories:

.. code-block:: python

   vf.register_cross_checker(
       ("_entity", "_atom_site"),
       lambda e, a: set(e.id).issuperset(set(a.label_entity_id))
   )

   # Run cross-validation
   mmcif.data_1ABC._entity.validate.against(mmcif.data_1ABC._atom_site)

   # Or validate first, then cross-check
   mmcif.data_1ABC._entity.validate().against(mmcif.data_1ABC._atom_site)

Validation Severity
-------------------

Validation errors carry severity levels:

.. code-block:: python

   from sloth import ValidationError, ValidationSeverity

   # Severity levels:
   # ValidationSeverity.ERROR   — prevents processing
   # ValidationSeverity.WARNING — flags potential issues
   # ValidationSeverity.INFO    — informational messages
