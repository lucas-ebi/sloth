Validation
==========

SLOTH provides a pluggable validation system that supports per-category validators
and cross-category checkers via a simple ``handler.register()`` API.

Setting Up Validation
---------------------

Register validators directly on the handler using category names:

.. code-block:: python

   from sloth import MMCIFHandler

   handler = MMCIFHandler()
   handler.register(
       "_atom_site",
       lambda cat: print(f"Validating {cat.name} ({cat.row_count} rows)")
   )

   mmcif = handler.read("1abc.cif")

Single Category Validation
--------------------------

.. code-block:: python

   mmcif.data_1ABC._atom_site.validate()

Cross-Category Validation
-------------------------

Register a cross-checker by passing a tuple of category names:

.. code-block:: python

   handler.register(
       ("_entity", "_atom_site"),
       lambda e, a: set(e.id).issuperset(set(a.label_entity_id))
   )

   # Run cross-validation
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
