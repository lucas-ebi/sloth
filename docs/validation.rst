Validation
==========

SLOTH provides a pluggable validation system that supports per-category validators
and cross-category checkers, built on the generic plugin architecture.

Setting Up Validation
---------------------

Create a :class:`~sloth.mmcif.plugins.PluginFactory`, register a
:class:`~sloth.mmcif.validator.ValidatorPlugin`, and pass it to the handler:

.. code-block:: python

   from sloth import MMCIFHandler, PluginFactory, ValidatorPlugin

   plugin = ValidatorPlugin()
   plugin.register_validator(
       "_atom_site",
       lambda cat: print(f"Validating {cat.name} ({cat.row_count} rows)")
   )

   pf = PluginFactory()
   pf.register("validate", plugin, scope="category")

   handler = MMCIFHandler(plugin_factory=pf)
   mmcif = handler.read("1abc.cif")

Single Category Validation
--------------------------

.. code-block:: python

   mmcif.data_1ABC._atom_site.validate()

Cross-Category Validation
-------------------------

Register a cross-checker for related categories:

.. code-block:: python

   plugin.register_cross_checker(
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
