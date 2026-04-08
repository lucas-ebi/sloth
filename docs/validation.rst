Validation
==========

SLOTH provides a layered validation system built on the
:doc:`plugin system <api/plugins>`.  Two ready-to-use validator classes and a
library of composable rule factories cover everything from mmCIF dictionary
conformance to wwPDB deposition business rules.

Quick Start
-----------

The fastest way to get full validation is **strict mode**:

.. code-block:: python

   from sloth import MMCIFHandler

   handler = MMCIFHandler(strict=True)   # registers MmcifValidator automatically
   mmcif = handler.read("model.cif")

   # Per-category validation (raises ValidationError on failure)
   mmcif.data_1ABC._refine.validate()

   # Cross-category validation
   mmcif.data_1ABC._entity.validate().against(mmcif.data_1ABC._atom_site)

``strict=True`` registers :class:`~sloth.mmcif.validator.MmcifValidator`, which
includes **all** dictionary-derived checks *and* wwPDB deposition rules.

Validator Classes
-----------------

SLOTH ships two validator classes in :mod:`sloth.mmcif.validator`, both subclasses
of :class:`~sloth.mmcif.validator.ValidatorPlugin`:

:class:`~sloth.mmcif.validator.DictionaryValidator`
   Auto-generated from the bundled ``mmcif_pdbx_v50.dic`` (or any mmCIF
   dictionary).  Covers mandatory items, enumerations, type-regex patterns,
   foreign keys, composite keys, and parent/child category presence — all
   extracted via :class:`~sloth.mmcif.serializer.DictionaryParser`.

:class:`~sloth.mmcif.validator.MmcifValidator`
   Extends ``DictionaryValidator`` with wwPDB deposition business rules
   expressed as declarative class-level data tables.  Adding a new rule is as
   simple as appending a tuple.

Use them directly when you want explicit control:

.. code-block:: python

   from sloth import MMCIFHandler
   from sloth.mmcif.validator import DictionaryValidator, MmcifValidator

   handler = MMCIFHandler()

   # Schema-only (no wwPDB rules)
   handler.register("validate", DictionaryValidator())

   # Full wwPDB + schema
   handler.register("validate", MmcifValidator())

Single-Category Validation
--------------------------

.. code-block:: python

   mmcif.data_1ABC._atom_site.validate()

Cross-Category Validation
-------------------------

Register a cross-checker by passing a tuple of category names, or use the
built-in validators which register cross-checkers automatically:

.. code-block:: python

   # The built-in validators already register FK / parent-child / ordering
   # cross-checkers.  Just chain .against():
   mmcif.data_1ABC._entity_src_nat.validate().against(
       mmcif.data_1ABC._entity
   )

You can also register custom cross-checkers:

.. code-block:: python

   handler.register(
       ("_entity", "_atom_site"),
       lambda e, a: check_entity_coverage(e, a),
   )

   mmcif.data_1ABC._entity.validate().against(mmcif.data_1ABC._atom_site)

Multiple validators (and cross-checkers) can be registered for the same
category — they all run in registration order.

Custom Rules with Factories
----------------------------

The :mod:`sloth.mmcif.validator` module exports 18 composable factory functions
that return validator callables.  Use them to build a custom
:class:`~sloth.mmcif.validator.ValidatorPlugin`:

.. code-block:: python

   from sloth.mmcif.validator import ValidatorPlugin
   from sloth.mmcif.validator import (
       mandatory_items,
       value_length,
       ordering_check,
       foreign_key,
   )

   vp = ValidatorPlugin()

   # Category-level rules
   vp.register_validator("_struct", mandatory_items(["title"]))
   vp.register_validator("_struct", value_length("title", min_len=10))

   # Cross-category rule
   vp.register_cross_checker(
       ("_atom_site", "_entity"),
       foreign_key("label_entity_id", "id"),
   )

   handler.register("validate", vp)

**Single-category factories:**

- :func:`~sloth.mmcif.validator.mandatory_items` — items must be non-null
- :func:`~sloth.mmcif.validator.one_of_following` — at least one item must be set
- :func:`~sloth.mmcif.validator.value_length` — string length bounds
- :func:`~sloth.mmcif.validator.value_range` — numeric bounds
- :func:`~sloth.mmcif.validator.conditional_mandatory` — items required when a
  trigger has specific values
- :func:`~sloth.mmcif.validator.regex_check` — values must match a regex
- :func:`~sloth.mmcif.validator.ordering_check` — numeric ordering between two
  items in the same category
- :func:`~sloth.mmcif.validator.allowed_pairs` — restrict value combinations
- :func:`~sloth.mmcif.validator.min_rows` — minimum row count
- :func:`~sloth.mmcif.validator.enumeration_check` — values must be in an allowed set
- :func:`~sloth.mmcif.validator.type_check` — values must match a type regex

**Cross-category factories:**

- :func:`~sloth.mmcif.validator.foreign_key` — FK integrity across categories
- :func:`~sloth.mmcif.validator.parent_child` — parent must exist when child does
- :func:`~sloth.mmcif.validator.composite_key` — multi-column FK integrity
- :func:`~sloth.mmcif.validator.oper_expression` — validate ``oper_expression``
  references against ``_pdbx_struct_oper_list``
- :func:`~sloth.mmcif.validator.cross_mandatory` — items required in cat B when
  cat A is present
- :func:`~sloth.mmcif.validator.cross_ordering` — numeric ordering across two
  categories

Validation Severity
-------------------

Every rule factory accepts a ``severity`` parameter:

.. code-block:: python

   from sloth import ValidationError, ValidationSeverity
   from sloth.mmcif.validator import value_range

   # ERROR — prevents processing (default for most factories)
   # WARNING — flags potential issues
   # INFO — informational notices

   check = value_range("defocus", min_val=0, max_val=200,
                        severity=ValidationSeverity.WARNING)

Extending MmcifValidator
-------------------------

To add wwPDB rules, subclass :class:`~sloth.mmcif.validator.MmcifValidator` and
extend the declarative tables:

.. code-block:: python

   from sloth.mmcif.validator import MmcifValidator

   class MyValidator(MmcifValidator):
       # Add mandatory items for a custom category
       _MANDATORY = MmcifValidator._MANDATORY + [
           ("_my_category", ["required_field_a", "required_field_b"]),
       ]

Or add rules at runtime after instantiation:

.. code-block:: python

   from sloth.mmcif.validator import MmcifValidator, regex_check

   v = MmcifValidator()
   v.register_validator("_my_category", regex_check("code", r"^[A-Z]{3}$"))
