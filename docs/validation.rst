Validation
==========

SLOTH provides a layered validation system built on the
:doc:`plugin system <api/validator>`.  Two ready-to-use validator classes and a
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

``strict=True`` registers :class:`~sloth.mmcif.rules.MmcifValidator`, which
includes **all** dictionary-derived checks *and* wwPDB deposition rules.

Validator Classes
-----------------

SLOTH ships two validator classes in :mod:`sloth.mmcif.rules`, both subclasses
of :class:`~sloth.mmcif.validator.ValidatorPlugin`:

:class:`~sloth.mmcif.rules.DictionaryValidator`
   Auto-generated from the bundled ``mmcif_pdbx_v50.dic`` (or any mmCIF
   dictionary).  Covers mandatory items, enumerations, type-regex patterns,
   foreign keys, composite keys, and parent/child category presence — all
   extracted via :class:`~sloth.mmcif.serializer.DictionaryParser`.

:class:`~sloth.mmcif.rules.MmcifValidator`
   Extends ``DictionaryValidator`` with wwPDB deposition business rules
   expressed as declarative class-level data tables.  Adding a new rule is as
   simple as appending a tuple.

Use them directly when you want explicit control:

.. code-block:: python

   from sloth import MMCIFHandler
   from sloth.mmcif.rules import DictionaryValidator, MmcifValidator

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

The :mod:`sloth.mmcif.rules` module exports 18 composable factory functions
that return validator callables.  Use them to build a custom
:class:`~sloth.mmcif.validator.ValidatorPlugin`:

.. code-block:: python

   from sloth.mmcif.validator import ValidatorPlugin
   from sloth.mmcif.rules import (
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

- :func:`~sloth.mmcif.rules.mandatory_items` — items must be non-null
- :func:`~sloth.mmcif.rules.one_of_following` — at least one item must be set
- :func:`~sloth.mmcif.rules.value_length` — string length bounds
- :func:`~sloth.mmcif.rules.value_range` — numeric bounds
- :func:`~sloth.mmcif.rules.conditional_mandatory` — items required when a
  trigger has specific values
- :func:`~sloth.mmcif.rules.regex_check` — values must match a regex
- :func:`~sloth.mmcif.rules.ordering_check` — numeric ordering between two
  items in the same category
- :func:`~sloth.mmcif.rules.allowed_pairs` — restrict value combinations
- :func:`~sloth.mmcif.rules.min_rows` — minimum row count
- :func:`~sloth.mmcif.rules.enumeration_check` — values must be in an allowed set
- :func:`~sloth.mmcif.rules.type_check` — values must match a type regex

**Cross-category factories:**

- :func:`~sloth.mmcif.rules.foreign_key` — FK integrity across categories
- :func:`~sloth.mmcif.rules.parent_child` — parent must exist when child does
- :func:`~sloth.mmcif.rules.composite_key` — multi-column FK integrity
- :func:`~sloth.mmcif.rules.oper_expression` — validate ``oper_expression``
  references against ``_pdbx_struct_oper_list``
- :func:`~sloth.mmcif.rules.cross_mandatory` — items required in cat B when
  cat A is present
- :func:`~sloth.mmcif.rules.cross_ordering` — numeric ordering across two
  categories

Validation Severity
-------------------

Every rule factory accepts a ``severity`` parameter:

.. code-block:: python

   from sloth import ValidationError, ValidationSeverity
   from sloth.mmcif.rules import value_range

   # ERROR — prevents processing (default for most factories)
   # WARNING — flags potential issues
   # INFO — informational notices

   check = value_range("defocus", min_val=0, max_val=200,
                        severity=ValidationSeverity.WARNING)

Extending MmcifValidator
-------------------------

To add wwPDB rules, subclass :class:`~sloth.mmcif.rules.MmcifValidator` and
extend the declarative tables:

.. code-block:: python

   from sloth.mmcif.rules import MmcifValidator

   class MyValidator(MmcifValidator):
       # Add mandatory items for a custom category
       _MANDATORY = MmcifValidator._MANDATORY + [
           ("_my_category", ["required_field_a", "required_field_b"]),
       ]

Or add rules at runtime after instantiation:

.. code-block:: python

   from sloth.mmcif.rules import MmcifValidator, regex_check

   v = MmcifValidator()
   v.register_validator("_my_category", regex_check("code", r"^[A-Z]{3}$"))
