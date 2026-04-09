Validation
==========

SLOTH provides a layered validation system built on the
:doc:`plugin system <api/plugins>`.  Two ready-to-use validator classes and a
library of composable rule factories cover everything from mmCIF dictionary
conformance to wwPDB deposition business rules.

All validation code lives in :mod:`sloth.mmcif.validator`.

Quick Start
-----------

The simplest way to validate is with :class:`~sloth.mmcif.validator.MMCIFValidator`:

.. code-block:: python

   from sloth import MMCIFHandler, MMCIFValidator

   handler = MMCIFHandler()
   mmcif = handler.read("model.cif")

   # Full validation (dictionary schema + wwPDB rules)
   vp = MMCIFValidator()
   report = vp.validate(mmcif)

   print(report.is_valid)      # True / False
   print(len(report.errors))   # number of ERROR-level issues
   print(len(report.warnings)) # number of WARNING-level issues

   # Raise on first error
   report.raise_on_error()

``validate()`` is **polymorphic** — it works on a single
:class:`~sloth.mmcif.models.Category`, a
:class:`~sloth.mmcif.models.DataBlock`, or an entire
:class:`~sloth.mmcif.models.MMCIFDataContainer`.

Per-Category Validation
~~~~~~~~~~~~~~~~~~~~~~~

Register a validator on a model to get dot-notation access:

.. code-block:: python

   from sloth import MMCIFValidator

   vp = MMCIFValidator()
   block = mmcif.data_1ABC

   # Register on a category
   block._refine.register("validate", vp)
   block._refine.validate()

   # Cross-category validation
   block._entity.register("validate", vp)
   block._entity.validate().against(block._atom_site)

Validator Classes
-----------------

SLOTH ships two validator classes in :mod:`sloth.mmcif.validator`, both subclasses
of :class:`~sloth.mmcif.validator.ValidatorPlugin`:

:class:`~sloth.mmcif.validator.SchemaValidator`
   Auto-generated from the bundled ``mmcif_pdbx_v50.dic`` (or any mmCIF
   dictionary).  Covers mandatory items, enumerations, type-regex patterns,
   foreign keys, composite keys, and parent/child category presence — all
   extracted via :class:`~sloth.mmcif.serializer.DictionaryParser`.

:class:`~sloth.mmcif.validator.MMCIFValidator`
   Extends ``SchemaValidator`` with wwPDB deposition business rules
   expressed as declarative class-level data tables.  Adding a new rule is as
   simple as appending a tuple.

Use them directly:

.. code-block:: python

   from sloth.mmcif.validator import SchemaValidator, MMCIFValidator

   # Schema-only (no wwPDB rules)
   schema_vp = SchemaValidator()
   report = schema_vp.validate(mmcif)

   # Full wwPDB + schema
   full_vp = MMCIFValidator()
   report = full_vp.validate(mmcif)

Multi-Level Validators
~~~~~~~~~~~~~~~~~~~~~~

For validating entire blocks or containers via the plugin interface
(dot-notation):

:class:`~sloth.mmcif.validator.DataBlockValidator`
   Wraps a ``ValidatorPlugin`` and runs all per-category validators + cross-
   category checkers across every category in a :class:`DataBlock`.
   Returns a :class:`~sloth.mmcif.validator.ValidationReport`.

:class:`~sloth.mmcif.validator.ContainerValidator`
   Delegates to ``DataBlockValidator`` for each block in an
   :class:`MMCIFDataContainer`.

Register them on models for dot-notation access:

.. code-block:: python

   from sloth.mmcif.validator import (
       MMCIFValidator, DataBlockValidator, ContainerValidator,
   )

   vp = MMCIFValidator()
   bv = DataBlockValidator(vp)
   cv = ContainerValidator(bv)

   # Register on a container for one-call validation
   mmcif.register("validate", cv)
   wrapper = mmcif.validate()
   wrapper.report.raise_on_error()

   # Or register on a block
   block.register("validate", bv)
   wrapper = block.validate()
   print(wrapper.report.is_valid)

Validation Report
~~~~~~~~~~~~~~~~~

:class:`~sloth.mmcif.validator.ValidationReport` collects all
:class:`~sloth.mmcif.validator.ValidationError` instances:

.. code-block:: python

   report = vp.validate(container)

   report.is_valid          # True if no ERROR-level issues
   report.errors            # list of ERROR-level ValidationError
   report.warnings          # list of WARNING-level ValidationError
   report.all_issues        # everything (ERROR + WARNING + INFO)
   report.raise_on_error()  # raises the first ERROR, or does nothing

Single-Category Validation
--------------------------

.. code-block:: python

   # Register validator, then use dot-notation
   block._atom_site.register("validate", vp)
   block._atom_site.validate()

Cross-Category Validation
-------------------------

The built-in validators register cross-checkers automatically for FK,
parent/child, and ordering constraints.  Chain ``.against()`` to run them:

.. code-block:: python

   block._entity_src_nat.register("validate", vp)
   block._entity_src_nat.validate().against(block._entity)

You can also register custom cross-checkers on a ``ValidatorPlugin``:

.. code-block:: python

   vp = ValidatorPlugin()
   vp.register_cross_checker(
       ("_entity", "_atom_site"),
       lambda e, a: check_entity_coverage(e, a),
   )

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

   # Validate directly
   report = vp.validate(mmcif)

   # Or register on a model for dot-notation
   block._struct.register("validate", vp)
   block._struct.validate()

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

Extending MMCIFValidator
-------------------------

To add wwPDB rules, subclass :class:`~sloth.mmcif.validator.MMCIFValidator` and
extend the declarative tables:

.. code-block:: python

   from sloth.mmcif.validator import MMCIFValidator

   class MyValidator(MMCIFValidator):
       # Add mandatory items for a custom category
       _MANDATORY = MMCIFValidator._MANDATORY + [
           ("_my_category", ["required_field_a", "required_field_b"]),
       ]

Or add rules at runtime after instantiation:

.. code-block:: python

   from sloth.mmcif.validator import MMCIFValidator, regex_check

   v = MMCIFValidator()
   v.register_validator("_my_category", regex_check("code", r"^[A-Z]{3}$"))
