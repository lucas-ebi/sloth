Changelog
=========

v0.8.1 (2026-04-09)
--------------------

- Fix README: replace stale ``handler.validate()`` examples with
  ``MMCIFValidator().validate()`` API.

v0.8.0 (2026-04-09)
--------------------

**Breaking changes**

- ``handler.register()`` and ``handler.validate()`` have been **removed**.
  Registration now lives on model instances:
  ``category.register("validate", vp)``,
  ``block.register("validate", bv)``,
  ``container.register("validate", cv)``.
- ``PluginScope`` enum has been **removed**.  Scope is inferred from which
  model class ``register()`` is called on.
- ``PluginFactory`` no longer accepts a ``scope`` parameter — plugins are
  stored in a flat ``name → Plugin`` mapping.
- Renames: ``DictionaryValidator`` → :class:`~sloth.mmcif.validator.SchemaValidator`,
  ``MmcifValidator`` → :class:`~sloth.mmcif.validator.MMCIFValidator`,
  ``BlockValidator`` → :class:`~sloth.mmcif.validator.DataBlockValidator`.

**New features**

- **Model-level plugin registration** — ``register(name, plugin)`` is now a
  method on :class:`~sloth.mmcif.models.Category`,
  :class:`~sloth.mmcif.models.DataBlock`, and
  :class:`~sloth.mmcif.models.MMCIFDataContainer` (inherited from
  :class:`~sloth.mmcif.models.DataContainer`).
- **Standalone ``validate()``** — call
  ``MMCIFValidator().validate(data)`` directly without any handler
  involvement.  Accepts a ``Category``, ``DataBlock``, or
  ``MMCIFDataContainer``.
- Plugin wiring (``register()``, ``_lookup_plugin()``) is now defined once
  in the :class:`~sloth.mmcif.models.DataContainer` base class.

v0.7.0 (2026-04-08)
--------------------

**Breaking changes**

- Attribute access on non-existent categories / blocks now returns a
  lightweight *pending proxy* (``_PendingCategory`` / ``_PendingDataBlock``)
  that auto-commits on write and raises ``AttributeError`` with fuzzy
  suggestions on read.
- ``sloth.mmcif.rules`` module merged into :mod:`sloth.mmcif.validator`.
  All rule factories and validator classes now live in a single module.

**New features**

- **Multi-level validation** — :class:`~sloth.mmcif.validator.ValidatorPlugin`,
  :class:`~sloth.mmcif.validator.DataBlockValidator`, and
  :class:`~sloth.mmcif.validator.ContainerValidator` validate a
  :class:`Category`, :class:`DataBlock`, or
  :class:`MMCIFDataContainer` and return a
  :class:`~sloth.mmcif.validator.ValidationReport` with ``.errors``,
  ``.warnings``, ``.is_valid``, and ``.raise_on_error()`` helpers.
- **Schema-aware warnings** — assigning an unknown item to a known category
  (or creating an unknown category) emits a :class:`SchemaWarning` with
  "Did you mean …?" suggestions drawn from the bundled mmCIF dictionary.
- **Fuzzy matching in error messages** — ``AttributeError`` on
  ``Category``, ``DataBlock``, and ``MMCIFDataContainer`` includes
  ``difflib.get_close_matches`` suggestions.
- **Tab completion** — ``__dir__()`` on all three model classes exposes item
  names, category names, block names, and registered plugin names for
  IPython / Jupyter tab completion.
- **``ValidationSeverity`` enum** — ``ERROR``, ``WARNING``, ``INFO``
  severity levels accepted by every rule factory.
- **``ValidationReport``** — accumulator for ``ValidationError`` instances
  with filtering by severity.
- **``DataBlockValidator`` / ``ContainerValidator``** — plugins that run all
  per-category validators + cross-category checkers across an entire block
  or container.
- **``DataSourceFormat`` enum** moved to :mod:`~sloth.mmcif.defaults`.

**Bug fixes**

- ``DictionaryParser``: fix multiline value detection (now handles
  continuation lines whose first token is a bare value, not a key).
- ``DictionaryParser``: fix multi-loop storage so that successive
  ``loop_`` blocks in the same save frame no longer overwrite each other.
- ``DictionaryParser``: deterministic graph-based nesting resolves
  composite primary-key categories that previously caused non-deterministic
  parent/child ordering.

**Chores**

- Hoist non-conditional inline imports to module level across source and
  test files.
- Centralise all enums (``DataSourceFormat``, ``ValidationSeverity``) in
  :mod:`sloth.mmcif.defaults`.
- CI: weekly GitHub Actions workflow to auto-update the bundled mmCIF
  dictionary from wwPDB.

v0.6.0 (2026-04-07)
--------------------

- **Validation rules module** (``sloth.mmcif.rules``):

  - ``SchemaValidator``: auto-generates checks from the bundled mmCIF
    dictionary via ``DictionaryParser`` (mandatory items, enumerations,
    type-regex patterns, FK/composite-key integrity, parent/child presence)
  - ``MMCIFValidator``: extends ``SchemaValidator`` with wwPDB deposition
    business rules expressed as declarative class-level data tables
  - 18 composable rule factory functions for custom validation
- ``ValidatorPlugin`` supports multiple validators per category (list-based)
- Generic plugin system: ``PluginFactory``, ``Plugin``, ``PluginWrapper``, ``FunctionPlugin``
- Streamlined registration: ``handler.register("_cat", func)`` for validators, tuples for cross-checkers
- Delete support: ``del block._category``, ``block.delete("_category")``, same for items
- Update docs, cookbook, and API reference for new plugin API

v0.5.4 (2026-04-07)
--------------------

- Fix project URLs in pyproject.toml (lucas/sloth → lucas-ebi/sloth)

v0.5.3 (2026-04-07)
--------------------

- Add Sphinx + Read the Docs documentation with full API reference
- Integrate interactive cookbook notebook into docs
- Streamline README as concise PyPI landing page
- Add string interning note to performance table

v0.5.2 (2025-12-15)
--------------------

- Initial public release on TestPyPI
- High-performance gemmi-backed parser and writer
- Lazy object construction with ``cached_property``
- Dot-notation and dictionary access patterns
- JSON export with automatic relationship resolution
- JSON import with automatic flattening
- Pluggable validation system with cross-category support
- mmCIF dictionary parsing and FK/PK mapping
