Changelog
=========

Unreleased
----------

- **Validation rules module** (``sloth.mmcif.rules``):
  - ``DictionaryValidator``: auto-generates checks from the bundled mmCIF
    dictionary via ``DictionaryParser`` (mandatory items, enumerations,
    type-regex patterns, FK/composite-key integrity, parent/child presence)
  - ``MmcifValidator``: extends ``DictionaryValidator`` with wwPDB deposition
    business rules expressed as declarative class-level data tables
  - 18 composable rule factory functions for custom validation
- ``ValidatorPlugin`` supports multiple validators per category (list-based)
- ``MMCIFHandler(strict=True)`` auto-registers ``MmcifValidator``
- Generic plugin system: ``PluginFactory``, ``Plugin``, ``PluginWrapper``, ``FunctionPlugin``
- Streamlined registration: ``handler.register("_cat", func)`` for validators, tuples for cross-checkers
- Delete support: ``del block._category``, ``block.delete("_category")``, same for items
- Safe access mode: ``auto_create=False`` on ``DataBlock`` / ``MMCIFDataContainer``
- Strict mode: ``MMCIFHandler(strict=True)`` wires ``auto_create=False`` through the full parse chain
- Remove backward-compat shims: ``ValidatorFactory``, ``validator_factory=`` kwargs, property aliases
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
