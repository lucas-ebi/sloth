Changelog
=========

Unreleased
----------

- Generic plugin system: ``PluginFactory``, ``Plugin``, ``PluginWrapper``, ``FunctionPlugin``
- Validation as a plugin: ``ValidatorPlugin`` + ``CategoryValidator`` in ``validator.py``
- Delete support: ``del block._category``, ``block.delete("_category")``, same for items
- Safe access mode: ``auto_create=False`` on ``DataBlock`` / ``MMCIFDataContainer``
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
