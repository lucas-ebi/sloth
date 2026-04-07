Data Models
===========

.. module:: sloth.mmcif.models
   :synopsis: Core data models for mmCIF structures

This module defines the core data models that represent mmCIF file structures.
SLOTH's data hierarchy mirrors the mmCIF format:

- :class:`MMCIFDataContainer` — top-level container (one or more data blocks)
- :class:`DataBlock` — a named data block (e.g., ``data_1ABC``)
- :class:`Category` — a category within a block (e.g., ``_atom_site``)
- :class:`Row` — a single record in a category
- :class:`Item` — a column/field in a category

Enumerations
------------

.. autoclass:: DataSourceFormat
   :members:
   :undoc-members:

Abstract Base Classes
---------------------

.. autoclass:: DataNode
   :members:

.. autoclass:: DataContainer
   :members:

Top-level Container
-------------------

.. autoclass:: MMCIFDataContainer
   :members:
   :undoc-members:
   :show-inheritance:

Data Block
----------

.. autoclass:: DataBlock
   :members:
   :undoc-members:
   :show-inheritance:

Category
--------

.. autoclass:: Category
   :members:
   :undoc-members:
   :show-inheritance:

Row
---

.. autoclass:: Row
   :members:
   :undoc-members:
   :show-inheritance:

Item
----

.. autoclass:: Item
   :members:
   :undoc-members:
   :show-inheritance:

Lazy Loading Internals
----------------------

These classes provide efficient lazy-loading wrappers over gemmi data structures.
They are used internally and typically not instantiated directly.

.. autoclass:: LazyGemmiColumn
   :members:
   :show-inheritance:

.. autoclass:: LazyRowList
   :members:
   :show-inheritance:

.. autoclass:: LazyItemDict
   :members:
   :show-inheritance:

.. autoclass:: LazyKeyList
   :members:
   :show-inheritance:
