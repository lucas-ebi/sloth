Serializer & Relationship Resolution
=====================================

.. module:: sloth.mmcif.serializer
   :synopsis: Dictionary parsing, mapping generation, caching, and relationship resolution

This module provides the internal machinery for resolving mmCIF dictionary
relationships and building nested JSON structures.

Cache Management
----------------

.. autoclass:: CacheManager
   :members:
   :show-inheritance:

.. autofunction:: get_cache_manager

Dictionary Parsing
------------------

.. autoclass:: DictionaryParser
   :members:
   :show-inheritance:

Mapping Generation
------------------

.. autoclass:: MappingGenerator
   :members:
   :show-inheritance:

Relationship Resolution
-----------------------

.. autoclass:: RelationshipResolver
   :members:
   :show-inheritance:

.. autoclass:: RelationshipMetadata
   :members:

.. autoclass:: RelationshipConstraint
   :members:

Ownership Analysis
------------------

.. autoclass:: OwnershipAnalyzer
   :members:
   :show-inheritance:

Nesting
-------

.. autoclass:: NestingBuilder
   :members:
   :show-inheritance:
