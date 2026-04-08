Plugin System
=============

.. module:: sloth.mmcif.plugins
   :synopsis: Generic plugin factory and base classes

Core Classes
------------

.. autoclass:: PluginFactory
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: Plugin
   :members:
   :show-inheritance:

.. autoclass:: PluginWrapper
   :members:
   :show-inheritance:

.. autoclass:: FunctionPlugin
   :members:
   :show-inheritance:

Validation as a Plugin Example
------------------------------

See the validation implementation in:

- :mod:`sloth.mmcif.validator` for ``ValidatorPlugin`` and ``CategoryValidator``
- :mod:`sloth.mmcif.rules` for concrete validator implementations
