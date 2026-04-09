Plugin System
=============

.. module:: sloth.mmcif.plugins
   :synopsis: Generic plugin factory and base classes

Core Classes
------------

.. autoclass:: Plugin
   :members:
   :show-inheritance:

.. autoclass:: PluginWrapper
   :members:
   :show-inheritance:

.. autoclass:: FunctionPlugin
   :members:
   :show-inheritance:

.. autoclass:: PluginFactory
   :members:
   :undoc-members:
   :show-inheritance:

Model-Level Registration
------------------------

All :class:`~sloth.mmcif.models.DataContainer` subclasses
(:class:`~sloth.mmcif.models.Category`,
:class:`~sloth.mmcif.models.DataBlock`,
:class:`~sloth.mmcif.models.MMCIFDataContainer`) expose
``register(name, plugin)`` — see :class:`~sloth.mmcif.models.DataContainer`.

Validation as a Plugin Example
------------------------------

See :mod:`sloth.mmcif.validator` for ``ValidatorPlugin``, ``CategoryValidator``,
and concrete rule factories — the canonical example of the plugin system.
