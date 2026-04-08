SLOTH Documentation
====================

.. image:: ../logo.png
   :alt: SLOTH Logo
   :align: center
   :width: 300px

.. raw:: html

   <p align="center"><em>Lazy by design. Fast by default.</em></p>
   <br/>

**SLOTH** (*Structural Loader with On-demand Traversal Handling*) is a
high-performance mmCIF parser for structural biology workflows. Built on the
C++ `gemmi <https://gemmi.readthedocs.io/>`_ backend, SLOTH performs eager
parsing and lazy object construction — efficient for both large-scale
pipelines and interactive exploration.

Key features:

- **High-speed parsing** via the gemmi C++ backend
- **Lazy construction** of row and item objects for memory efficiency
- **Pythonic dot-notation access** to mmCIF data
- **Pluggable validation** system with cross-category support
- **JSON export/import** with automatic relationship resolution

.. code-block:: python

   from sloth import MMCIFHandler

   handler = MMCIFHandler()
   mmcif = handler.read("1abc.cif")

   print(mmcif.data_1ABC._struct.title[0])
   print(mmcif.data_1ABC._atom_site.Cartn_x[0])


Getting Started
---------------

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   installation
   quickstart
   usage
   validation
   import_export
   cookbook

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/handler
   api/models
   api/parser
   api/writer
   api/exporter
   api/importer
   api/serializer
   api/plugins
   api/validator
   api/common

.. toctree::
   :maxdepth: 1
   :caption: Project

   changelog
   contributing


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
