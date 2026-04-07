``MMCIFHandler`` — Main Entry Point
====================================

.. module:: sloth.mmcif.handler
   :synopsis: High-performance mmCIF file handler

The :class:`MMCIFHandler` is the primary interface for reading, writing,
exporting, and importing mmCIF data.

.. code-block:: python

   from sloth import MMCIFHandler

   handler = MMCIFHandler()
   mmcif = handler.read("1abc.cif")

.. autoclass:: MMCIFHandler
   :members:
   :undoc-members:
   :show-inheritance:
