Quick Start
===========

Parsing an mmCIF File
---------------------

.. code-block:: python

   from sloth import MMCIFHandler

   handler = MMCIFHandler()
   mmcif = handler.read("1abc.cif")

   # Access data with dot notation
   block = mmcif.data_1ABC
   print(block._struct.title[0])
   print(block._atom_site.Cartn_x[0])

Selective Parsing
-----------------

Load only the categories you need:

.. code-block:: python

   mmcif = handler.read("1abc.cif", categories=["_atom_site", "_entity"])

Writing mmCIF Files
-------------------

.. code-block:: python

   handler.write(mmcif, "output.cif")

Quick JSON Export
-----------------

.. code-block:: python

   # Export to nested JSON with resolved relationships
   json_str = handler.export(mmcif, indent=2)

   # Export directly to file
   handler.export(mmcif, file_path="output.json", indent=2)

Quick JSON Import
-----------------

.. code-block:: python

   # Import from JSON (automatically flattens nested structure)
   mmcif = handler.load("output.json")
