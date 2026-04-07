Import & Export
===============

SLOTH supports round-trip conversion between mmCIF and nested JSON formats.
The JSON exporter automatically resolves parent-child relationships defined in
the mmCIF dictionary.

JSON Export
-----------

Export mmCIF data to nested JSON:

.. code-block:: python

   from sloth import MMCIFHandler

   handler = MMCIFHandler()
   mmcif = handler.read("1abc.cif")

   # Return JSON string (pretty-printed)
   json_str = handler.export(mmcif, indent=2)

   # Write to file
   handler.export(mmcif, file_path="output.json", indent=2)

   # Compact JSON (no indentation)
   handler.export(mmcif, file_path="compact.json")

Nested JSON Structure
~~~~~~~~~~~~~~~~~~~~~

Child categories are automatically nested within their parents:

.. code-block:: json

   {
     "data_DEMO": {
       "_entry": ["..."],
       "_entity": [
         {
           "id": "1",
           "type": "polymer",
           "_entity_poly": [
             {
               "entity_id": "1",
               "_entity_poly_seq": ["..."]
             }
           ]
         }
       ]
     }
   }

.. note::

   All category names maintain the ``_`` prefix convention, whether at the top
   level or nested.

JSON Import
-----------

Import nested JSON back to mmCIF:

.. code-block:: python

   # From file
   mmcif = handler.load("output.json")

   # Access data using standard mmCIF notation
   print(mmcif.data[0]._entity.id)
   print(mmcif.data[0]._atom_site.Cartn_x)

Round-trip Verification
-----------------------

.. code-block:: python

   handler.export(mmcif, file_path="test.json")
   imported = handler.load("test.json")

   orig_block = mmcif.data[0]
   imp_block = imported.data[0]

   assert set(orig_block.categories) == set(imp_block.categories)

Advanced Export Options
-----------------------

The :class:`~sloth.mmcif.exporter.JSONExporter` class provides fine-grained control:

.. code-block:: python

   from sloth.mmcif import JSONExporter

   exporter = JSONExporter(
       dict_path="path/to/mmcif_pdbx_v50.dic",  # custom dictionary
       denormalize=True,  # embed reference data
       quiet=True,        # suppress progress output
   )
   json_str = exporter.export_data(mmcif, indent=2)

The :class:`~sloth.mmcif.importer.JSONImporter` provides equivalent control for imports:

.. code-block:: python

   from sloth.mmcif import JSONImporter

   importer = JSONImporter(
       dict_path="path/to/mmcif_pdbx_v50.dic",
       quiet=True,
   )
   mmcif = importer.import_data("output.json")
