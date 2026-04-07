Usage Guide
===========

This guide covers the main access patterns and data manipulation features of SLOTH.

Access Patterns
---------------

Dot Notation
~~~~~~~~~~~~

The most natural way to access mmCIF data:

.. code-block:: python

   from sloth import MMCIFHandler

   handler = MMCIFHandler()
   mmcif = handler.read("1abc.cif")

   block = mmcif.data_1ABC
   atom_site = block._atom_site
   print(atom_site.Cartn_x[0])

Dictionary Notation
~~~~~~~~~~~~~~~~~~~

Useful when category or field names are dynamic:

.. code-block:: python

   category_name = "_atom_site"
   field_name = "Cartn_x"
   x = mmcif.data[0][category_name][field_name]

Row-wise Access
~~~~~~~~~~~~~~~

Iterate over rows in a category:

.. code-block:: python

   first_atom = atom_site[0]
   print(first_atom.type_symbol, first_atom.Cartn_x)

   # Iterate all rows
   for atom in atom_site:
       print(atom.label_atom_id, atom.Cartn_x)

Column-wise Access
~~~~~~~~~~~~~~~~~~

Access entire columns at once:

.. code-block:: python

   x_coords = atom_site.Cartn_x      # All X coordinates
   atom_ids = atom_site.label_atom_id  # All atom labels

Filtering and Slicing
~~~~~~~~~~~~~~~~~~~~~

Use list comprehensions for powerful filtering:

.. code-block:: python

   # CA atoms from chain A
   ca_atoms = [
       a for a in atom_site
       if a.label_atom_id == "CA" and a.label_asym_id == "A"
   ]

   # Mean X coordinate
   avg_x = sum(float(x) for x in atom_site.Cartn_x) / atom_site.row_count

Iterating Over Structure
~~~~~~~~~~~~~~~~~~~~~~~~

Enumerate all categories and items in a block:

.. code-block:: python

   for cat_name in block.categories:
       category = block[cat_name]
       for item_name in category.items:
           print(f"{cat_name}.{item_name}: {len(category[item_name])} values")


Data Creation
-------------

Manual File Creation
~~~~~~~~~~~~~~~~~~~~

Write CIF text directly:

.. code-block:: python

   sample = """data_1ABC
   _entry.id 1ABC_STRUCTURE
   loop_
   _atom_site.group_PDB
   _atom_site.id
   _atom_site.type_symbol
   _atom_site.Cartn_x
   _atom_site.Cartn_y
   _atom_site.Cartn_z
   ATOM 1 N 10.123 20.456 30.789
   ATOM 2 C 11.234 21.567 31.890
   """
   with open("sample.cif", "w") as f:
       f.write(sample)

Programmatic Creation
~~~~~~~~~~~~~~~~~~~~~

Build structures using the object model:

.. code-block:: python

   from sloth.mmcif import MMCIFDataContainer, DataBlock, Category

   mmcif = MMCIFDataContainer()
   block = DataBlock("1ABC")

   cat = Category("_entry")
   cat["id"] = ["1ABC_STRUCTURE"]
   block["_entry"] = cat

   mmcif["1ABC"] = block

Dot-based Auto-creation
~~~~~~~~~~~~~~~~~~~~~~~

The most concise approach — objects are created on the fly:

.. code-block:: python

   mmcif = MMCIFDataContainer()
   mmcif.data_1ABC._entry.id = ["1ABC_STRUCTURE"]
   mmcif.data_1ABC._atom_site.Cartn_x = ["10.1", "11.2"]


Strict Mode
-----------

By default, accessing a non-existent category or data block silently creates it.
This is convenient when *building* structures, but can mask typos when *reading*
data.  Pass ``strict=True`` to prevent this:

.. code-block:: python

   handler = MMCIFHandler(strict=True)
   mmcif = handler.read("1abc.cif")

   block = mmcif.data_1ABC
   block._atom_site          # OK — parsed from the file
   block._atm_site           # AttributeError with a helpful message

The error message lists the available categories so you can spot the typo quickly.

Strict mode can also be set at the model level:

.. code-block:: python

   from sloth.mmcif import DataBlock

   block = DataBlock("test", auto_create=False)
   block._nonexistent  # AttributeError


Deleting Data
-------------

Remove categories from a block or items from a category using ``del`` or the
``.delete()`` method.

Using ``del``
~~~~~~~~~~~~~

.. code-block:: python

   # Delete a category
   del block._obsolete_category

   # Delete an item from a category
   del block._atom_site.auth_asym_id

Using ``.delete()``
~~~~~~~~~~~~~~~~~~~

The string-based API is useful when the name is dynamic:

.. code-block:: python

   block.delete("_obsolete_category")
   block._atom_site.delete("auth_asym_id")

Both raise an error if the target does not exist (``AttributeError`` for ``del``,
``KeyError`` for ``.delete()``).
