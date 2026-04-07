Installation
============

From TestPyPI
-------------

.. code-block:: bash

   pip install -i https://test.pypi.org/simple/ sloth-mmcif

From Source
-----------

.. code-block:: bash

   git clone https://github.com/lucas-ebi/sloth.git
   cd sloth
   pip install -e ".[dev]"

Requirements
------------

- Python 3.8+
- `gemmi <https://gemmi.readthedocs.io/>`_ >= 0.6.0
- msgpack
- jsonschema >= 4.0.0

Optional dependencies (for development):

.. code-block:: bash

   pip install -e ".[dev]"

This installs pytest, black, flake8, mypy, and build tools.
