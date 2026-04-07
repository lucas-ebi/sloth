Contributing
============

Contributions are welcome! Here's how to get started:

1. Fork the `repository <https://github.com/lucas-ebi/sloth>`_
2. Create a feature branch (``git checkout -b feature/my-feature``)
3. Add tests for your changes
4. Run the test suite:

   .. code-block:: bash

      pytest

5. Submit a pull request

Development Setup
-----------------

.. code-block:: bash

   git clone https://github.com/lucas-ebi/sloth.git
   cd sloth
   pip install -e ".[dev]"

Code Style
----------

This project uses `black <https://github.com/psf/black>`_ for formatting and
`flake8 <https://flake8.pycqa.org/>`_ for linting:

.. code-block:: bash

   black sloth/ tests/
   flake8 sloth/ tests/
