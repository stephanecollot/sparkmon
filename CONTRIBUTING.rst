Contributor Guide
=================

Thank you for your interest in improving this project.
This project is open-source under the `MIT license`_ and
welcomes contributions in the form of bug reports, feature requests, and pull requests.

Here is a list of important resources for contributors:

- `Source Code`_
- `Documentation`_
- `Issue Tracker`_
- `Code of Conduct`_

.. _MIT license: https://opensource.org/licenses/MIT
.. _Source Code: https://github.com/stephanecollot/sparkmon
.. _Documentation: https://sparkmon.readthedocs.io/
.. _Issue Tracker: https://github.com/stephanecollot/sparkmon/issues

How to report a bug
-------------------

Report bugs on the `Issue Tracker`_.

When filing an issue, make sure to answer these questions:

- Which operating system and Python version are you using?
- Which version of this project are you using?
- What did you do?
- What did you expect to see?
- What did you see instead?

The best way to get your bug fixed is to provide a test case,
and/or steps to reproduce the issue.


How to request a feature
------------------------

Request features on the `Issue Tracker`_.


How to set up your development environment
------------------------------------------

You need Python 3.6+ and the following tools:

- Poetry_
- Nox_
- nox-poetry_

Install the package with development requirements:

.. code:: console

   $ poetry install

You can now run an interactive Python session,
or the command-line interface:

.. code:: console

   $ poetry run python
   $ poetry run sparkmon

If you want to install it in your environment:

.. code:: console

   $ poetry build --format sdist
   $ tar -xvf dist/*-`poetry version -s`.tar.gz -O '*/setup.py' > setup.py
   $ pip install -e .

.. _Poetry: https://python-poetry.org/
.. _Nox: https://nox.thea.codes/
.. _nox-poetry: https://nox-poetry.readthedocs.io/


How to test the project
-----------------------

Run the full test suite:

.. code:: console

   $ nox

List the available Nox sessions:

.. code:: console

   $ nox --list-sessions

You can also run a specific Nox session.
For example, invoke the unit test suite like this:

.. code:: console

   $ nox --session=tests

Unit tests are located in the ``tests`` directory,
and are written using the pytest_ testing framework.

.. _pytest: https://pytest.readthedocs.io/


How to submit changes
---------------------

Open a `pull request`_ to submit changes to this project.

Your pull request needs to meet the following guidelines for acceptance:

- The Nox test suite must pass without errors and warnings.
- Include unit tests. This project maintains 90% code coverage.
- If your changes add functionality, update the documentation accordingly.

Feel free to submit early, though—we can always iterate on this.

To run linting and code formatting checks before commiting your change, you can install pre-commit as a Git hook by running the following command:

.. code:: console

   $ nox --session=pre-commit -- install

Recommended way to run linting and code formatting:
.. code:: console

   $ # with python 3.7
   $ git add -u
   $ nox --session=pre-commit
   $ git add -u
   $ git commit -m "" --no-verify


It is recommended to open an issue before starting work on anything.
This will allow a chance to talk it over with the owners and validate your approach.

.. _pull request: https://github.com/stephanecollot/sparkmon/pulls
.. github-only
.. _Code of Conduct: CODE_OF_CONDUCT.rst


How release a new version
-------------------------

Make a new branch only to make the release and run `poetry version`, as follow:

.. code:: console

   $ git checkout master
   $ git pull
   $ # Where <version> = X.X.X
   $ git checkout -b release_<version>
   $ poetry version <version>
   $ git add pyproject.toml
   $ git commit -m "<project> <version>"
   $ git push

Then make a pull request, and merge it to master.
It will automatically: make a release note in Github, make a tag and release to PyPI

https://cookiecutter-hypermodern-python.readthedocs.io/en/2021.7.15/guide.html#how-to-make-a-release
