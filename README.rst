sparkmon
========

|PyPI| |Python Version| |License|

|Read the Docs| |Tests| |Codecov|

|pre-commit| |Black|

.. |PyPI| image:: https://img.shields.io/pypi/v/sparkmon.svg
   :target: https://pypi.org/project/sparkmon/
   :alt: PyPI
.. |Python Version| image:: https://img.shields.io/pypi/pyversions/sparkmon
   :target: https://pypi.org/project/sparkmon
   :alt: Python Version
.. |License| image:: https://img.shields.io/pypi/l/sparkmon
   :target: https://opensource.org/licenses/Apache-2.0
   :alt: License
.. |Read the Docs| image:: https://img.shields.io/readthedocs/sparkmon/latest.svg?label=Read%20the%20Docs
   :target: https://sparkmon.readthedocs.io/
   :alt: Read the documentation at https://sparkmon.readthedocs.io/
.. |Tests| image:: https://github.com/stephanecollot/sparkmon/workflows/Tests/badge.svg
   :target: https://github.com/stephanecollot/sparkmon/actions?workflow=Tests
   :alt: Tests
.. |Codecov| image:: https://codecov.io/gh/stephanecollot/sparkmon/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/stephanecollot/sparkmon
   :alt: Codecov
.. |pre-commit| image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white
   :target: https://github.com/pre-commit/pre-commit
   :alt: pre-commit
.. |Black| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
   :alt: Black

Description
-----------

``sparkmon`` is a Python package to monitor Spark applications. You can see it as an advanced Spark UI, that keep track all of `Spark REST API <SparkREST_>`_ metrics over time. It is specifically useful to do memory profiling.


Features
--------

Monitoring plot example:

.. image:: docs/_static/monitoring-plot-example.png

* Log the executors metrics
* Plot monitoring, display in a notebook, or export to a file
* Can monitor remote Spark application
* Can run directly in your PySpark application, or run in a notebook, or via the command-line interface
* Log to mlflow


Requirements
------------

* Python
* Spark
* mlflow (optional)


Installation
------------

You can install *sparkmon* via pip_ from PyPI_:

.. code:: console

   $ pip install sparkmon
   $ pip install sparkmon[mlflow]


Usage
-----

.. code-block:: python

   import sparkmon

   # Create an app connection
   # via a Spark session
   application = sparkmon.create_application_from_spark(spark)
   # or via a remote Spark web UI link
   application = sparkmon.create_application_from_link(index=0, web_url='http://localhost:4040')

   # Create and start the monitoring process
   mon = sparkmon.SparkMon(application, period=5, callbacks=[
       sparkmon.callbacks.plot_to_image,
       sparkmon.callbacks.log_to_mlflow,
   ])
   mon.start()

   # Stop monitoring
   mon.stop()

You can also use it from a notebook: `Notebook Example <Example_>`_

There is also a command-line interface, see  `Command-line Reference <Usage_>`_ for details.


How does it work?
-----------------

``SparkMon`` is running in the background a Python thread that is querying Spark web UI API and logging all the executors information over time.

The ``callbacks`` list parameters allows you to define what do after each update, like exporting executors historical info to a csv, or plotting to a file, or to your notebook.


Contributing
------------

Contributions are very welcome.
To learn more, see the `Contributor Guide`_.


License
-------

Distributed under the terms of the `Apache 2.0 license`_,
*sparkmon* is free and open source software.


Issues
------

If you encounter any problems,
please `file an issue`_ along with a detailed description.


Credits
-------

This project was generated from `@cjolowicz`_'s `Hypermodern Python Cookiecutter`_ template.

.. _@cjolowicz: https://github.com/cjolowicz
.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _Apache 2.0 license: https://opensource.org/licenses/Apache-2.0
.. _PyPI: https://pypi.org/
.. _Hypermodern Python Cookiecutter: https://github.com/cjolowicz/cookiecutter-hypermodern-python
.. _file an issue: https://github.com/stephanecollot/sparkmon/issues
.. _pip: https://pip.pypa.io/
.. github-only
.. _Contributor Guide: CONTRIBUTING.rst
.. _Usage: https://sparkmon.readthedocs.io/en/latest/usage.html
.. _Example: https://sparkmon.readthedocs.io/en/latest/example.html
.. _SparkREST: https://spark.apache.org/docs/latest/monitoring.html#rest-api
