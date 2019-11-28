========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis|
        | |coveralls| |codecov|
        | |scrutinizer| |codacy| |codeclimate|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/cli/badge/?style=flat
    :target: https://readthedocs.org/projects/cli
    :alt: Documentation Status

.. |travis| image:: https://api.travis-ci.org/open-fred/cli.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/open-fred/cli

.. |coveralls| image:: https://coveralls.io/repos/open-fred/cli/badge.svg?branch=master&service=github
    :alt: Coverage Status
    :target: https://coveralls.io/r/open-fred/cli

.. |codecov| image:: https://codecov.io/github/open-fred/cli/coverage.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/open-fred/cli

.. |codacy| image:: https://img.shields.io/codacy/grade/[Get ID from https://app.codacy.com/app/open-fred/cli/settings].svg
    :target: https://www.codacy.com/app/open-fred/cli
    :alt: Codacy Code Quality Status

.. |codeclimate| image:: https://codeclimate.com/github/open-fred/cli/badges/gpa.svg
   :target: https://codeclimate.com/github/open-fred/cli
   :alt: CodeClimate Quality Status

.. |version| image:: https://img.shields.io/pypi/v/open_FRED-cli.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/open_FRED-cli

.. |wheel| image:: https://img.shields.io/pypi/wheel/open_FRED-cli.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/open_FRED-cli

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/open_FRED-cli.svg
    :alt: Supported versions
    :target: https://pypi.org/project/open_FRED-cli

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/open_FRED-cli.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/open_FRED-cli

.. |commits-since| image:: https://img.shields.io/github/commits-since/open-fred/cli/v0.0.0dev.svg
    :alt: Commits since latest release
    :target: https://github.com/open-fred/cli/compare/v0.0.0dev...master


.. |scrutinizer| image:: https://img.shields.io/scrutinizer/quality/g/open-fred/cli/master.svg
    :alt: Scrutinizer Status
    :target: https://scrutinizer-ci.com/g/open-fred/cli/


.. end-badges

"The open_FRED command line interface and other tools for working with databases containing open_FRED data."

* Free software: BSD 3-Clause License

Installation
============

It's probably best to install the package into a `virtual environment`_.
Once you've created a virtualenv, :code:`activate` it and install the
package into it via

::

    pip install open_FRED-cli

You can also install the in-development version with::

    pip install https://github.com/open-fred/cli/archive/master.zip

Once you've done either, make yourself familiar with the program by running::

    open_FRED --help

.. _virtual environment: http://docs.python-guide.org/en/latest/dev/virtualenvs/

Documentation
=============


https://cli.readthedocs.io/


Development
===========

To run the all tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
