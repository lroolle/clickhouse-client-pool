clickhouse-client-pool
======================

.. contents:: **Table of Contents**
    :backlinks: none

Intro
------------

A Naive **Thread Safe** clickhouse-client-pool based on `clickhouse_driver <https://clickhouse-driver.readthedocs.io/en/latest/>`_.


Installation
------------

clickhouse-client-pool is distributed on `PyPI <https://pypi.org>`_ as a universal
wheel and is available on Linux/macOS and Windows and supports
Python 2.7/3.6+.

.. code-block:: bash

    $ pip install clickhouse-client-pool


.. code-block:: python

    from clickhouse_client_pool import Client


    client = Client('127.0.0.1', 9000, max_connections=10)
    client.execute("select 1")



Installation
------------


License
-------

clickhouse-client-pool is distributed under the terms of

- `MIT License <https://choosealicense.com/licenses/mit>`_

at your option.
