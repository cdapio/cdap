.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. highlight:: console

.. _http-restful-api-transactions:

====================================
Transaction Service HTTP RESTful API
====================================

Certain internals of the CDAP Transaction Service are exposed via HTTP RESTful endpoints and are documented here.

.. _http-restful-api-transactions-number:

Number of Invalid Transactions
------------------------------

To retrieve the number of invalid transactions in the system, issue an HTTP GET request::

  GET <base-url>/transactions/invalid/size

The response is a JSON string::

  { "size": <size> }


.. _http-restful-api-transactions-truncate:

Truncate Invalid Transactions by Time
-------------------------------------

To truncate invalid transactions before a specific time, issue an HTTP POST request::

  POST <base-url>/transactions/invalid/remove/until

with the timestamp in milliseconds as a JSON string in the body::

  { "time" : <timestamp-ms> }

**Note:** Since improperly removing invalid transactions may result in data inconsistency,
please refer to :ref:`Transaction Service Maintenance <tx-maintenance>` for proper usage.