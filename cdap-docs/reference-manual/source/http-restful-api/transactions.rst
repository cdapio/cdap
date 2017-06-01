.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. highlight:: console

.. _http-restful-api-transactions:
.. _http-restful-api-v3-transactions:

====================================
Transaction Service HTTP RESTful API
====================================

Use the CDAP Transaction Service HTTP RESTful API to access selected
internals of the CDAP Transaction Service that are exposed via endpoints.

Additional details are found in the :ref:`Developer Manual: Transaction System <developer:transaction-system>`.

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


.. _http-restful-api-transactions-number:

Number of Invalid Transactions
==============================

To retrieve the number of invalid transactions in the system, issue an HTTP GET request::

  GET /v3/transactions/invalid/size

The response is a JSON string, with the integer number of invalid transactions as ``<size>``::

  { "size": <size> }


.. _http-restful-api-transactions-truncate:

Truncate Invalid Transactions by Time
=====================================

To truncate invalid transactions before a specific time, issue an HTTP POST request::

  POST /v3/transactions/invalid/remove/until

with the timestamp in milliseconds (``<timestamp-ms>``) as a JSON string in the body::

  { "time" : <timestamp-ms> }

**Note:** Since improperly removing invalid transactions may result in data inconsistency,
please refer to :ref:`Transaction Service Maintenance <tx-maintenance>` for proper usage.
