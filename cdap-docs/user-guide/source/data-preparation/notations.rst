.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-notations:

=========
Notations
=========


.. highlight:: console

Directive
=========
A directive is represented as simple text in this format::

  <command> <argument> <argument> ... <argument>

where ``command`` is the directive, and ``arguments`` depend on the directive being called.


.. highlight:: json

Record
======
A record in this documentation is represented as a JSON object with the object key
representing the column name and the object value representing the data, with no mention
of data types. For example::

  {
    "id": 1,
    "fname": "root",
    "lname": "joltie",
    "address": {
      "housenumber": "678",
      "street": "Mars Street",
      "city": "Marcity",
      "state": "Maregon",
      "country": "Mari"
    },
    "gender", "M"
  }
