.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide

.. _user-guide-data-preparation-output-formatters:

=================
Output Formatters
=================

- `Write as CSV`_
- `Write as a JSON Map`_


.. _user-guide-data-preparation-write-as-csv:

Write as CSV
============

The WRITE-AS-CSV directive converts a record into CSV format.

Syntax
------
::

  write-as-csv <column>

``column`` will contain the CSV representation of the record.

Usage Notes
-----------
- The WRITE-AS-CSV directive converts a record into CSV format.

- If the ``column`` already exists, the directive will overwrite it.

Example
-------
Consider the simple record::

  {
    "int": 1,
    "string": "This, is a string."
  }

Running the directive::

  write-as-csv body

will generate this record::

  {
    "body": "1,\"This, is a string.\"",
    "int": 1,
    "string": "This, is a string."
  }


.. _user-guide-data-preparation-write-as-json-map:

Write as a JSON Map
===================

The WRITE-AS-JSON-MAP directive converts the record into a JSON Map.

Syntax
------
::

  write-as-json <column>


``column`` will contain the JSON of the fields of the record.

Usage Notes
-----------

- The WRITE-AS-JSON-MAP directive converts a record into a JSON map.

- If the ``column`` already exists, the directive will overwrite it.

- Depending on the type of object a field is holding, it will be transformed
  appropriately.

Example
-------
Consider the simple record::

  {
    "int": 1,
    "string": "this is a string"
  }

Running the directive::

  write-as-json-map body


will generate this record::

  {
    "body": "[{\"key\": \"int\", \"value\" : \"1\"}, {\"key\" : \"string\", \"value\" : \"this is a string\"}]",
    "int": 1,
    "string": "this is a string"
  }
