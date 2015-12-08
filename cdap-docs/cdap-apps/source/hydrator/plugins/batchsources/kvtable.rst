.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-etl-plugins-batch-sources-kvtable:

=======================
Batch Sources: KVTable 
=======================

.. rubric:: Description 

Reads the entire contents of a KeyValueTable, outputting records with a 'key' field and a
'value' field. Both fields are of type bytes.

.. rubric:: Use Case

The source is used whenever you need to read from a KeyValueTable in batch. For example,
you may want to periodically dump the contents of a KeyValueTable to a Table.

.. rubric:: Properties

**name:** KeyValueTable name. If the table does not already exist, it will be created.

.. rubric:: Example

::

  {
    "name": "KVTable",
    "properties": {
      "name": "items",
    }
  }

This example reads from a KeyValueTable named 'items'. It outputs records with this schema::

  +====================+
  | field name | type  |
  +====================+
  | key        | bytes |
  | value      | bytes |
  +====================+

