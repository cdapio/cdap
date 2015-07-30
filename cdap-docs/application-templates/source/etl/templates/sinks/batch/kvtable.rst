.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sinks: Batch: KVTable
===============================

.. rubric:: Description

Writes records to a KeyValueTable, using configurable fields from input records as the
key and value.

.. rubric:: Use Case

The source is used whenever you need to write to a KeyValueTable in batch. For example,
you may want to periodically copy portions of a Table into a KeyValueTable.

.. rubric:: Properties

**name:** Name of the dataset. If it does not already exist, one will be created.

**key.field:** The name of the field to use as the key. Defaults to 'key'.

**value.field:** The name of the field to use as the value. Defaults to 'value'.

.. rubric:: Example

::

  {
    "name": "KVTable",
    "properties": {
      "name": "items",
      "key.field": "id",
      "value.field": "description"
    }
  }

This example writes to a KeyValueTable named 'items'. It takes records with the following schema as input::

  +======================================+
  | field name     | type                |
  +======================================+
  | id             | bytes               |
  | description    | bytes               |
  +======================================+

When writing to the KeyValueTable, the 'id' field will be used as the key,
and the 'description' field will be used as the value.
