.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-batch-sinks-table:

===============================
Batch Sinks: Table 
===============================

.. rubric:: Description

Writes records to a Table with one record field mapping
to the Table rowkey, and all other record fields mapping to Table columns.

.. rubric:: Use Case

The sink is used whenever you need to write to a Table in batch. For example,
you may want to periodically dump the contents of a relational database into a CDAP Table.

.. rubric:: Properties

**name:** Name of the table dataset. If it does not already exist, one will be created.

**schema:** Optional schema of the table as a JSON Object. If the table does not
already exist, one will be created with this schema, which will allow the table to be
explored through Hive.

**schema.row.field:** The name of the record field that should be used as the row
key when writing to the table.

**case.sensitive.row.field:** Whether 'schema.row.field' is case sensitive; defaults to true.

.. rubric:: Example

::

  {
    "name": "Table",
    "properties": {
      "name": "users",
      "schema": "{
        \"type\":\"record\",
        \"name\":\"user\",
        \"fields\":[
          {\"name\":\"id\",\"type\":\"long\"},
          {\"name\":\"name\",\"type\":\"string\"},
          {\"name\":\"birthyear\",\"type\":\"int\"}
        ]
      }",
      "schema.row.field": "id"
    }
  }

This example writes to a Table named 'users'. It takes records with the following schema as input::

  +======================================+
  | field name     | type                |
  +======================================+
  | id             | long                |
  | name           | string              |
  | birthyear      | int                 |
  +======================================+

The 'id' field will be used as the rowkey when writing to the table. The 'name' and 'birthyear' record
fields will be written to columns named 'name' and 'birthyear'.
