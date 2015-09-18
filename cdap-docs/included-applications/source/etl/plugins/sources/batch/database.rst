.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

========================
Sources: Batch: Database 
========================

.. rubric:: Description

Reads from a database using a configurable SQL query.
Outputs one record for each row returned by the query.

.. rubric:: Use Case

The source is used whenever you need to read from a database. For example, you may want
to create daily snapshots of a database table by using this source and writing to
a TimePartitionedFileSet.

.. rubric:: Properties

**importQuery:** The SELECT query to use to import data from the specified table. You can
specify an arbitrary number of columns to import, or import all columns using \*. You can
also specify a number of WHERE clauses or ORDER BY clauses. However, LIMIT and OFFSET
clauses should not be used in this query.

.. highlight:: sql

**countQuery:** The SELECT query to use to get the count of records to import from the
specified table. Examples::

  SELECT COUNT(*) from <my_table> where <my_column> 1
  SELECT COUNT(my_column) from my_table

*Note:* Please include the same WHERE clauses in this query as the ones used in the import
query to reflect an accurate number of records to import.

**connectionString:** JDBC connection string including database name.

**user:** User identity for connecting to the specified database. Required for databases that need
authentication. Optional for databases that do not require authentication.

**password:** Password to use to connect to the specified database. Required for databases
that need authentication. Optional for databases that do not require authentication.

**jdbcPluginName:** Name of the JDBC plugin to use. This is the value of the 'name' key
defined in the JSON file for the JDBC plugin.

**jdbcPluginType:** Type of the JDBC plugin to use. This is the value of the 'type' key
defined in the JSON file for the JDBC plugin. Defaults to 'jdbc'.

.. rubric:: Example

::

  {
    "name": "Database",
    "properties": {
      "importQuery": "select id,name,email,phone from users",
      "countQuery": "select count(*) from users",
      "connectionString": "jdbc:postgresql://localhost:5432/prod",
      "user": "postgres",
      "password": "",
      "jdbcPluginName": "postgres",
      "jdbcPluginType": "jdbc"
    }
  }

This example connects to a database using the specified 'connectionString', which means
it will connect to the 'prod' database of a PostgreSQL instance running on 'localhost'.
It will run the 'importQuery' against the 'users' table to read four columns from the table.
The column types will be used to derive the record field types output by the source.
For example, if the 'id' column is a primary key of type int and the other columns are
non-nullable varchars, output records will have this schema::

  +======================================+
  | field name     | type                |
  +======================================+
  | id             | int                 |
  | name           | string              |
  | email          | string              |
  | phone          | string              |
  +======================================+

