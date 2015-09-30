.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-batch-sinks-database:

===============================
Batch Sinks: Database
===============================

.. rubric:: Description

Writes records to a database table. Each record will be written to a row in the table.

.. rubric:: Use Case

This sink is used whenever you need to write to a database table.
Suppose you periodically build a recommendation model for products on your online store.
The model is stored in a FileSet and you want to export the contents
of the FileSet to a database table where it can be served to your users.

.. rubric:: Properties

**tableName:** Name of the table to export to.

**columns:** Comma-separated list of columns in the specified table to export to.

.. ifconfig:: release_version in ('greater_than_3.2.0')

  **columnCase:** Sets the case of the column names returned by the column check query.
  Possible options are ``upper`` or ``lower``. By default or for any other input, the column names are not modified and
  the names returned from the database are used as-is. Note that setting this property provides predictability
  of column name cases across different databases but might result in column name conflicts if multiple column
  names are the same when the case is ignored. (Optional)

.. connection information from DBConfig.java

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
      "tableName": "users",
      "columns": "id,name,email,phone",
      "connectionString": "jdbc:postgresql://localhost:5432/prod",
      "user": "postgres",
      "password": "",
      "jdbcPluginName": "postgres",
      "jdbcPluginType": "jdbc"
    }
  }

This example connects to a database using the specified 'connectionString', which means
it will connect to the 'prod' database of a PostgreSQL instance running on 'localhost'.
Each input record will be written to a row of the 'users' table, with the value for each
column taken from the value of the field in the record. For example, the 'id' field in
the record will be written to the 'id' column of that row.
