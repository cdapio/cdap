.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sinks: Batch: Elasticsearch
===============================

.. rubric:: Description

CDAP Elasticsearch Batch Sink takes the structured record from the input source and
converts it to a JSON string, then indexes it in Elasticsearch using the index, type, and
id specified by the user. The Elasticsearch server should be running prior to creating the
adapter.

.. rubric:: Use Case

This sink is used whenever you need to write to an Elasticsearch server.

.. rubric:: Properties

**index:** The name of the index where the data will be stored If the index does not
already exist, it will be created using Elasticsearch's default properties.

**type:** The name of the type where the data will be stored. If it does not already
exist, it will be created.

**idField:** The field that will determine the id for the document. It should match a fieldname
in the structured record of the input.

**hostname:** The hostname and port for the Elasticsearch server; such as localhost:9200.

.. rubric:: Example

::

  {
    "name": "Database",
    "properties": {
      "index": "users",
      "type": "id,name,email,phone",
      "idField": "jdbc:postgresql://localhost:5432/prod",
      "hostname": "localhost:9200"
    }
  }

This example connects to a database using the specified 'connectionString', which means
it will connect to the 'prod' database of a PostgreSQL instance running on 'localhost'.
Each input record will be written to a row of the 'users' table, with the value for each
column taken from the value of the field in the record. For example, the 'id' field in
the record will be written to the 'id' column of that row.

