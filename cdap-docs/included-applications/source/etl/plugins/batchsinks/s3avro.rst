.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-batch-sinks-s3avro:

===================
Batch Sinks: S3Avro
===================

.. rubric:: Description

Batch sink to write to S3 in Avro format.

.. rubric:: Use Case

This source is used whenever you need to write to Amazon S3 in Avro format. For example,
you might want to create daily snapshots of a database by reading the entire contents of a
table, writing to this sink, and then other programs can analyze the contents of the
specified file.

.. highlight:: xml

.. rubric:: Properties

**accessID:** Access ID of the Amazon S3 instance to connect to.

**accessKey:** Access Key of the Amazon S3 instance to connect to.

**basePath:** The path where the data will be recorded. Defaults to the name of the dataset

**pathFormat:** The format for the path that will be suffixed to the basePath; for
example: the format ``'yyyy-MM-dd-HH-mm'`` will create a file path ending in
``'2015-01-01-20-42'``. Default format used is ``'yyyy-MM-dd-HH-mm'``.

**schema:** The Avro schema of the record being written to the sink as a JSON object.

.. rubric:: Example

::

  {
    "name": "S3Avro",
    "properties": {
      "accessKey": "key",
      "accessID": "ID",
      "basePath": "s3n://logs",
      "pathFormat": "yyyy-MM-dd-HH-mm",
      "schema": "{
        \"type\":\"record\",
        \"name\":\"user\",
        \"fields\":[
          {\"name\":\"id\",\"type\":\"long\"},
          {\"name\":\"name\",\"type\":\"string\"},
          {\"name\":\"birthyear\",\"type\":\"int\"}
        ]
      }"
    }
  }

This example will connect to Amazon S3 and write files following the pattern
'yyyy-MM-dd-HH-mm'. It will write data in Parquet format using the given schema. Every
time the pipeline runs, the most recent run will be stored in S3 under the directory
'logs'.
