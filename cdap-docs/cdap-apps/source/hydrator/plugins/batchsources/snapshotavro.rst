.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-etl-plugins-batch-sources-snapshotavro:

===========================
Batch Sources: SnapshotAvro
===========================

.. rubric:: Description

A batch source that reads from a corresponding SnapshotAvro sink.
The source will only read the most recent snapshot written to the sink.

.. rubric:: Use Case

This source is used whenever you want to read data written to the corresponding
SnapshotAvro sink. It will read only the last snapshot written to that sink.
For example, you might want to create daily snapshots of a database by reading the entire contents of
a table and writing it to a SnapshotAvro sink. You might then want to use this source to read the most
recent snapshot and run some data analysis on it.

.. rubric:: Properties

**name:** Name of the PartitionedFileSet to which records are written.
If it doesn't exist, it will be created.

**schema:** The Avro schema of the record being written to the sink as a JSON object.

**basePath:** Base path for the PartitionedFileSet. Defaults to the name of the dataset.

**fileProperties:** Advanced feature to specify any additional properties that should be used with the sink,
specified as a JSON object of string to string. These properties are set on the dataset if one is created.
The properties are also passed to the dataset at runtime as arguments.

.. rubric:: Example

::

  {
    "name": "SnapshotAvro",
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
      }"
    }
  }

This example will read from a SnapshotFileSet named 'users'. It will read data in Avro format
using the given schema. Every time the pipeline runs, only the most recently added snapshot will
be read.
