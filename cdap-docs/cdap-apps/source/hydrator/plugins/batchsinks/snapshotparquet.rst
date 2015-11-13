.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-etl-plugins-batch-sinks-snapshotparquet:

============================
Batch Sinks: SnapshotParquet
============================

.. rubric:: Description

A batch sink for a PartitionedFileSet that writes snapshots of data as a new
partition. Data is written in Parquet format. A corresponding SnapshotParquet source
can be used to read only the most recently written snapshot.

.. rubric:: Use Case

This sink is used whenever you want access to a PartitionedFileSet containing exactly the most
recent run's data in Parquet format. For example,
you might want to create daily snapshots of a database by reading the entire contents of
a table, writing to this sink, and then other programs can analyze the contents of the specified file.

.. rubric:: Properties

**name:** Name of the PartitionedFileSet to which records are written.
If it doesn't exist, it will be created.

**schema:** The Parquet schema of the record being written to the sink as a JSON object.

**basePath:** Base path for the PartitionedFileSet. Defaults to the name of the dataset.

**fileProperties:** Advanced feature to specify any additional properties that should be used with the sink,
specified as a JSON object of string to string. These properties are set on the dataset if one is created.
The properties are also passed to the dataset at runtime as arguments.

.. rubric:: Example

::

  {
    "name": "SnapshotParquet",
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

This example will write to a PartitionedFileSet named 'users'. It will write data in Parquet format
using the given schema. Every time the pipeline runs, the most recent run will be stored in
a new partition in the PartitionedFileSet.
