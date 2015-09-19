.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

==========================
Sinks: Batch: SnapshotAvro
==========================

.. rubric:: Description

A batch sink for a FileSet that writes a snapshot of data in Avro format.
At the end of every pipeline run, the previous run's data will be overwritten
with the current run's data. All data for the run will be written to that
location in the file system.

.. rubric:: Use Case

This sink is used whenever you want access to a FileSet containing exactly the most
recent run's data in Avro format. Alternatively, it is used whenever you would like
the output of a run to be written to a constant location in a file system. For example,
you might want to create daily snapshots of a database by reading the entire contents of
a table, writing to this sink, and then other programs can analyze the contents of the specified file.

.. rubric:: Properties

**name:** Name of the FileSet to which records are written.
If it doesn't exist, it will be created.

**schema:** The Avro schema of the record being written to the sink as a JSON Object.

**basePath:** Base path for the FileSet. Defaults to the name of the dataset.

**pathExtension:** The extension where the dataset will be stored. The dataset will be stored at
<basePath>/<pathExtension.

.. rubric:: Example

::

  {
    "name": "SnapshotAvro",
    "properties": {
      "name": "users",
      "pathExtension": "latest",
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

This example will write to a FileSet named 'users'. It will write data in Avro format
using the given schema. Every time the pipeline runs, the most recent run will be stored in
the FileSet under the directory 'latest', and no records will exist of previous runs.
