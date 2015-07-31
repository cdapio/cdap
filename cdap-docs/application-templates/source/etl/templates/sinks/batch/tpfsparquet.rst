.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sinks: Batch: TPFSParquet
===============================

Sink for a TimePartitionedFileSet that writes data in Parquet format.
Every time the pipeline runs, a new partition in the TimePartitionedFileSet
will be created based on the logical start time of the run.
All data for the run will be written to that partition.

.. rubric:: Use Case

This sink is used whenever you want to write to a TimePartitionedFileSet in Parquet format.
For example, you might want to create daily snapshots of a database table by reading
the entire contents of the table and writing to this sink.

.. rubric:: Properties

**name:** Name of the TimePartitionedFileSet to which records are written.
If it doesn't exist, it will be created.

**schema:** The Avro schema of the record being written to the sink as a JSON Object.

**basePath:** Base path for the TimePartitionedFileSet. Defaults to the name of the dataset.

.. rubric:: Example

::

  {
    "name": "TPFSAvro",
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

This example will write to a TimePartitionedFileSet named 'users'. It will write data in Avro format
using the given schema. Every time the pipeline runs, a new partition in the TimePartitionedFileSet
will be created based on the logical start time of the run. All data for the run will be written to
that partition. For example, if the pipeline was scheduled to run at 10:00am on January 1, 2015,
a new partition will be created with year 2015, month 1, day 1, hour 10, and minute 0.

