.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-etl-plugins-batch-sinks-tpfsavro:

===============================
Batch Sinks: TPFSAvro
===============================

.. rubric:: Description

Sink for a ``TimePartitionedFileSet`` that writes data in Avro format.
Every time the pipeline runs, a new partition in the ``TimePartitionedFileSet``
will be created based on the logical start time of the run.
All data for the run will be written to that partition.

.. rubric:: Use Case

This sink is used whenever you want to write to a ``TimePartitionedFileSet`` in Avro format.
For example, you might want to create daily snapshots of a database table by reading
the entire contents of the table and writing to this sink.

.. rubric:: Properties

**name:** Name of the ``TimePartitionedFileSet`` to which records are written.
If it doesn't exist, it will be created.

**schema:** The Avro schema of the record being written to the sink as a JSON Object.

**basePath:** Base path for the ``TimePartitionedFileSet``. Defaults to the name of the dataset.

**filePathFormat:** Format for the time partition, as used by ``SimpleDateFormat``.
Defaults to formatting partitions such as ``2015-01-01/20-42.142017372000``.

**timeZone:** The string ID for the time zone to format the date in. Defaults to using UTC.
This setting is only used if ``filePathFormat`` is not null.

.. rubric:: Example

::

  {
    "name": "TPFSAvro",
    "properties": {
      "name": "users",
      "filePathFormat": "yyyy-MM-dd/HH-mm",
      "timeZone": "America/Los_Angeles",
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

This example will write to a ``TimePartitionedFileSet`` named ``'users'``. It will write data in
Avro format using the given schema. Every time the pipeline runs, a new partition in the
``TimePartitionedFileSet`` will be created based on the logical start time of the run with the
output directory ending with the date formatted as specified. All data for the run will be
written to that partition. For example, if the pipeline was scheduled to run at 10:00am on
January 1, 2015 in Los Angeles, a new partition will be created with year 2015, month 1,
day 1, hour 10, and minute 0, and the output directory for that partition would end with
``2015-01-01/10-00``.
