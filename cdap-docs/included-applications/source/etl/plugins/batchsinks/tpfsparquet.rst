.. meta::
    :author: cask data, inc.
    :copyright: copyright © 2015 cask data, inc.

.. _included-apps-etl-plugins-batch-sinks-tpfsparquet:

===============================
batch sinks: tpfsparquet
===============================

.. rubric:: description

sink for a timepartitionedfileset that writes data in parquet format.
every time the pipeline runs, a new partition in the timepartitionedfileset
will be created based on the logical start time of the run.
all data for the run will be written to that partition.

.. rubric:: use case

this sink is used whenever you want to write to a timepartitionedfileset in parquet format.
for example, you might want to create daily snapshots of a database table by reading
the entire contents of the table and writing to this sink.

.. rubric:: properties

**name:** name of the timepartitionedfileset to which records are written.
if it doesn't exist, it will be created.

**schema:** the avro schema of the record being written to the sink as a json object.

**basepath:** base path for the timepartitionedfileset. defaults to the name of the dataset.

**filepathformat:** format for the time partition, as used by simpledateformat.
defaults to formatting partitions as 2015-01-01/20-42.142017372000.

**timezone:** the string id for the timezone to format the date in. defaults to using utc.
this setting is only used if filepathformat is not null.

.. rubric:: example

::

  {
    "name": "tpfsavro",
    "properties": {
      "name": "users",
      "filepathformat": "yyyy-mm-dd/hh-mm",
      "timezone": "america/los_angeles",
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

this example will write to a timepartitionedfileset named 'users'. it will write data in
avro format using the given schema. every time the pipeline runs, a new partition in the
timepartitionedfileset will be created based on the logical start time of the run with the
output directory ending with the date formatted as specified. all data for the run will be
written to that partition. for example, if the pipeline was scheduled to run at 10:00am on
january 1, 2015 in los angeles, a new partition will be created with year 2015, month 1,
day 1, hour 10, and minute 0, and the output directory for that partition would end with
2015-01-01/10-00.
