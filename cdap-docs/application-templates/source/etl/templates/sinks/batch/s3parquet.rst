.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

==========================
Sinks: Batch: S3Parquet
==========================

.. rubric:: Description

A batch sink for a to write to Amazon S3 in Parquet format. 

.. rubric:: Use Case

This sink is used whenever you want to write data to S3 in Parquet format. 
Users might want to run a periodic processing job and write to S3 as a backup store. 
The output of the run will be stored in a directory with the suffix yyyy-mm-dd-hh from the base
path provided.
 
.. highlight:: xml

.. rubric:: Properties

**accessID:** Access ID of the Amazon S3 instance to connect to.

**accessKey:** Access key of the Amazon S3 instance to connect to.

**schema:** The Parquet schema of the record being written to the sink as a JSON Object.

**path:** Base path for S3 directory. Note: the path should start with ``s3n://``.


.. rubric:: Example

::

  {
    "name": "S3Parquet",
    "properties": {
      "accessKey": "key",
      "accessID": "ID",
      "path": "s3n://path/to/logs/",
      "name": "mys3",
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

This example will write to an S3 output located at ``s3n://path/to/logs``. It will write data in Parquet format
using the given schema. Every time the pipeline runs, a new output directory from the base path (``s3n://path/to/logs``)
will be created which will have the directory name corresponding to the start time in 'yyyy-mm-dd-hh' format.
