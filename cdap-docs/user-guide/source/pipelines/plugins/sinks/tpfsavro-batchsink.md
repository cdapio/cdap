# TimePartitionedFileSet Avro


Description
-----------
Sink for a ``TimePartitionedFileSet`` that writes data in Avro format.
Every time the pipeline runs, a new partition in the ``TimePartitionedFileSet``
will be created based on the logical start time of the run.
All data for the run will be written to that partition.


Use Case
--------
This sink is used whenever you want to write to a ``TimePartitionedFileSet`` in Avro format.
For example, you might want to create daily snapshots of a database table by reading
the entire contents of the table and writing to this sink.


Properties
----------
**name:** Name of the ``TimePartitionedFileSet`` to which records are written.
If it doesn't exist, it will be created. (Macro-enabled)

**schema:** The Avro schema of the record being written to the sink as a JSON Object. (Macro-enabled)

**basePath:** Base path for the ``TimePartitionedFileSet``. Defaults to the name of the dataset. (Macro-enabled)

**filePathFormat:** Format for the time partition, as used by ``SimpleDateFormat``.
Defaults to formatting partitions such as ``2015-01-01/20-42.142017372000``. (Macro-enabled)

**timeZone:** The string ID for the time zone to format the date in. Defaults to using UTC.
This setting is only used if ``filePathFormat`` is not null. (Macro-enabled)

**partitionOffset:** Amount of time to subtract from the pipeline runtime to determine the output partition. Defaults to 0m.
The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit,
with 's' for seconds, 'm' for minutes, 'h' for hours, and 'd' for days.
For example, if the pipeline is scheduled to run at midnight of January 1, 2016,
and the offset is set to '1d', data will be written to the partition for midnight Dec 31, 2015." (Macro-enabled)

**cleanPartitionsOlderThan:** Optional property that configures the sink to delete partitions older than a specified date-time after a successful run.
If set, when a run successfully finishes, the sink will subtract this amount of time from the runtime and delete any delete any partitions for time partitions older than that.
The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' for seconds,
'm' for minutes, 'h' for hours, and 'd' for days. For example, if the pipeline is scheduled to run at midnight of January 1, 2016,
and this property is set to 7d, the sink will delete any partitions for time partitions older than midnight Dec 25, 2015. (Macro-enabled)

**compressionCodec:** Optional parameter to determine the compression codec to use on the resulting data. 
Valid values are None, Snappy, and Deflate.

Example
-------
This example will write to a ``TimePartitionedFileSet`` named ``'users'``:

    {
        "name": "TPFSAvro",
        "type": "batchsink",
        "properties": {
            "name": "users",
            "filePathFormat": "yyyy-MM-dd/HH-mm",
            "timeZone": "America/Los_Angeles",
            "compressionCodec": "Snappy",
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

It will write data in Avro format using the given schema. Every time the pipeline runs, a
new partition in the ``TimePartitionedFileSet`` will be created based on the logical start
time of the run with the output directory ending with the date formatted as specified. All
data for the run will be written to that partition compressed using the Snappy codec.

For example, if the pipeline was scheduled to run at 10:00am on January 1, 2015 in Los
Angeles, a new partition will be created with year 2015, month 1, day 1, hour 10, and
minute 0, and the output directory for that partition would end with ``2015-01-01/10-00``.

---
- CDAP Pipelines Plugin Type: batchsink
- CDAP Pipelines Version: 1.7.0
