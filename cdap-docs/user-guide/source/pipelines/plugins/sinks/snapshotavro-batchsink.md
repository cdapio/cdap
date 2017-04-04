# Snapshot Avro


Description
-----------
A batch sink for a PartitionedFileSet that writes snapshots of data as a new
partition. Data is written in Avro format. A corresponding SnapshotAvro source
can be used to read only the most recently written snapshot.


Use Case
--------
This sink is used whenever you want access to a PartitionedFileSet containing exactly the
most recent run's data in Avro format. For example, you might want to create daily
snapshots of a database by reading the entire contents of a table, writing to this sink,
and then other programs can analyze the contents of the specified file.


Properties
----------
**name:** Name of the PartitionedFileSet to which records are written.
If it doesn't exist, it will be created. (Macro-enabled)

**schema:** The Avro schema of the record being written to the sink as a JSON object. (Macro-enabled)

**basePath:** Base path for the PartitionedFileSet. Defaults to the name of the dataset. (Macro-enabled)

**fileProperties:** Advanced feature to specify any additional properties that should be used with the sink,
specified as a JSON object of string to string. These properties are set on the dataset if one is created.
The properties are also passed to the dataset at runtime as arguments. (Macro-enabled)

**cleanPartitionsOlderThan:** Optional property that configures the sink to delete partitions older than a specified date-time after a successful run.
If set, when a run successfully finishes, the sink will subtract this amount of time from the runtime and delete any delete any partitions for time partitions older than that.
The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' for seconds,
'm' for minutes, 'h' for hours, and 'd' for days. For example, if the pipeline is scheduled to run at midnight of January 1, 2016,
and this property is set to 7d, the sink will delete any partitions for time partitions older than midnight Dec 25, 2015. (Macro-enabled)

**compressionCodec:** Optional parameter to determine the compression codec to use on the resulting data. 
Valid values are None, Snappy, and Deflate.

Example
-------
This example will write to a PartitionedFileSet named 'users'. It will write data in Avro format
using the given schema. Every time the pipeline runs, the most recent run will be stored in
a new partition in the PartitionedFileSet:

    {
        "name": "SnapshotAvro",
        "type": "batchsink",
        "properties": {
            "name": "users",
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

---
- CDAP Pipelines Plugin Type: batchsink
- CDAP Pipelines Version: 1.7.0
