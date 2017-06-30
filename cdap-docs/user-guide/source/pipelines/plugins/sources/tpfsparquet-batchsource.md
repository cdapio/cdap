# TimePartitionedFileSet Parquet


Description
-----------
Reads from a TimePartitionedFileSet whose data is in Parquet format.


Use Case
--------
The source is used when you need to read partitions of a TimePartitionedFileSet.
For example, suppose there is an application that ingests data by writing to a TimePartitionedFileSet,
where arrival time of the data is used as the partition key. You may want to create a pipeline that
reads the newly-arrived files, performs data validation and cleansing, and then writes to a Table.


Properties
----------
**name:** Name of the TimePartitionedFileSet from which the records are to be read from. (Macro-enabled)

**schema:** The Parquet schema of the record being read from the source as a JSON Object.

**basePath:** Base path for the TimePartitionedFileSet. Defaults to the name of the
dataset. (Macro-enabled)

**duration:** Size of the time window to read with each run of the pipeline. The format is
expected to be a number followed by an 's', 'm', 'h', or 'd' (specifying the time unit), with
's' for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, a value of
'5m' means each run of the pipeline will read 5 minutes of events from the TPFS source. (Macro-enabled)

**delay:** Optional delay for reading from TPFS source. The value must be of the same
format as the duration value. For example, a duration of '5m' and a delay of '10m' means
each run of the pipeline will read events for 5 minutes of data from 15 minutes before its logical
start time to 10 minutes before its logical start time. The default value is 0. (Macro-enabled)


Example
-------
This example reads from a TimePartitionedFileSet named 'webactivity', assuming the underlying
files are in Parquet format:

    {
        "name": "TPFSParquet",
        "type": "batchsource",
        "properties": {
            "name": "webactivity",
            "duration": "5m",
            "delay": "1m",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"webactivity\",
                \"fields\":[
                    {\"name\":\"date\",\"type\":\"string\"},
                    {\"name\":\"userid\",\"type\":\"long\"},
                    {\"name\":\"action\",\"type\":\"string\"},
                    {\"name\":\"item\",\"type\":\"string\"}
                ]
            }"
        }
    }

TimePartitionedFileSets are partitioned by year, month, day, hour, and minute. Suppose the
current run was scheduled to start at 10:00am on January 1, 2015. Since the 'delay'
property is set to one minute, only data before 9:59am January 1, 2015 will be read. This
excludes the partition for year 2015, month 1, day 1, hour 9, and minute 59. Since the
'duration' property is set to five minutes, a total of five partitions will be read. This
means partitions for year 2015, month 1, day 1, hour 9, and minutes 54, 55, 56, 57, and 58
will be read. 

The source will read the actual data using the given schema and will output records with
this schema:

    +=======================+
    | field name  | type    |
    +=======================+
    | date        | string  |
    | userid      | long    |
    | action      | string  |
    | item        | string  |
    +=======================+

---
- CDAP Pipelines Plugin Type: batchsource
- CDAP Pipelines Version: 1.7.0
