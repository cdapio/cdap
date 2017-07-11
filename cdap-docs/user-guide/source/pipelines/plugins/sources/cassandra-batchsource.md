# Cassandra


Description
-----------
Batch source to use Apache Cassandra as a source.

**Note** Apache Cassandra v. 2.1.0 is currently the only supported version of Apache Cassandra.


Use Case
--------
This source is used whenever you need to read data from Apache Cassandra.
For example, you may want to read in a column family from Cassandra
and store the data in an HBase table.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**initialAddress:** The initial address to connect to. (Macro-enabled)

**port:** The RPC port for Cassandra.
Check the configuration to make sure that ``start_rpc`` is true in ``cassandra.yaml``. (Macro-enabled)

**keyspace:** The keyspace to select data from. (Macro-enabled)

**partitioner:** The partitioner for the keyspace. (Macro-enabled)

**username:** The username for the keyspace (if one exists).
If this is not empty, then you must supply a password. (Macro-enabled)

**password:** The password for the keyspace (if one exists).
If this is not empty, then you must supply a username. (Macro-enabled)

**columnFamily:** The column family or table to select data from. (Macro-enabled)

**query:** The query to select data on. (Macro-enabled)

**schema:** The schema for the data as it will be formatted in CDAP.

**properties:** Any extra properties to include. The property-value pairs should be comma-separated,
and each property should be separated by a colon from its corresponding value.


Example
-------
This example connects to Apache Cassandra, which is running locally, and reads in records in the
specified keyspace (*megacorp*) and column family (*employee*) which match the query to (in this case) select all records.
All data from the column family will be read on each run:

    {
        "name": "Cassandra",
        "type": "batchsource",
        "properties": {
            "initialAddress": "localhost",
            "port": "9160",
            "keyspace": "megacorp",
            "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
            "columnFamily": "employees",
            "query": "select * from employees where token(id) > ? and token(id) <= ?",
            "schema": "{
                \"type\":\"record\",
                \"name\":\"etlSchemaBody\",
                \"fields\":[
                    {\"name\":\"id\",\"type\":\"long\"},
                    {\"name\":\"name\",\"type\":\"string\"},
                    {\"name\":\"age\",\"type\":\"int\"}
                ]
            }"
        }
    }

---
- CDAP Pipelines Plugin Type: batchsource
- CDAP Pipelines Version: 1.7.0
