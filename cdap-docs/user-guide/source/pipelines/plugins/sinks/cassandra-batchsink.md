# Cassandra


Description
-----------
Batch sink to use Apache Cassandra as a sink.

**Note** Apache Cassandra v. 2.1.0 is currently the only supported version of Apache Cassandra.


Use Case
--------
This sink is used whenever you need to write data into Cassandra.
For example, you may want to parse a file and read its contents into Cassandra,
which you can achieve with a stream batch source and Cassandra as a sink.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**initialAddress:** The initial address to connect to. (Macro-enabled)

**port:** The RPC port for Cassandra.
Check the configuration to make sure that ``start_rpc`` is true in ``cassandra.yaml``. (Macro-enabled)

**keyspace:** The keyspace to inject data into.
Create the keyspace before starting the adapter. (Macro-enabled)

**partitioner:** The partitioner for the keyspace. (Macro-enabled)

**columnFamily:** The column family or table to inject data into.
Create the column family before starting the adapter. (Macro-enabled)

**columns:** A comma-separated list of columns in the column family.
The columns should be listed in the same order as they are stored in the column family.

**primaryKey:** A comma-separated list of primary keys.


Example
-------
This example connects to Apache Cassandra, which is running locally, and writes the data to
the specified column family (*employees*), which is in the *megacorp* keyspace.
This column family has four columns and two primary keys, and Apache Cassandra
uses the default *Murmur3* partitioner:

    {
        "name": "Cassandra",
        "type" "batchsink",
        "properties": {
            "initialAddress": "localhost",
            "port": "9160",
            "keyspace": "megacorp",
            "partitioner": "org.apache.cassandra.dht.Murmur3Partitioner",
            "columnFamily": "employees",
            "columns": "fname,lname,age,salary",
            "primaryKey": "fname,lname"
        }
    }

---
- CDAP Pipelines Plugin Type: batchsink
- CDAP Pipelines Version: 1.7.0
