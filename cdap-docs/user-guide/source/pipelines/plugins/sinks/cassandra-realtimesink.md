# Cassandra


Description
-----------
Real-time sink to use Apache Cassandra as a sink.

**Note** Apache Cassandra v. 2.1.0 is currently the only supported version of Apache Cassandra.


Use Case
--------
This sink is used whenever you need to write data into Cassandra.
For example, you may want, in real time, to collect purchase records
and store them in Cassandra for later access.


Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**addresses:** A comma-separated list of address(es) to connect to.

**keyspace:** The keyspace to inject data into.
Create the keyspace before starting the adapter.

**username:** The username for the keyspace (if one exists).
If this is not empty, then you must also supply a password.

**password:** The password for the keyspace (if one exists).
If this is not empty, then you must also supply a username.

**columnFamily:** The column family or table to inject data into.
Create the column family before starting the adapter.

**columns:** A comma-separated list of columns in the column family.
The columns should be listed in the same order as they are stored in the column family.

**consistencyLevel:** The string representation of the consistency level for the query.

**compression:** The string representation of the compression for the query.


Example
-------
This example connects to Apache Cassandra, which is running locally, and writes the data to
the specified keyspace (*megacorp*) and column family (*purchases*):

    {
        "name": "Cassandra",
        "type": "realtimesink",
        "properties": {
            "addresses": "localhost:9042",
            "keyspace": "megacorp",
            "columnFamily": "purchases",
            "columns": "fname,lname,email,price",
            "consistencyLevel": "QUORUM",
            "compression": "NONE"
        }
    }

---
- CDAP Pipelines Plugin Type: realtimesink
- CDAP Pipelines Version: 1.7.0
