# Hive


Description
-----------
Converts a StructuredRecord to a HCatRecord and then writes it to an existing Hive table.


Configuration
-------------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**metastoreURI:** The URI of Hive metastore in the format ``thrift://<hostname>:<port>``.
Example: ``thrift://somehost.net:9083``.

**tableName:** The name of the Hive table. This table must exist.

**databaseName:** The name of the database. Defaults to 'default'.

**partitions:** Optional Hive expression filter for writing, provided as a JSON Map of key-value pairs that describe all of the
partition keys and values for that partition. For example: if the partition column is 'type', then this property
should be specified as ``{"type": "typeOne"}``.
To write multiple partitions simultaneously you can leave this empty; but all of the partitioning columns must
be present in the data you are writing to the sink.

**schema:** Optional schema to use while writing to the Hive table. If no schema is provided, then the schema of the
table will be used and it should match the schema of the data being written.

---
- CDAP Pipelines Plugin Type: batchsink
- CDAP Pipelines Version: 1.7.0
