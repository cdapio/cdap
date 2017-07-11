# Hive


Description
-----------
Reads records from a Hive table and converts each record into a StructuredRecord with the help
of the specified schema (if provided) or the table's schema.


Configuration
-------------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**metastoreURI:** The URI of Hive metastore in the format of ``thrift://<hostname>:<port>``.
Example: ``thrift://somehost.net:9083``.

**tableName:** The name of the Hive table. This table must exist.

**databaseName:** The name of the database. Defaults to 'default'.

**partitions:** Optional Hive expression filter for scan. This filter must only reference partition columns.
Values from other columns will cause the pipeline to fail.

**schema:** Optional schema to use while reading from the Hive table. If no schema is provided, then the schema of the
table will be used. Note: if you want to use a Hive table which has non-primitive types as a source, then you
should provide a schema with all non-primitive fields dropped, otherwise your pipeline will fail.

---
- CDAP Pipelines Plugin Type: batchsource
- CDAP Pipelines Version: 1.7.0
