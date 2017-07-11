# MongoDB


Description
-----------
Converts a StructuredRecord into a BSONWritable and then writes it to a MongoDB collection.


Configuration
-------------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**connectionString:** MongoDB Connection String. Example: `mongodb://localhost:27017/analytics.users` (Macro-enabled)
[Reference](http://docs.mongodb.org/manual/reference/connection-string)

---
- CDAP Pipelines Plugin Type: batchsink
- CDAP Pipelines Version: 1.7.0
