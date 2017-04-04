# MongoDB


Description
-----------
Takes a StructuredRecord from a previous node, converts it into a BSONDocument and writes it to a MongoDB collection.


Configuration
-------------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**connectionString:** MongoDB connection string. Example: `mongodb://localhost:27017/analytics.users`
[Reference](http://docs.mongodb.org/manual/reference/connection-string)

**dbName:** MongoDB database name. A physical container for collections. 
Each database has its own set of files on the file system. A single MongoDB server typically includes multiple databases.

**collectionName:** MongoDB collection name. A grouping of MongoDB documents. 
A collection is the equivalent of an RDBMS table and exists within a single database.
Collections do not enforce a schema. Documents within a collection can have different fields. 
Typically, all documents in a collection have a similar or related purpose
[Reference](https://docs.mongodb.org/manual/reference/glossary/#term-collection).

---
- CDAP Pipelines Plugin Type: realtimesink
- CDAP Pipelines Version: 1.7.0
