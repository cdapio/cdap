# MongoDB


Description
-----------
Reads documents from a MongoDB collection and converts each document into a StructuredRecord with the help
of a specified schema. The user can optionally provide input query, input fields, and splitter classes.


Configuration
-------------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**connectionString:** MongoDB connection string. Example: `mongodb://localhost:27017/analytics.users` (Macro-enabled)
[Reference](http://docs.mongodb.org/manual/reference/connection-string)

**schema:** Specifies the schema of the documents.

**authConnectionString:** Auxiliary MongoDB connection string to authenticate against when constructing splits. (Macro-enabled)

**inputQuery:** Optionally filter the input collection with a query. This query must be represented in JSON format
and use the MongoDB extended-JSON format to represent non-native JSON data types. (Macro-enabled)

**inputFields:** Projection document that can limit the fields that appear in each document. 
If no projection document is provided, all fields will be read. (Macro-enabled)

**splitterClass:** The name of the Splitter class to use. If left empty, the MongoDB Hadoop Connector will attempt
to make a best-guess as to which Splitter to use. (Macro-enabled) The Hadoop connector provides these Splitters:

  - `com.mongodb.hadoop.splitter.StandaloneMongoSplitter`
  - `com.mongodb.hadoop.splitter.ShardMongoSplitter`
  - `com.mongodb.hadoop.splitter.ShardChunkMongoSplitter`
  - `com.mongodb.hadoop.splitter.MultiMongoCollectionSplitter`

---
- CDAP Pipelines Plugin Type: batchsource
- CDAP Pipelines Version: 1.7.0
