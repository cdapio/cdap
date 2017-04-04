# SolrSearch


Description
-----------
The Solr search batch sink takes the structured record from the input source and indexes it into the Solr server or
SolrCloud using the collection and key field specified by the user.

The incoming fields from the previous stage in pipelines are mapped to Solr fields. Also, user is able to specify the
mode of the Solr to connect to. For example, SingleNode Solr or SolrCloud.

Use Case
--------
Solr search batch sink is used to write data to the Solr server or SolrCloud. For example, the plugin can be used in
conjuction with a stream batch source to parse a file and read its contents in Solr.

Properties
----------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**solrMode:** Solr mode to connect to. For example, SingleNode Solr or SolrCloud.

**solrHost:** The hostname and port for the Solr server. For example, localhost:8983 if SingleNode Solr or
zkHost1:2181,zkHost2:2181,zkHost3:2181 for SolrCloud.

**collectionName:** Name of the collection where data will be indexed and stored in Solr.

**keyField:** Field that will determine the unique key for the document to be indexed. It must match a field name
in the structured record of the input.

**batchSize:** Number of documents to create a batch and send it to Solr for indexing. After each batch, commit will
be triggered. Default batch size is 10000. (Macro-enabled)

**outputFieldMappings:** List of the input fields to map to the output Solr fields. This is a comma-separated list of
key-value pairs, where each pair is separated by a colon ':' and specifies the input and output names. For example,
'firstname:fname,lastname:lname' specifies that the 'firstname' should be renamed to 'fname' and the 'lastname'
should be renamed to 'lname'.

Conditions
----------
The Solr server should be running prior to creating the application.

All the fields that user wants to index into the Solr server, should be properly declared and defined in Solr's
schema.xml file. The Solr server schema should be properly defined prior to creating the application.

If keyField('id') in the input record is NULL, then that particular record will be filtered out.

Example
-------
This example connects to a 'SinlgeNode Solr' server, running locally at the default port of 8983, and writes the
data to the specified collection (test_collection). The data is indexed using the id field coming in the input record
. And also the fieldname 'office address' is mapped to the 'address' field in Solr's index.

    {
      "name": "SolrSearch",
      "type": "batchsink",
        "properties": {
          "solrMode": "SingleNode",
          "solrHost": "localhost:8983",
          "collectionName": "test_collection",
          "keyField": "id",
          "batchSize": "10000",
          "outputFieldMappings": "office address:address"
        }
    }

For example, suppose the Solr search sink receives the input record:

    +===================================================================================================+
    | id : STRING | firstname : STRING  | lastname : STRING |  office address : STRING  | pincode : INT |
    +===================================================================================================+
    | 100A        | John                | Wagh              |  NE Lakeside              | 480001        |
    | 100B        | Brett               | Lee               |  SE Lakeside              | 480001        |
    +===================================================================================================+

 Once Solr search sink plugin execution is completed, all the rows from input data will be indexed in the
 test_collection with the fields id, firstname, lastname, address and pincode.

---
- CDAP Pipelines Plugin Type: batchsink
- CDAP Pipelines Version: 1.7.0
