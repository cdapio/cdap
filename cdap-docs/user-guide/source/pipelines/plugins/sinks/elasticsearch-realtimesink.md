# Elasticsearch


Description
-----------
Takes the Structured Record from the input source and converts it to a JSON string, then indexes it in
Elasticsearch using the index, type, and idField specified by the user. The Elasticsearch server should
be running prior to creating the application.

This sink is used whenever you need to write data into Elasticsearch.
For example, you may want to read Kafka logs and store them in Elasticsearch
to be able to search on them.


Configuration
-------------
**referenceName:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**es.cluster:** The name of the cluster to connect to; defaults to ``'elasticsearch'``.

**es.transportAddresses:** The addresses for nodes; specify the address for at least one node,
and separate others by commas; other nodes will be sniffed out.

**es.index:** The name of the index where the data will be stored; if the index does not already exist,
it will be created using Elasticsearch's default properties.

**es.type:** The name of the type where the data will be stored; if it does not already exist, it will be created.

**es.idField:** The field that will determine the id for the document; it should match a fieldname in the
Structured Record of the input; if left blank, Elasticsearch will create a unique id for each document.


Example
--------
This example connects to Elasticsearch, which is running locally, and writes the data to
the specified index (*logs*) and type (*cdap*). The data is indexed using the timestamp (*ts*) field
in the record:

    {
        "name": "Elasticsearch",
        "type": "batchsink",
        "properties": {
            "es.transportAddresses": "localhost:9300",
            "es.index": "logs",
            "es.type": "cdap",
            "es.idField": "ts"
        }
    }

---
- CDAP Pipelines Plugin Type: realtimesink
- CDAP Pipelines Version: 1.7.0
