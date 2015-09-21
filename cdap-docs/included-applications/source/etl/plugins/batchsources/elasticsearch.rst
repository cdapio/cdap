.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-batch-sources-elasticsearch:

===============================
Batch Sources: Elasticsearch
===============================

.. rubric:: Description

CDAP Elasticsearch Batch Source pulls documents from Elasticsearch according to the query
specified by the user and converts each document to a structured record with the fields
and schema specified by the user. The Elasticsearch server should be running prior to
creating the application.

.. rubric:: Use Case

This source is used whenever you need to read data from Elasticsearch. For example, you
may want to read in an index and type from Elasticsearch and store the data in an HBase
table.

.. rubric:: Properties

**es.host:** The hostname and port for the Elasticsearch server; for example,
``localhost:9200``.

**es.index:** The name of the index to query.

**es.type:** The name of the type where the data is stored.

**query:** The query to use to import data from the specified index. See 
`Elasticsearch for query examples 
<https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>`__.

**schema:** The schema or mapping of the data in Elasticsearch.

.. rubric:: Example

::

  {
    "name": "Elasticsearch",
    "properties": {
      "es.host": "localhost:9200",
      "es.index": "megacorp",
      "es.type": "employee",
      "query": "?q=*",
      "schema": "{
        \"type\":\"record\",
        \"name\":\"etlSchemaBody\",
        \"fields\":[
          {\"name\":\"id\",\"type\":\"long\"},
          {\"name\":\"name\",\"type\":\"string\"},
          {\"name\":\"age\",\"type\":\"int\"}]}"
    }
  }

This example connects to Elasticsearch, which is running locally, and reads in records in
the specified index (``megacorp``) and type (``employee``) which match the query to (in
this case) select all records. All data from the index will be read on each run.