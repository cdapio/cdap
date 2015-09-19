.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Batch Sources: Elasticsearch
===============================

.. rubric:: Description

CDAP Elasticsearch Batch Source pulls documents from Elasticsearch according to the query
specified by the user and converts each document to a structured record with the fields
and schema specified by the user. The Elasticsearch server should be running prior to
creating the application.

.. rubric:: Use Case

This sink is used whenever you need to read from an Elasticsearch server.

.. rubric:: Properties

**hostname:** The hostname and port for the Elasticsearch server; for example,
localhost:9200.

**index:** The name of the index to query.

**query:** The query to use to import data from the specified index. See Elasticsearch for
query examples.

**schema:** The schema or mapping of the data in Elasticsearch.

**type:** The name of the type where the data is stored.

.. rubric:: Example
