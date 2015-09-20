.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-real-time-sinks-elasticsearch:

===============================
Real-time Sinks: Elasticsearch
===============================

.. rubric:: Description

CDAP Elasticsearch Real-time Sink takes the structured record from the input source and
converts it to a JSON string, then indexes it in Elasticsearch using the index, type, and
idField specified by the user. The Elasticsearch server should be running prior to creating the
application.

.. rubric:: Use Case

This sink is used whenever you need to write data into Elasticsearch. For example, you may
want to read Kafka logs and store them in Elasticsearch to be able to search on them.

.. rubric:: Properties

**es.cluster:** The name of the cluster to connect to. Defaults to ``'elasticsearch'``.

**es.transportAddresses:** The addresses for nodes. Specify the address for at least one
node, and separate others by commas. Other nodes will be sniffed out.

**es.index:** The name of the index where the data will be stored. If the index does not
already exist, it will be created using Elasticsearch's default properties.

**es.type:** The name of the type where the data will be stored. If it does not already
exist, it will be created.

**es.idField:** The field that will determine the id for the document. It should match a
fieldname in the structured record of the input. If left blank, Elasticsearch will create
a unique id for each document.

.. rubric:: Example

::

  {
   "name": "Elasticsearch",
      "properties": {
        "es.transportAddresses": "localhost:9300",
        "es.index": "logs",
        "es.type": "cdap",
        "es.idField": "ts"
      }
  }
  
This example connects to Elasticsearch, which is running locally, and writes the data to
the specified index (``logs``) and type (``cdap``). The data is indexed using the
timestamp (``ts``) field in the record.
