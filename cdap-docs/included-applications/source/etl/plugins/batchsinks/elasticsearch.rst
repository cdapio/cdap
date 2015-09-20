.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-batch-sinks-elasticsearch:

===============================
Batch Sinks: Elasticsearch
===============================

.. rubric:: Description

CDAP Elasticsearch Batch Sink takes the structured record from the input source and
converts it to a JSON string, then indexes it in Elasticsearch using the index, type, and
idField specified by the user. The Elasticsearch server should be running prior to creating the
application.

.. rubric:: Use Case

This sink is used whenever you need to write to an Elasticsearch server. For example, you
may want to parse a file and read its contents into Elasticsearch, which you can achieve
with a stream batch source and Elasticsearch as a sink.

.. rubric:: Properties

**es.host:** The hostname and port for the Elasticsearch server; for example, ``localhost:9200``.

**es.index:** The name of the index where the data will be stored If the index does not
already exist, it will be created using Elasticsearch's default properties.

**es.type:** The name of the type where the data will be stored. If it does not already
exist, it will be created.

**es.idField:** The field that will determine the id for the document. It should match a fieldname
in the structured record of the input.

.. rubric:: Example

::

  {
   "name": "Elasticsearch",
      "properties": {
        "es.host": "localhost:9200",
        "es.index": "megacorp",
        "es.type": "employee",
        "es.idField": "id"
      }
  }

This example connects to Elasticsearch, which is running locally, and writes the data to
the specified index (megacorp) and type (employee). The data is indexed using the id field
in the record. Each run, the documents will be updated if they are still present in the
source.
