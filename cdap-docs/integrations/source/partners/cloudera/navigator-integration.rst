.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _navigator-integration:

=================================
Navigator Integration Application
=================================


Introduction
============

The Navigator Integration App is an application built by the team at Cask for bridging CDAP Metadata
with Cloudera's data management tool, Navigator. The Navigator Integration App is a CDAP-native application
that uses a real-time flow to fetch the CDAP Metadata and write it to Cloudera Navigator.

Resources
---------
- :ref:`Overview of CDAP Metadata <metadata-lineage>`
- `Cloudera Navigator <http://www.cloudera.com/products/cloudera-navigator.html>`__
- :ref:`Real-time processing using Flows <flows-flowlets-index>`
- :cdap-kafka-flow:`Kafka Flowlet Library <>`
- :ref:`Overview of Audit Logging in CDAP <audit-logging>`


Getting Started
===============

Prerequisites
-------------
To use the Navigator Integration App, you need CDAP version 3.3.0 (or higher) and Navigator version 2.4.0 (or higher).

Metadata Publishing to Kafka
----------------------------
The Navigator Integration App contains a flow that subscribes to the Kafka topic to which the CDAP audit logs are
published. Hence, before using this application, you should
:ref:`enable audit publishing <audit-logging-configuring-audit-publishing>`.


Deploying the Application
=========================
**Step 1:** Start by deploying the artifact JAR (download the :navigator-jar:`released JAR from Maven <>`, version |navigator-version|).
Deploy the JAR using the CDAP CLI:

  .. container:: highlight

    .. parsed-literal::
      |$| cdap cli load artifact target/navigator-|navigator-version|-jar


**Step 2:** Create an application configuration file that contains:

- Kafka Metadata Config (``metadataKafkaConfig``): Kafka Consumer Flowlet configuration information
  (information about where we can fetch metadata updates)
- Navigator Config (``navigatorConfig``): Information required by the Navigator Client to publish data to Navigator

A sample Application Configuration file::

  {
    "config": {
      "metadataKafkaConfig": {
        "zookeeperString": "hostname:2181/cdap/kafka"
      },
      "navigatorConfig": {
        "navigatorHostName": "navigatormetadataserver",
        "username": "abcd",
        "password": "1234"
      }
    }
  }

**Metadata Kafka Config**

This key contains a property map with these properties:

  Required Properties:

  - ``zookeeperString``: Kafka ZooKeeper string that can be used to subscribe to the CDAP metadata updates
  - ``brokerString``: Kafka Broker string to which CDAP metadata is published

  *Note:* Specify either the ``zookeeperString`` or the ``brokerString``.

  Optional Properties:

  - ``topic``: Kafka Topic to which CDAP metadata updates are published; default is ``cdap-metadata-updates`` which
    corresponds to the default topic used in CDAP for metadata updates
  - ``numPartitions``: Number of Kafka partitions; default is set to ``10``
  - ``offsetDataset``: Name of the dataset where Kafka offsets are stored; default is ``kafkaOffset``

**Navigator Config**

This key contains a property map with these properties:

  Required Properties:

  - ``navigatorHostName``: Navigator Metadata Server hostname
  - ``username``: Navigator Metadata Server username
  - ``password``: Navigator Metadata Server password

  Optional Properties:

  - ``navigatorPort``: Navigator Metadata Server port; default is ``7187``
  - ``autocommit``: Navigator SDK's autocommit property; default is ``true``
  - ``namespace``: Navigator namespace; default is ``CDAP``
  - ``applicationURL``: Navigator Application URL; default is ``http://navigatorHostName``
  - ``fileFormat``: Navigator File Format; default is ``JSON``
  - ``navigatorURL``: Navigator URL; default is ``http://navigatorHostName:navigatorPort/api/v8``
  - ``metadataParentURI``: Navigator Metadata Parent URI; default is ``http://navigatorHostName:navigatorPort/api/v8/metadata/plugin``

**Step 3:** Create a CDAP Application by providing the configuration file:

  .. container:: highlight

    .. parsed-literal::
      |$| cdap cli create app metaApp navigator |navigator-version| USER appconfig.txt


Starting the Application
========================
To start the MetadataFlow:

  .. container:: highlight

    .. parsed-literal::
      |$| cdap cli start flow metaApp.MetadataFlow

You should now be able to view CDAP Metadata in the Cloudera Navigator UI. Note that all CDAP Entities use ``SDK`` as
the SourceType and use ``CDAP`` as the namespace (this can be modified). Since the Navigator SDK doesn't allow the adding
of new EntityTypes, we have used this mapping between CDAP and Navigator EntityTypes:

+-------------------+-----------------------+
| CDAP EntityType   | Navigator EntityType  |
+===================+=======================+
| Application       | File                  |
+-------------------+-----------------------+
| Artifact          | File                  |
+-------------------+-----------------------+
| Dataset           | Dataset               |
+-------------------+-----------------------+
| Program           | Operation             |
+-------------------+-----------------------+
| Stream            | Dataset               |
+-------------------+-----------------------+
| StreamView        | Table                 |
+-------------------+-----------------------+
