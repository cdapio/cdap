.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _navigator-integration:

=================================
Navigator Integration Application
=================================

Introduction
============

The Cask™ Data Application Platform (CDAP) is an integrated, open source application
development platform for the Hadoop ecosystem that provides developers with data and
application abstractions to simplify and accelerate application development.

The Navigator Integration App is one such application built by the team at Cask for bridging CDAP Metadata
with Cloudera's data management tool, Navigator. The Navigator Integration App is a CDAP-native application
that uses a real-time flow to fetch the CDAP Metadata and write it to Cloudera Navigator.

Resources
---------
- `Overview of CDAP Metadata
  <http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/metadata-lineage.html#metadata>`__
- `Cloudera Navigator <http://www.cloudera.com/products/cloudera-navigator.html>`__
- `Real-time processing using Flows
  <http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/flows-flowlets/index.html>`__
- `Kafka Flowlet Library
  <https://github.com/caskdata/cdap-packs/tree/develop/cdap-kafka-pack/cdap-kafka-flow>`__


Getting Started
===============

Prerequisites
-------------
To use the Navigator Integration App, you need CDAP version 3.3.0 (or higher) and Navigator version 2.4.0 (or higher).

Metadata Publishing to Kafka
----------------------------
The Navigator Integration App contains a flow that subscribes to the Kafka topic to which the CDAP Metadata service publishes
the metadata updates. Hence, before using this application, you should enable publishing of metadata updates to
Kafka, as described in the CDAP document `Enable Metadata Update Notifications
<http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/metadata-lineage.html#metadata-update-notifications>`__.


Deploying the Navigator Integration App
---------------------------------------
**Step 1:** Start by deploying the artifact JAR (download the `released JAR from Maven
<http://search.maven.org/remotecontent?filepath=co/cask/cdap/metadata/navigator/0.1.0/navigator-0.1.0.jar>`__).
Deploy the JAR using the CDAP CLI::

  > load artifact target/navigator-<version>-jar

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

**Metadata Kafka Config:**

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

**Navigator Config:**

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

**Step 3:** Create a CDAP Application by providing the configuration file::

  > create app metaApp navigator 0.1.0 USER appconfig.txt

Starting the Navigator Integration App
--------------------------------------
To start the MetadataFlow::

  > start flow metaApp.MetadataFlow

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

Mailing Lists
-------------
CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

IRC Channel
-----------
CDAP IRC Channel: #cdap on irc.freenode.net


License and Trademarks
======================

Copyright © 2016 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.

Cloudera Navigator is a trademark of Cloudera.
