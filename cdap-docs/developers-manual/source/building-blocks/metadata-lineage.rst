.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _metadata-lineage:

====================
Metadata and Lineage
====================

Overview
========
Metadata and Lineage are a new and important feature of CDAP. CDAP Metadata helps show how
datasets and programs are related to each other and helps in understanding the impact of a
change before the change is made. 

This feature provides full visibility into the impact of changes while providing an audit
trail of access to datasets by programs and applications. It gives a clear view when
identifying trusted data sources and enables the ability to track the trail of sensitive
data.

CDAP captures metadata from many different sources |---| as well as those specified by a
user |---| on different entities and objects. The container model of CDAP provides for the
seamless aggregation of a wide variety of machine-generated metadata that is automatically
associated with datasets. This capability allows flexibility for the developers and data
scientist to innovate and build solutions on Hadoop without any responsibility for
compliance and governance.


.. _metadata-lineage-metadata:

Metadata
========
Metadata |---| consisting of **properties** (a list of key-value pairs) or **tags** (a
list of keys) |---| can be used to annotate datasets, streams, programs, and applications.

Using the CDAP :ref:`Metadata HTTP RESTful API <http-restful-api-metadata>`, you can set,
retrieve, and delete the metadata annotations of applications, datasets, streams, and
programs in CDAP.

Metadata keys, values, and tags must conform to the CDAP :ref:`supported characters 
<supported-characters>`, and are limited to 50 characters in length. The entire metadata
object associated with a single entity is limited to 10K bytes in size.

Metadata can be used to tag different CDAP components so that they are easily identifiable
and managed. You can tag a datasets as *experimental* or an application as *production*.

Metadata can be **searched**, either to find entities:

- that have a particular **value** for *any key* in their properties;
- that have a particular **key** with a particular *value* in their properties; or
- that have a particular **tag**.


.. _metadata-update-notifications:

Metadata Update Notifications
=============================
CDAP has the capability of publishing notifications to an external Apache Kafka instance
upon metadata updates.

This capability is controlled by these properties set in the ``cdap-site.xml``, as described in the
:ref:`Administration Manual <appendix-cdap-site.xml>`:

- ``metadata.updates.publish.enabled``: Determines if publishing of updates is enabled; defaults to ``false``;
- ``metadata.updates.kafka.broker.list``: The Kafka broker list to publish to; and
- ``metadata.updates.kafka.topic``: The Kafka topic to publish to; defaults to ``cdap-metadata-updates``.

If ``metadata.updates.publish.enabled`` is *true*, then ``metadata.updates.kafka.broker.list`` **must** be defined.

When enabled, upon every property or tag update, CDAP will publish a notification message
to the configured Kafka instance. The contents of the message are a JSON representation of
the `MetadataChangeRecord 
<https://github.com/caskdata/cdap/blob/develop/cdap-proto/src/main/java/co/cask/cdap/proto/metadata/MetadataChangeRecord.java>`__ 
class.

Here is an example JSON message, pretty-printed::

  {
     "previous":{
        "entityId":{
           "type":"application",
           "id":{
              "namespace":{
                 "id":"default"
              },
              "applicationId":"PurchaseHistory"
           }
        },
        "scope":"USER",
        "properties":{
           "key2":"value2",
           "key1":"value1"
        },
        "tags":[
           "tag1",
           "tag2"
        ]
     },
     "changes":{
        "additions":{
           "entityId":{
              "type":"application",
              "id":{
                 "namespace":{
                    "id":"default"
                 },
                 "applicationId":"PurchaseHistory"
              }
           },
           "scope":"USER",
           "properties":{

           },
           "tags":[
              "tag3"
           ]
        },
        "deletions":{
           "entityId":{
              "type":"application",
              "id":{
                 "namespace":{
                    "id":"default"
                 },
                 "applicationId":"PurchaseHistory"
              }
           },
           "scope":"USER",
           "properties":{

           },
           "tags":[

           ]
        }
     },
     "updateTime":1442883836781
  }


.. _metadata-lineage-lineage:

Lineage
=======
**Lineage** can be retrieved for dataset and stream entities. A lineage shows
|---| for a specified time range |---| all data access of the entity, and details of where
that access originated from.

For example: with a stream, writing to a stream may take place from a worker, which
obtained the data from a combination of a dataset and a stream. The data in those entities
comes from possibly other entities. The number of levels of the lineage that are
calculated is set when a request is made to view the lineage of a particular entity.

In the case of streams, the lineage includes whether the access was reading or writing to
the stream. In the case of datasets, in this CDAP version, lineage can only indicate that
dataset access took place, and does not provide indication if that access was for reading
or writing. Later versions of CDAP will address this limitation.
