.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

===============
Monitoring CDAP
===============

Logs and Metrics
================
CDAP collects logs and metrics for all of its internal services. Being able to view these
details can be really helpful in debugging CDAP applications as well as analyzing their
performance. CDAP gives access to its logs, metrics, and other monitoring information
through RESTful APIs as well as a Java Client.

See the :ref:`Logging <http-restful-api-logging>`, :ref:`Metrics <http-restful-api-metrics>`,
and :ref:`Monitoring <http-restful-api-monitor>` HTTP APIs, and the
:ref:`Java Client <reference:java-client-api>` for more information.


Monitoring Utilities
====================
CDAP can be monitored using external systems such as `Nagios <https://www.nagios.org/>`__; a Nagios-style plugin 
`is available <https://github.com/caskdata/cdap-monitoring-tools/blob/develop/nagios/README.rst>`__
for checking the status of CDAP applications, programs, and the CDAP instance itself.


.. _monitoring-metadata-update-notifications:

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
  