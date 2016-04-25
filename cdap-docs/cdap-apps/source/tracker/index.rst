.. meta::
    :author: Cask Data, Inc.
    :description: Cask Tracker
    :copyright: Copyright © 2016 Cask Data, Inc.

:hide-toc: true

.. _cdap-apps-tracker-index:

============
Cask Tracker
============   

.. figure:: ../_images/tracker_logo_sm.png
   :figwidth: 100%
   :width: 2in
   :align: left

.. figure:: ../_images/img_trackdiagram.png
   :figwidth: 100%
   :width: 3in
   :align: center


Introduction
============

how it is based off of the metadata system - add plenty of links
 


Cask Tracker ("Tracker") is a self-service CDAP extension that automatically captures
metadata and lets you see how data is flowing into and out of datasets, streams, and
stream views.

It allows you to perform impact and root-cause analysis, and delivers an audit-trail for
auditability and compliance. Tracker furnishes access to structured information that
describes, explains, locates, and makes it easier to retrieve, use, and manage datasets.

Tracker also allows for the storage of metadata where it can be accessed and indexed. This
allows it to be is easily searched and provides consistent, high-quality metadata.


**Harvest, Index, and Track Datasets**

- Immediate, timely, and seamless capture of technical, business, and operational metadata,
  enabling faster and better traceability of all datasets.

- Tracker quickly, reliably, and accurately indexes technical, business, and operational metadata
  to help locate datasets.

- Through its use of lineage, it lets you understand the impact of changing a dataset on
  other datasets, processes or queries.

- Tracks the flow of data across enterprise systems and data lakes.

- Provides trusted and complete metadata on datasets, enabling traceability to resolve
  data issues and to improve data quality.


**Supports Standardization, Governance, and Compliance Needs**

- Provide IT with the traceability needed in governing datasets and in applying compliance
  rules through seamless integration with other extensions.

- Tracker has consistent definitions of metadata-containing information about the data to reconcile
  differences in terminologies.

- It helps you in the understanding of the lineage of your business-critical data.


**Blends Metadata Analytics and Integrations**

- See how your datasets are being created, accessed, and processed with built-in usage
  analytics capabilities.

- Multi-dimensional usage analytics to understand the interaction between users,
  applications, and datasets.

- Extensible integrations are available with enterprise-grade MDM (master data management)
  systems such as `Cloudera Navigator <https://www.cloudera.com/products/cloudera-navigator.html>`__ 
  for centralizing metadata repository and the delivery of complete, accurate, and correct
  data.


Example Use Case
----------------
An example use case describes how Tracker was employed in the `data cleansing and validating of
three billion records <http://customers.cask.co/rs/882-OYR-915/images/tracker-casestudy1.pdf>`__.


Tracker Application
===================
The Cask Tracker application consists of an application in CDAP with two programs and two datasets:

- ``_Tracker`` application: names begins with an underscore
- ``AuditLog`` service: it exposes the Tracker audit log as an API
- ``AuditLogFlow`` flow: subscribes to Kafka audit messages and stores them in the ``AuditLog``	dataset
- ``AuditLog`` custom dataset: type ``co.cask.tracker.entity.AuditLogTable``
- ``kafkaOffset`` dataset: type key-value table

The Tracker UI is shipped with CDAP, started automatically as part of the CDAP UI, and is available at:

  http://localhost:9999/ns/default/tracker/home

Tracker is built from a system artifact included with CDAP::

  tracker-|cask-tracker-version|.jar

  tracker-0.1.0-SNAPSHOT.jar


Installation
============
Cask Tracker is deployed from its system artifact included with CDAP. A CDAP administrator
does not need to build anything to add Cask Tracker to CDAP; they merely need to enable
the application.

This property is required in the ``cdap-site.xml`` for the audit log functionality of Tracker::
  
  <!-- Audit Configuration -->

  <property>
    <name>audit.enabled</name>
    <value>true</value>
    <description>
      Determines whether to publish audit messages to Apache Kafka
    </description>
  </property>

  <property>
    <name>audit.kafka.topic</name>
    <value>audit</value>
    <description>
      Apache Kafka topic name to which audit messages are published
    </description>
  </property>

Enabling Tracker
----------------
To enable Tracker, go to Tracker UI at http://localhost:9999/ns/default/tracker/home and
press the ``"Enable Tracker"`` button to enable Tracker.

Once pressed, the application will be deployed, the datasets created, the flow and service started, and search
and audit logging will become available.

If you are enabling Tracker from outside the UI, you will, in addition to enabling auditing 
in the ``cdap-site.xml`` as described above, need to:

- load the artifact (``tracker-``\ |literal-cask-tracker-version|\ ``.jar``)
  
- create the application based on the artifact, supplying a configuration file::
    
    {
      "config": {
        "auditLogKafkaConfig": {
          "zookeeperString": "<host>:<port>/cdap/kafka"
        }
      }
    }

  substituting for ``<host>`` and ``<port>`` with appropriate values.
  
Restarting CDAP
---------------
As Tracker is an application running inside CDAP, it does not start up automatically when
CDAP is restarted. Each time that you start CDAP, you will need to re-enable Tracker. If you
are using the audit log feature of Tracker, it is important that Tracker be enabled **before**
you begin any other applications, or their activities will not be recorded by Tracker in
its audit log.

If the installation of CDAP is an upgrade from a previous version, all activity and
datasets prior to the enabling of Tracker will not be available or seen in the Tracker UI.

Disabling and Removing Tracker
------------------------------
If for some reason you need to disable or remove Tracker, you would need to:

- stop the Tracker flow
- stop the Tracker service
- delete the Tracker application
- delete the Tracker datasets


Tracker and its UI
==================
overview of what does tracker provide? - how to navigate the UI, what is where etc

Search
------
Searching in Tracker is provided by an interface similar to that of a popular search engine:

.. figure:: ../_images/tracker-home-search.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

In the text box, you enter your search terms:

- Multiple search terms can be searched by separating them with a space character.
- Search terms are case-insensitive.
- Search the metadata of entities by using either a complete or partial name followed by
  an asterisk ``*``, as described in the :ref:`Metadata HTTP RESTful API
  <metadata_query_terms>`.
- Tracker searches tags, properties, and schema of CDAP datasets, streams, and stream views.

For example, if you have just started CDAP and enabled Tracker, you could enter a search
term such as ``a* k*``, which will find all entities that begin with the letter ``a`` or
``k``.

The results would appear similar to this:

.. figure:: ../_images/tracker-first-search.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

In this example, Tracker has found two datasets that satisfy the condition. The search
used is shown in the upper-left, and the results show both the datasets found with
information and links for each.

**On the left side** is the **Filter** pane, which provides information on what was found (the
entities and metadata types) with statistics of the number found for each category. A blue
checkbox allows you to filter based on these attributes. If you mouse over a category, an
``only`` link will appear, which allows you to select *only* that category as a filter.

Note that the *entities* and *metadata* filters have an ``and`` relationship; at least one
selection must be made in each of *entities* and *metadata* for there to be any results
that appear.

**On the right side** is a sortable list of results. It is sortable by *Create Date* or the entity
ID (name), either *A-Z* (alphabetical ascending), or *Z-A* (alphabetical descending).

Each entry in the list provides a summery of information about the entity, and its name is
a hyperlink to further details: metadata, lineage, and audit log.

The **Jump** button provides three actions: go to the selected entity in CDAP, or add it
to a new Cask Hydrator pipeline as a source or as a sink. Datasets can be added as sources or
sinks to batch pipelines, while streams can be sources in batch pipelines or sinks in
real-time pipelines.






Tracker HTTP RESTful API
========================
APIs
audit logs - TBD
metadata APIs 
search APIs?


Tracker supports searching of the *AuditLog* dataset through an HTTP RESTful
API. To search for any audit log entries for a particular dataset, stream, or stream view,
submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/apps/_Tracker/services/AuditLog/methods/auditlog/<entity-type>/<name>
    [?startTime=<time>][&endTime=<time>][&offset=<offset>][&limit=<limit>]

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<entity-type>``
     - One of ``dataset``, ``stream``, or ``stream_view``
   * - ``<name>``
     - Name of the ``<entity-type>``
   * - ``<time>`` *(Optional)*
     - Time range defined by start (*startTime*, default ``0``) and end (*endTime*, default ``now``) times, where the times are either in
       milliseconds since the start of the Epoch, or a relative time, using ``now`` and times added to it.
       You can apply simple math, using ``now`` for the current time, ``s`` for seconds, ``m``
       for minutes, ``h`` for hours and ``d`` for days. For example: ``now-5d-12h`` is 5 days
       and 12 hours ago.
   * - ``<offset>`` *(Optional)*
     - The offset to start the results at for paging; default is ``0``.
   * - ``<limit>`` *(Optional)*
     - The maximum number of results to return in the results; default is ``10``.
     
The results (if any)



Example::

  curl -w'\n' -X GET 'http://localhost:10000/v3/namespaces/default/apps/_Tracker/services/AuditLog/methods/auditlog/stream/who?limit=1&startTime=now-5d-12h&endTime=now-12h'


Results (reformatted for display)::

  {
    "totalResults": 5,
    "results": [
      {
        "version": 1,
        "time": 1461266805472,
        "entityId": {
          "namespace": "default",
          "stream": "who",
          "entity": "STREAM"
        },
        "user": "unknown",
        "type": "METADATA_CHANGE",
        "payload": {
          "previous": {
            "SYSTEM": {
              "properties": {
                "creation-time": "1461266804916",
                "ttl": "9223372036854775807"
              },
              "tags": [
                "who"
              ]
            }
          },
          "additions": {
            "SYSTEM": {
              "properties": {
                "schema": "{\"type\":\"record\",\"name\":\"stringBody\",\"fields\":[{\"name\":\"body\",\"type\":\"string\"}]}"
              },
              "tags": []
            }
          },
          "deletions": {
            "SYSTEM": {
              "properties": {},
              "tags": []
            }
          }
        }
      },
      
      . . .
      
      {
        "version": 1,
        "time": 1461266805404,
        "entityId": {
          "namespace": "default",
          "stream": "who",
          "entity": "STREAM"
        },
        "user": "unknown",
        "type": "CREATE",
        "payload": {}
      }
    ],
    "offset": 0
  }


.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Returns the audit log entries requested in the body of the response.
   * - ``400 BAD REQUEST``
     - Returned if the input values are invalid, such as an incorrect date format, negative
       offsets or limits, or an invalid range. The response will include an appropriate error
       message.
   * - ``404 NOT FOUND``
     - No entities matching the specified query were found.
   * - ``500 SERVER ERROR``
     - Unknown server error.

/auditmetrics/topEntities?limit={limit}


