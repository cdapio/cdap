.. meta::
    :author: Cask Data, Inc.
    :description: Cask Tracker
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _cask-tracker-index:

============
Cask Tracker
============   

Introduction
============

Cask Tracker ("Tracker") is a self-service CDAP extension that automatically captures
:ref:`metadata <metadata-lineage>` and lets you see how data is flowing into and out 
of datasets, streams, and stream views.

It allows you to perform impact and root-cause analysis, delivers an audit-trail for
auditability and compliance, and allows you to preview data. Tracker furnishes access to
structured information that describes, explains, and locates data, making it easier to
retrieve, use, and manage datasets.

Tracker also allows users to update metadata for datasets and streams. Users can add,
remove, and update tags and user properties directly in the UI. It allows users to set
a preferred dictionary of terms so that teams can use the same lexicon when updating metadata.

Tracker's UI shows a graphical visualization of the :ref:`lineage
<metadata-lineage-lineage>` of an entity. A lineage shows |---| for a specified time range
|---| all data access of the entity, and details of where that access originated from.

Tracker also captures activity metrics for datasets. You can see the datasets that are
being used the most and view usage metrics for each dataset. This allows teams to easily
determine the appropriate dataset to use for an analysis. The Tracker Meter rates each
dataset on a scale that shows how active a dataset is in the system. Users can see the
datasets that are being used the most and view usage metrics for each dataset. This
allows teams to easily find the right dataset to use for analysis. The Tracker Meter
also rates each dataset on a scale to quickly show you how active a dataset is in the
system.

**Harvest, Index, Track, and Analyze Datasets**

- Immediate, timely, and seamless capture of technical, business, and operational metadata,
  enabling faster and better traceability of all datasets.

- Through its use of lineage, it lets you understand the impact of changing a dataset on
  other datasets, processes or queries.

- Tracks the flow of data across enterprise systems and data lakes.

- Provides viewing and updating complete metadata on datasets, enabling traceability to resolve
  data issues and to improve data quality.

- Collects usage metrics about datasets so that you know which datasets are being used most often.

- Provides the ability to designate certain tags as "preferred" so that teams can easily find and tag datasets.

- Allows users to preview data directly in the UI.

**Supports Standardization, Governance, and Compliance Needs**

- Provide IT with the traceability needed in governing datasets and in applying compliance
  rules through seamless integration with other extensions.

- Tracker has consistent definitions of metadata-containing information about the data to
  reconcile differences in terminologies.

- It helps you in the understanding of the lineage of your business-critical data.

**Blends Metadata Analytics and Integrations**

- See how your datasets are being created, accessed, and processed.

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
Cask Tracker consists of an application in CDAP with two programs and four datasets:

- ``_Tracker`` application: names begins with an underscore
- ``TrackerService``: Service exposing the Tracker API endpoints
- ``AuditLogFlow``: Flow that subscribes to Kafka audit messages and stores them in the
  ``_auditLog`` dataset
- ``_auditLog``: Custom dataset for storing audit messages
- ``_kafkaOffset``: Key-value table for storing Kafka offsets
- ``_auditMetrics``: Custom cube dataset for collecting dataset metrics
- ``_auditTagsTable``: Custom dataset for storing preferred tags
- ``_timeSinceTable``: Custom dataset for storing the last time a specific audit
  message was received

The Tracker UI is shipped with CDAP, started automatically in standalone CDAP as part of the
CDAP UI. It is available at:

  http://localhost:11011/ns/default/tracker/home
  
or (Distributed CDAP):

  http://host:dashboard-bind-port/ns/default/tracker/home
  

Tracker is built from a system artifact included with CDAP, |literal-cask-tracker-version-jar|.

.. highlight:: xml  

Installation
============
Cask Tracker is deployed from its system artifact included with CDAP. A CDAP administrator
does not need to build anything to add Cask Tracker to CDAP; they merely need to enable
the application after starting CDAP.

These two properties are used by Tracker for its audit log functionality::
  
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

As these are the default settings for these properties, they do not need to be included in the
``cdap-site.xml`` file.

Enabling Tracker
----------------
Tracker is enabled automatically in Standalone CDAP and the UI is available at
http://localhost:9999/ns/default/tracker/home. In the Distributed version of CDAP,
you must manually enable Tracker by visiting
http://host:dashboard-bind-port/ns/default/tracker/home and pressing the
``"Enable Tracker"`` button.

Once pressed, the application will be deployed, the datasets created (if necessary), the
flow and service started, and search and audit logging will become available.

If you are enabling Tracker from outside the UI, you will, in addition to enabling auditing 
in the ``cdap-site.xml`` as described above, need to follow these steps:

- Using the CDAP CLI, load the artifact (|literal-cask-tracker-version-jar|):

  .. container:: highlight

    .. parsed-literal::

      |cdap >| load artifact target/|cask-tracker-version-jar|

.. highlight:: json  

- Create an application configuration file (``appconfig.txt``) that contains the Kafka
  Audit Log reader configuration (the property ``auditLogKafkaConfig``). It is the Kafka
  Consumer Flowlet configuration information. For example::
    
    {
      "config": {
        "auditLogKafkaConfig": {
          "zookeeperString": "<host>:<port>/cdap/kafka"
        }
      }
    }

  substituting for ``<host>`` and ``<port>`` with appropriate values.
  
- Create a CDAP application using the configuration file:

  .. container:: highlight

    .. parsed-literal::

      |cdap >| create app TrackerApp tracker |cask-tracker-version| USER appconfig.txt

**Audit Log Kafka Config:**

This key contains a property map with:

- Required Properties:

  - ``zookeeperString``: Kafka Zookeeper string that can be used to subscribe to the CDAP
    audit log updates
  - ``brokerString``: Kafka Broker string to which CDAP audit log data is published

  *Note:* Specify either the ``zookeeperString`` or the ``brokerString``.

- Optional Properties:

  - ``topic``: Kafka Topic to which CDAP audit updates are published; default is ``audit``
    which corresponds to the default topic used in CDAP for audit log updates
  - ``numPartitions``: Number of Kafka partitions; default is set to ``10``
  - ``offsetDataset``: Name of the dataset where Kafka offsets are stored; default is
    ``_kafkaOffset``

Restarting CDAP
---------------
As Tracker is an application running inside CDAP, it does not start up automatically when
CDAP is restarted. Each time that you start CDAP, you will need to re-enable Tracker.
Re-enabling Tracker does not recreate the datasets; instead, the same datasets as were
used in previous runs are used.

If you are using the audit log feature of Tracker, it is best that Tracker be enabled
**before** you begin any other applications.

If the installation of CDAP is an upgrade from a previous version, all activity and
datasets prior to the enabling of Tracker will not be available or seen in the Tracker UI.

Disabling and Removing Tracker
------------------------------
If for some reason you need to disable or remove Tracker, you would need to:

- stop all ``_Tracker`` programs
- delete the Tracker application
- delete the Tracker datasets

Tracker and its UI
==================

Search
------
Searching in Tracker is provided by an interface similar to that of a popular search engine:

.. figure:: /_images/tracker-home-search.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

In the text box, you enter your search terms:

- Multiple search terms can be searched by separating them with a space character.
- Search terms are case-insensitive.
- Search the metadata of entities by using either a complete or partial name followed by
  an asterisk ``*``, as described in the :ref:`Metadata HTTP RESTful API
  <http-restful-api-metadata-query-terms>`.
- Tracker searches tags, properties, and schema of CDAP datasets, streams, and stream views.

For example, if you have just started CDAP and enabled Tracker, you could enter a search
term such as ``a* k*``, which will find all entities that begin with the letter ``a`` or
``k``.

The results would appear similar to this:

.. figure:: /_images/tracker-first-search.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

In this example, Tracker has found two datasets that satisfied the condition. The search
used is shown in the upper-left, and the results show the datasets found with
information and links for each.

**On the left side** is the **Filter** pane, which provides information on what was found (the
entities and metadata types) with statistics of the number found for each category. A blue
checkbox allows you to filter based on these attributes. If you mouse over a category, an
``only`` link will appear, which allows you to select *only* that category as a filter.

Note that the *entities* and *metadata* filters have an ``and`` relationship; at least one
selection must be made in each of *entities* and *metadata* for there to be any results
that appear.

**On the right side** is a sortable list of results. It is sortable by one of *Create Date*, the entity
ID (name), or the Tracker Score.

Each entry in the list provides a summery of information about the entity, and its name is
a hyperlink to further details: metadata, lineage, and audit log.

The **Jump** button provides three actions: go to the selected entity in CDAP, or add it
to a new Cask Hydrator pipeline as a source or as a sink. Datasets can be added as sources or
sinks to batch pipelines, while streams can be sources in batch pipelines or sinks in
real-time pipelines.

Entity Details
--------------
Clicking on a name in the search results list will take you to details for a particular
entity. Details are provided on the tabs *Metadata*, *Lineage*, *Audit Log*, *Preview*
(included if the dataset is explorable), and *Usage*.

**Metadata**

The *Metadata* tab provides lists of the *System Tags*, *User Tags*, *Schema*, *User
Properties*, and *System Properties* that were found for the entity. The values shown will
vary depending on the type of entity and each individual entity. For instance, a stream
may have a schema attached, and if so, it will be displayed.

.. figure:: /_images/tracker-metadata.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

You can add user tags to any entity by clicking the plus button in the UI. You can also
remove tags by hovering over the tag and clicking the x. You can also add and remove User
Properties for the dataset or stream. This is useful for storing additional details about
the dataset for others to see.

**Lineage**

The *Lineage* tab shows the relationship between an entity and the programs that are
interacting with it. As different lineage diagrams can be created for the same entity,
depending on the particular set of programs selected to construct the diagram, a green
button in the shape of an arrow is used to cycle through the different lineage digrams
that a particular entity participates in.

A date menu in the left side of the diagram lets you control the time range that the
diagram displays. By default, the last seven days are used, though a custom range can be
specified, in addition to common time ranges (two weeks to one year).

.. figure:: /_images/tracker-lineage.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

**Audit Log**

The *Audit Log* tab shows each record in the *_auditLog* dataset that has been created for
that particular entity, displayed in reverse chronological order. Because of how datasets
work in CDAP, reading and writing from a flow or service to a dataset shows an access of
"UNKNOWN" rather than indicating if it was read or write access. This will be addressed in
a future release.

A date menu in the left side of the diagram lets you control the time range that the
diagram displays. By default, the last seven days are used, though a custom range can be
specified, in addition to common time ranges (two weeks to one year).

.. figure:: /_images/tracker-audit-log.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

**Preview**

The *Preview* tab (if available) shows a preview for the dataset. It is available for all datasets that are
explorable. You can scroll for up to 500 records. For additional analysis, use the *Jump*
menu to go into CDAP and explore the dataset using a custom query.

.. figure:: /_images/tracker-preview.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

**Usage**

The *Usage* tab shows a set of graphs displaying usage metrics for the dataset. At the top is a
histogram of all audit messages for a particular dataset. Along the bottom of the screen is a set of
charts displaying the Applications and Programs that are accessing the dataset, and a table showing
the last time a specific message was received about the dataset. Clicking the Application name or
the Program name will take you to that entity in the main CDAP UI.

.. figure:: /_images/tracker-usage.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

**Preferred Tags**

The *Tags* tab at the top of the page allows you to enter a common set of preferred terms to use when
adding tags to datasets. Preferred tags show up first when adding tags, and will guide your team to
use the same terminology. Any preferred tag that has not been attached to any entities can be deleted
by clicking the red trashcan icon. If a preferred tag has been added to an entity, you cannot delete it,
but you can demote it back to just being a user tag.

.. figure:: /_images/tracker-tags.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

To add preferred tags, click the **Add Preferred Tags** button and use the UI to add or import a
list of tags that you would like to be "preferred". If the tag already exists in CDAP,
it will be promoted from being a user tag to being a preferred tag. If it is a new tag
in CDAP, it will be added in the *Preferred Tags* list.

.. figure:: /_images/tracker-tags-upload.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Integrations
------------

Tracker allows for an easy integration with `Cloudera Navigator
<https://www.cloudera.com/products/cloudera-navigator.html>`__  by providing a UI to
connecting to a Navigator instance:

.. figure:: /_images/tracker-integration-configuration.png
  :figwidth: 100%
  :width: 800px
  :align: center
  :class: bordered-image

Details on completing this form are described in CDAP's documentation on the
:ref:`Navigator Integration Application <navigator-integration>`.

