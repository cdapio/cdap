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
associated with datasets. This gives developers and data scientists flexibility when
innovating and building solutions on Hadoop, without the worry of maintaining compliance
and governance for every application.

.. _metadata-lineage-metadata:

Metadata
========
Metadata |---| consisting of **properties** (a list of key-value pairs) or **tags** (a
list of keys) |---| can be used to annotate artifacts, applications, programs, datasets,
streams, and views.

Using the CDAP :ref:`Metadata HTTP RESTful API <http-restful-api-metadata>`, you can set,
retrieve, and delete these metadata annotations.

Metadata keys, values, and tags must conform to the CDAP :ref:`supported characters 
<supported-characters>`, and are limited to 50 characters in length. The entire metadata
object associated with a single entity is limited to 10K bytes in size.

Discovery
=========
Metadata can be used to tag different CDAP components so that they are easily identifiable
and managed. This helps in discovering CDAP components.

For example, you can tag a dataset as *experimental* or an application as *production*. These
entities can then be discovered by using the annotated metadata in **search queries**. Using search,
users can discover entities:

- that have a particular **value** for *any key* in their properties;
- that have a particular **key** with a particular *value* in their properties; or
- that have a particular **tag**.

.. _metadata-lineage-system-metadata:

System Metadata
===============
While CDAP allows users to tag entities with metadata properties and tags, it also
tags entities with system properties and tags by default. These default properties and tags can be retrieved
using the CDAP :ref:`Metadata HTTP RESTful API <http-restful-api-metadata>` by setting the
``scope`` query parameter to *system*. These default annotations can be used to discover CDAP entities using the
Metadata Search API. 

This table lists the **system** metadata annotations of CDAP entities:

.. list-table::
   :widths: 20 40 40
   :header-rows: 1

   * - Entity
     - System Properties
     - System Tags
   * - Artifacts
     - * Plugins (name and version)
     - * Artifact name
   * - Applications
     - * Programs (name and type) contained in the application
       * Plugins 
       * Schedules (name and description)
     - * Application name
       * Artifact name
   * - Programs
     - * n/a
     - * Program name and type 
       * Program mode (*batch* or *realtime*)
       * Workflow node names (for workflows only)
   * - Datasets
     - * Schema (field names and types)
       * Dataset type (*FileSet, Table, KeyValueTable,* etc.)
       * TTL (Time To Live)
     - * Dataset name
       * *batch* for Datasets accessible through Map Reduce or Spark programs
       * *explore* for Datasets that can be queried through Explore interface
   * - Streams
     - * Schema (field names and types)
       * TTL
     - * Stream name
   * - Stream Views
     - * Schema (field names and types)
     - * View name

.. _metadata-navigator-integration:

Cloudera Navigator Integration
==============================
CDAP Metadata can be pushed to Cloudera Navigator for metadata discovery and search. 
Refer to :ref:`Cloudera Navigator Integration <navigator-integration>` for more information.


.. _metadata-lineage-lineage:

Lineage
=======
**Lineage** can be retrieved for dataset and stream entities. A lineage shows
|---| for a specified time range |---| all data access of the entity, and details of where
that access originated from.

For example: with a stream, writing to a stream can take place from a worker, which may
have obtained the data from a combination of a dataset and a (different) stream. The data
in those entities can come from (possibly) other entities. The number of levels of the
lineage that are calculated is set when a request is made to view the lineage of a
particular entity.

In the case of streams, the lineage includes whether the access was reading or writing to
the stream. 

In the case of datasets, lineage can indicate if a dataset access was for reading,
writing, or both, if the methods in the dataset have appropriate :ref:`annotations
<custom-datasets-access-annotations>`. If annotations are absent, lineage can only
indicate that a dataset access took place, and does not provide indication if that access
was for reading or writing.
