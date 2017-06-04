.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.

.. _metadata-system-metadata:

===============
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
