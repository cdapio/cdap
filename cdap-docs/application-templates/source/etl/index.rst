.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _apptemplates-etl-index:

===================
ETL Overview 
===================


A Quick Intro to ETL
====================

Most data infrastructures are front ended with an ETL system (Extract-Transform-Load). The
purpose of the system is to:

- Extract data from one or more sources;
- Transform the data to fit the operational needs; and
- Load the data into a desired destination.

ETL usually comprises a source, zero or more transformations and a sink, in what is called
an ETL Pipeline:

.. image:: ../_images/etl-pipeline.png
   :width: 6in
   :align: center


ETL Templates, Adapters and Plugins 
-----------------------------------

An *Application Template* is a blueprint that is used to create an Adapter, an instantiation of
a template in CDAP.

CDAP provides two Application Templates |---| the ETL Templates **ETLBatch** and
**ETLRealtime** |---| which are used to create Adapters that perform ETL in either batch
and realtime. The  *ETLBatch* and *ETLRealtime* templates consist of a variety of sources,
transformations and sinks that are packaged together.

The batch sources can write to any batch sinks that are available and realtime sources can
write to any realtime sinks. Transformations work with either sinks or sources.

This lists the available sources, sinks and transformations (transforms):

.. list-table::
   :widths: 30 40 30
   :header-rows: 1

   * - Sources
     - Transformations
     - Sinks
   * - - **Batch Sources:**

         - Database
         - KVTable
         - Stream
         - Table

     -   
         - Projection
         - ScriptFilter
         - Script
         - StructuredRecordToGenericRecord

     - - **Batch Sinks:**

         - Cube
         - Database
         - KVTable
         - TPFSAvro
         - Table

   * - - **Realtime Sources:**

         - JMS
         - Kafka
         - Twitter
         - Test

     -   
         - Projection
         - ScriptFilter
         - Script
         - StructuredRecordToGenericRecord

     - - **Realtime Sinks:**

         - Cube
         - Stream
         - Table



ETL Adapters
............
An *ETL Adapter* is an instantiation of an ETL Template that has been given a specific
configuration on creation.

**Batch adapters** can be scheduled to run periodically using a cron expression and can read
data from batch sources using a Mapreduce job. The batch adapter then performs any
optional transformations before writing to a batch sink.

**Realtime adapters** are designed to poll sources periodically to fetch the data, perform any
optional transformations, and then write to a realtime sink.

ETL Adapters are created by preparing a configuration that specifies the ETL template and
which source, transformations (transforms), and sinks are used to create the adapter. The
configuration can either be written as a JSON file or, in the case of the CDAP UI,
specified in-memory.

ETL Plugins
...........
The sources, transformations and sinks are generically called plugins. Plugins provide a
way to extend the functionality of existing templates. An adapter can be created with
existing plugins or, if the user wishes, they can write their own source, transform, and
sink plugins to add their own.


Template and Plugin Details
===========================

.. |templates| replace:: **ETL Template and Plugin Details:**
.. _templates: templates.html

- |templates|_ Detailed description of available ETL Application Template and Plugins.

.. |creating| replace:: **Creating An ETL Adapter:**
.. _creating: creating.html

- |creating|_ Steps for creating an ETL Adapter from an ETL Application Template.

.. |operations| replace:: **Operating An ETL Adapter:**
.. _operations: operations.html

- |operations|_ Lifecycle controls for operating an ETL Adapter.

.. |etl-custom| replace:: **Creating Custom ETL Plugins:**
.. _etl-custom: custom.html

- |etl-custom|_ Intended for developers writing custom ETL Plugins.

