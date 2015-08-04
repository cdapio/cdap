.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _apptemplates-etl-index:

============
ETL Overview 
============


A Quick Intro to ETL
====================

Most data infrastructures are front ended with an ETL system (Extract-Transform-Load). The
purpose of the system is to:

- Extract data from one or more sources;
- Transform the data to fit the operational needs; and
- Load the data into a desired destination.

ETL usually comprises a source, zero or more transformations and a sink, in what is called
an *ETL pipeline:*

.. image:: ../_images/etl-pipeline.png
   :width: 6in
   :align: center


.. rubric:: ETL Templates, Adapters and Plugins 

An *Application Template* is a blueprint that is used to create an *Adapter*, an instantiation of
a template in CDAP.

CDAP provides two application templates |---| the ETL templates **ETLBatch** and
**ETLRealtime** |---| which are used to create adapters that perform ETL in either batch
or real time. The  *ETLBatch* and *ETLRealtime* templates consist of a variety of sources,
transformations and sinks that are packaged together.

The batch sources can write to any batch sinks that are available and real-time sources can
write to any real-time sinks. Transformations work with either sinks or sources.

This lists the available sources, sinks and transformations (transforms):

.. list-table::
   :widths: 30 40 30
   :header-rows: 1

   * - Sources
     - Transformations
     - Sinks
   * - - **Batch Sources**

         - Database
         - File
         - KVTable
         - Stream
         - Table
         - TPFSAvro

     - - Transformations

         - LogParser
         - Projection
         - Script
         - ScriptFilter
         - StructuredRecordToGenericRecord

     - - **Batch Sinks**

         - Cube
         - Database
         - KVTable
         - Table
         - TPFSAvro
         - TPFSParquet

   * - - **Real-time Sources**

         - JMS
         - Kafka
         - SQS
         - Test
         - Twitter

     - - Transformations

         - LogParser
         - Projection
         - Script
         - ScriptFilter
         - StructuredRecordToGenericRecord

     - - **Real-time Sinks**

         - Cube
         - Stream
         - Table



.. rubric:: ETL Adapters

An *ETL Adapter* is an instantiation of an ETL template that has been given a specific
configuration on creation.

**Batch adapters** can be scheduled to run periodically using a cron expression and can read
data from batch sources using a MapReduce job. The batch adapter then performs any
optional transformations before writing to a batch sink.

**Real time adapters** are designed to poll sources periodically to fetch the data, perform any
optional transformations, and then write to a real-time sink.

ETL adapters are created by preparing a configuration that specifies the ETL template and
which source, transformations (transforms), and sinks are used to create the adapter. The
configuration can either be written as a JSON file or, in the case of the CDAP UI,
specified in-memory.

.. rubric:: ETL Plugins

The sources, transformations and sinks are generically called plugins. Plugins provide a
way to extend the functionality of existing templates. An adapter can be created with
existing plugins or, if the user wishes, they can write their own source, transform, and
sink plugins to add their own.


Template and Plugin Details
===========================

.. |templates| replace:: **ETL Template and Plugin Details:**
.. _templates: templates/index.html

- |templates|_ Detailed description of available ETL application templates and plugins.

.. |creating| replace:: **Creating An ETL Adapter:**
.. _creating: creating.html

- |creating|_ Steps for creating an ETL adapter from an ETL application template.

.. |operations| replace:: **Adapter Lifecycle Management:**
.. _operations: operations.html

- |operations|_ Lifecycle controls for operating an ETL adapter.

.. |etl-custom| replace:: **Creating Custom ETL Plugins:**
.. _etl-custom: custom.html

- |etl-custom|_ Intended for developers writing custom ETL plugins.

