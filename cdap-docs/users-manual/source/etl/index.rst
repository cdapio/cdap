.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _users-etl-index:

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
an ETL Pipeline:

.. image:: ../_images/etl-pipeline.png
   :width: 6in
   :align: center


ETL Templates, Adapters and Plugins 
-----------------------------------

An Application Template is a blueprint that is used to create an Adapter, an instance of
a template in CDAP.

CDAP provides two Application Templates |---| the ETL Templates **etlBatch** and
**etlRealtime** |---| which are used to create Adapters that perform ETL in either batch
and realtime. The  *etlBatch* and *etlRealtime* templates consist of a variety of sources,
transformations and sinks that are packaged together.

The batch sources can write to any batch sinks that are available and realtime sources can
write to any realtime sinks. Transformations work with either sinks or sources.

This matrix depicts the list of available sources, transformations, and sinks:

.. list-table::
   :widths: 25 25 25 25
   :header-rows: 1

   * - Template Type
     - Sources
     - Transformations
     - Sinks
   * - **Batch**
     - Stream
     - Filter
     - Datasets (Table)
   * - 
     - Datasets (Table)
     - Projection
     - Database
   * - 
     - Database
     - Script (Javascript)
     - TimePartitionedFileSets
   * - 
     - 
     - 
     - 
   * - **Realtime**
     - Twitter
     - Filter
     - Stream
   * - 
     - JMS
     - Projection
     - Datasets (Table)
   * - 
     - Kafka
     - Script (Javascript)
     - Cube Dataset


ETL Adapters
............
An ETL Adapter is an instance of an ETL Template that has been given a specific
configuration on creation.

Batch adapters can be scheduled to run periodically using a cron expression and can read
data from batch sources using a Mapreduce job. The batch adapter then performs any
optional transformations before writing to a batch sink.

Realtime adapters are designed to poll sources periodically to fetch the data, perform any
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




OLD MATERIAL
------------

An ETL Adapter is a representation of an ETL pipeline. An ETL pipeline contains a
Source, optional Transforms (zero or more) and a Sink. Only 
one such pipeline can be created in each ETL Adapter.

ETL Adapters are created from either of the two ETL Application Templates shipped with CDAP:

- ETL Batch
- ETL Realtime

ETL Batch Template
..................

ETL Batch Template is implemented using a Schedule added to a Workflow. The Workflow will
trigger a MapReduce driver program. A new Schedule is added for every new Adapter that is
created and started. This is the same mechanism used for all templates that use Workflow
as their driver program.

ETL Realtime Template
.....................

The ETL Realtime is implemented using a Worker. It is a continuously-running Worker program
that is launched when the Adapter is started. A new Worker program is started for every
Adapter that is created and started. This is the same mechanism used for all templates
that use Worker as their driver program.


What's a Plugin?
----------------
A Plugin carries out one of the steps in the ETL Pipeline. There are three main types of
Plugins used in an ETL Adapter:

- Source;
- Transform; and
- Sink. 

These are the basic building blocks for an ETL Adapter, both Batch and Realtime. When
creating an ETL Adapter, users provide the list of plugins that are to be used. In
addition to these basic plugin types, there can be other external plugins that are
used within the basic Plugin types. [Additional details are available in an Advanced section.]

Depending upon whether the ETL Batch or ETL Realtime template is used to create an ETL
Adapter, an appropriate source and sink needs to be used. That is, the Realtime Source and
Realtime Sink cannot be used to create an ETL Adapter from the ETL Batch template. 

Transforms can be used in either ETL Realtime and ETL Batch templates.


Custom Application Template and Plugins
---------------------------------------
If you are interested in writing your own Application Templates and Plugins, or
extending the existing ETL framework, please see these Developers’ Manual sections. [link]
