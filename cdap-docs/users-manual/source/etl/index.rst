.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _users-etl-index:

============
ETL Overview
============


What is ETL?
============
ETL is **Extract**, **Transformation** and **Loading** of data, and is a common first-step
in any data application. CDAP endeavors to make performing ETL possible out-of-box without
writing code; instead, you just configure an ETL Adapter and then operate it.

Typically, ETL is operated as a pipeline. Data comes from a Data Source, is (possibly) sent
through a series of one or more Transformations, and then is persisted by a Data Sink.

This diagram outlines the steps of an ETL Pipeline:


.. image:: ../_images/etl-pipeline.png
   :width: 6in
   :align: center


What’s an ETL Adapter?
----------------------

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
