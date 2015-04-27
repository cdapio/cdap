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
writing code; instead, you just configure an ETL Adaptor and then operate it.

Typically, ETL is operated as a pipeline. Data comes from a Data Source, is (possibly) sent
through a series of one or more Transformations, and then is persisted by a Data Sink.

This diagram outlines these steps:


.. image:: ../_images/etl-pipeline.png
   :width: 6in
   :align: center


What’s an ETL Adaptor?
----------------------

An ETL Adaptor is a CDAP Application that is used to create such an ETL pipeline. Only 
one such pipeline can be created in each ETL Adaptor.

ETL Adaptors are created from either of the two ETL Application Templates shipped with CDAP:

- ETL Batch
- ETL Realtime

Application Templates are built from CDAP Plugins. Application Templates are used to build CDAP
Adaptors, of which one type is an ETL Adaptor.

(If you are interested in writing your own Adaptor, Application Template, Plugin, or
extending the existing ETL framework, please see these Developers’ Manual sections. [link])


Supported Use Cases
-------------------

The CDAP ETL framework is designed around supporting two different ETL Use Cases: Batch and 
Realtime.


- Batch

  TBD

- Realtime

  TBD
  


