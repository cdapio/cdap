.. meta::
    :author: Cask Data, Inc.
    :description: Users' Manual
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _apptemplates-index:

==================================================
CDAP Application Templates (Beta)
==================================================

.. _apptemplates-intro-application-templates:

Introduction to Application Templates
=====================================
CDAP allows developers to create distributed applications using the foundational
application building blocks (Datasets, Streams, Flows, MapReduce, Services, Workflows and
Workers). However, there are use cases that require that a similar type of action be
performed in different circumstances, differing only slightly in each case. 

For example, perhaps you need to copy data from five different relational tables to CDAP
Datasets periodically; you wouldn't want to deploy five different applications to
perform these tasks. Instead, you would want to reuse a common program that can be
configured to perform similar actions on differently configured data sources. In situations
like these, you can create or use an *Application Template*.

An **Application Template** is a blueprint that can be configured in different ways to
create multiple instances or *Adapters* of the same application. A useful analogy is that of
Java classes and instances. In Java, you often write a Class that performs a certain
function, but behaves a little differently based on arguments that are passed into its
constructor. In order to create an instance of the class, you call its constructor and
pass in arguments that specify the particular behavior you want. 

Similarly, an Application Template performs generally the same function, but behaves
differently depending on the configuration it is given. To create an instance of an
Application Template |---| called an *Adapter* |---| you make a REST call providing the
configuration.

Users of CDAP manage the lifecycle of an **Adapter** in a very similar manner to managing the
life cycle of a CDAP program.

CDAP comes packaged with two Application Templates to create ETL (Extract Transform Load)
pipelines, simply by configuring the ETL templates and not writing any code at all. In the
future, a variety of application templates will be delivered. The framework is extensible:
users can write their own application templates if they so chose, and can manage the
lifecycle of their custom application templates using CDAP.


ETL (Beta)
==========
ETL is **Extract**, **Transformation** and **Loading** of data, a common first-step in any data
application. CDAP endeavors to make performing ETL possible out-of-box without writing
code; instead, you just configure CDAP appropriately and operate it.

These sections describe:

.. |overview| replace:: **ETL Overview:**
.. _overview: etl/index.html

- |overview|_ An introduction to ETL, ETL Templates, Adapters, and Plugins.


.. |etl-templates| replace:: **ETL Templates:**
.. _etl-templates: etl/templates.html

- |etl-templates|_ Details on exploring Application Templates using RESTful APIs.


.. |etl-creating| replace:: **Creating An ETL Adapter:**
.. _etl-creating: etl/creating.html

- |etl-creating|_ Covers using the ETL Application Templates and Plugins included with CDAP to create an ETL Adapter.


.. |etl-operations| replace:: **Operating An ETL Adapter:**
.. _etl-operations: etl/operations.html

- |etl-operations|_ Covers using an ETL Adapter to perform ETL.


