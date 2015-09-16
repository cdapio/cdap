.. meta::
    :author: Cask Data, Inc.
    :description: Users' Manual
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _apptemplates-index:

==========================
CDAP Application Templates 
==========================


.. _apptemplates-intro-application-templates:

.. rubric:: Introduction to Application Templates

CDAP allows developers to create distributed applications using the foundational
application building blocks (datasets, streams, flows, MapReduce, services, workflows and
workers). However, there are use cases that require that a similar type of action be
performed in different circumstances, differing only slightly in each case. 

For example, perhaps you need to copy data from five different relational tables to CDAP
Datasets periodically; you wouldn't want to deploy five different applications to
perform these tasks. Instead, you would want to reuse code or a common program that can be
configured to perform similar actions on differently configured data sources. In situations
like these, you can create or use an *Application Template*.

An **Application Template** is an application that is reusable through configuration and
extensible through plugins. Plugins extend the application template by implementing an
interface expected by the template. One or more plugins are packaged in a specifically
constructed JAR file.

To create an instantiation of an application template |---| called an **Adapter** |---| you
can make a RESTful call, use a CLI command, or use the CDAP UI, providing the
configuration at the time. Users of CDAP manage the lifecycle of an adapter in a very
similar manner to managing the life cycle of a CDAP program.

CDAP comes packaged with two application templates to create ETL (Extract, Transform, and
Load) pipelines, simply by configuring the ETL templates and not writing any code at all.
An adapter created from an ETL Application template is referred to as an *ETL pipeline* or
*ETL adapter*.

In the future, a variety of application templates will be delivered. The framework is
extensible: users can write their own application templates if they so chose, and can
manage the lifecycle of their custom application templates using CDAP.


.. rubric:: ETL 

ETL is **Extract**, **Transform** and **Load** of data, a common first-step in any data
application. CDAP endeavors to make performing ETL possible out-of-box without writing
code; instead, you just configure CDAP appropriately and operate it.

These sections describe:

.. |overview| replace:: **ETL Overview:**
.. _overview: etl/index.html

- |overview|_ An introduction to ETL, ETL templates, adapters, and plugins.


.. |etl-templates| replace:: **ETL Templates:**
.. _etl-templates: etl/plugins/index.html

- |etl-templates|_ Details on Application Templates and exploring Application templates using RESTful APIs.


.. |etl-creating| replace:: **Creating an ETL Adapter:**
.. _etl-creating: etl/creating.html

- |etl-creating|_ Covers using the ETL application templates and plugins included with CDAP to create an ETL adapter.


.. |etl-operations| replace:: **Adapter Lifecycle Management:**
.. _etl-operations: etl/operations.html

- |etl-operations|_ Covers using an ETL adapter to perform ETL.


.. |etl-custom| replace:: **Creating Custom ETL Plugins:**
.. _etl-custom: etl/custom.html

- |etl-custom|_ Intended for developers writing custom ETL plugins.


.. |etl-third-party| replace:: **Using Third-party Jars:**
.. _etl-third-party: etl/third-party.html

- |etl-third-party|_ Explains how to use a third-party JAR (such as JDBC driver) as a plugin.
