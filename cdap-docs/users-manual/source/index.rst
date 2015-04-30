.. meta::
    :author: Cask Data, Inc.
    :description: Users' Manual
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _users-index:

==================================================
CDAP Users’ Manual
==================================================

Introduction
============
The motivation behind the CDAP Users’ Manual is that the users of CDAP should be able
to build solutions to their common big data problems without writing a single line of code.

Through the use of *Application Templates* and the creation of *Adapters*, a CDAP User can
take CDAP's out-of-the-box capabilities and perform common tasks such as ETL (Extraction,
Transformation and Loading) of data.


What are Application Template and Adapters?
-------------------------------------------

An *Application Template* is an extension to a CDAP Application. A Template has a
configuration class and a program which can use this configuration and be started. The
configuration class represents the customization that is provided for each solution that a
user creates. 

For example, the configuration might be a set of properties that is used by the program.
Each solution created results in a unique configuration and is referred to as an
*Adapter*. When a user starts an Adapter, a new run of the program specified inside the
Application Template is started.


What are Plugins?
-----------------
Each Adapter run may need to load an external class, depending upon the configuration
that is used to create the Adapter. These classes are packaged in JARs known as *Plugin*
JARs. There are specific Plugins associated with each Application Template. 


ETL
===
ETL is **Extract**, **Transformation** and **Loading** of data, and is a common first-step in any data
application. CDAP endeavors to make performing ETL possible out-of-box without writing
code; instead, you just configure an ETL Adapter and then operate it.

These sections describe how to do this:

.. |overview| replace:: **ETL Overview:**
.. _overview: etl/index.html

- |overview|_ An introduction to ETL and using CDAP out-of-the-box for ETL.


.. |etl-plugins| replace:: **ETL Plugins:**
.. _etl-plugins: etl/plugins.html

- |etl-plugins|_ Data sources, data sinks, and transformations; included with CDAP,
  available for use in ETL Adapters.


.. |etl-creating| replace:: **Creating An ETL Adapter:**
.. _etl-creating: etl/creating.html

- |etl-creating|_ Covers creating an ETL Adapter using the Plugins included with CDAP.


.. |etl-operations| replace:: **Operating An ETL Adapter:**
.. _etl-operations: etl/operations.html

- |etl-operations|_ Covers using an ETL Adapter created using the Plugins included with CDAP.


