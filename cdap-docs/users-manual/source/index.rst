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
to build solutions to common big data problems without writing a single line of Java code.

Through the use of *Application Templates* and the creation of *Adapters*, a CDAP User can
take CDAP's out-of-the-box capabilities and perform common tasks such as the ETL (Extraction,
Transformation and Loading) of data.

ETL (Beta)
==========
ETL is **Extract**, **Transformation** and **Loading** of data, and is a common first-step in any data
application. CDAP endeavors to make performing ETL possible out-of-box without writing
code; instead, you just configure CDAP appropriately and then operate it.

These sections describe:

.. |overview| replace:: **ETL Overview:**
.. _overview: etl/index.html

- |overview|_ An introduction to ETL, ETL Templates, Adapters, and Plugins.


.. |etl-plugins| replace:: **ETL Plugins:**
.. _etl-plugins: etl/plugins.html

- |etl-plugins|_ Data sources, data sinks, and transformations; included with CDAP,
  available for use in ETL Adapters.


.. |etl-creating| replace:: **Creating An ETL Adapter:**
.. _etl-creating: etl/creating.html

- |etl-creating|_ Covers using the Plugins included with CDAP to create an ETL Adapter.


.. |etl-operations| replace:: **Operating An ETL Adapter:**
.. _etl-operations: etl/operations.html

- |etl-operations|_ Covers using an ETL Adapter created with the CDAP ETL Templates.


