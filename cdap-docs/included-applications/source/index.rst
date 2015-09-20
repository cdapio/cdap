.. meta::
    :author: Cask Data, Inc.
    :description: Users' Manual
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-index:

==========================
CDAP Included Applications 
==========================

.. _included-apps-intro:

.. rubric:: Introduction to Included Applications

CDAP comes packaged with several system artifacts to create two types of applications: **ETL
(Extract, Transform, and Load) pipelines** and **Data Quality applications**, simply by
configuring the system artifacts and not writing any code at all.

An application created from a configured system artifact following the ETL pattern is
referred to as an *ETL pipeline* or (interchangeably) as an *ETL application*. Similarly, an
application built following the Data Quality pattern is referred to as a *Data Quality
application*.

In the future, a variety of system artifacts will be delivered. The framework is
extensible: users can write their own artifacts if they so chose, and can
manage the lifecycle of their custom applications using CDAP.


.. rubric:: ETL 

ETL is **Extract**, **Transform** and **Load** of data, a common first-step in any data
application. CDAP endeavors to make performing ETL possible out-of-box without writing
code; instead, you just configure CDAP appropriately and operate it.

These sections describe:

.. |overview| replace:: **ETL Overview:**
.. _overview: etl/index.html

- |overview|_ An introduction to ETL, ETL applications, and ETL plugins.


.. |etl-creating| replace:: **Creating an ETL Application:**
.. _etl-creating: etl/creating.html

- |etl-creating|_ Covers using the system artifacts and ETL plugins included with CDAP to create an ETL application.


.. |etl-operations| replace:: **Application Lifecycle Management:**
.. _etl-operations: ../reference-manual/http-restful-api/lifecycle.html

- |etl-operations|_ Manage ETL Applications using CDAP's :ref:`Lifecycle HTTP RESTful API <http-restful-api-lifecycle>`.


.. |etl-custom| replace:: **Creating Custom ETL Plugins:**
.. _etl-custom: etl/custom.html

- |etl-custom|_ Intended for developers writing custom ETL plugins.


.. |etl-plugins| replace:: **ETL Plugins:**
.. _etl-plugins: etl/plugins/index.html

- |etl-plugins|_ Details on ETL plugins and exploring available plugins using RESTful APIs.


.. |etl-third-party| replace:: **Using Third-Party Jars:**
.. _etl-third-party: etl/plugins/third-party.html

- |etl-third-party|_ Explains how to use a third-party JAR (such as a JDBC driver) as a plugin.
