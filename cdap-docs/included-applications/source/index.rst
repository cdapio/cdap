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


.. _included-apps-intro-hydrator:

.. rubric:: Cask Hydrator and ETL Pipelines

ETL is **Extract**, **Transform** and **Load** of data, a common first-step in any data
application. CDAP endeavors to make performing ETL possible out-of-box without writing
code; instead, you just configure CDAP appropriately and operate it.

In this CDAP release, we have added support for self-service batch and real-time data
ingestion combined with ETL for the building of Hadoop Data Lakes. Called **Cask Hydrator**, it
provides CDAP users a seamless and easy way to configure and operate ingestion pipelines
from different types of sources and data. 

*Cask Hydrator* provides an easy method of configuring
*pipelines* using a visual editor. You drag and drop sources, transformations, and sinks,
configuring an ETL pipeline within minutes. It provides an operational view of the
resulting ETL pipeline that allows for monitoring of metrics, logs, and other run-time
information.

.. figure:: /_images/hydrator-pipelines.png
   :figwidth: 100%
   :width: 6in
   :align: center
   :class: bordered-image

   **CDAP Hydrator Pipelines:** Administration of created pipelines with current status

.. figure:: /_images/hydrator-studio.png
   :figwidth: 100%
   :width: 6in
   :align: center
   :class: bordered-image

   **CDAP Hydrator Studio:** Visual editor showing the creation of an ETL pipeline


These sections describe:

.. |overview| replace:: **ETL Overview:**
.. _overview: etl/index.html

- |overview|_ An introduction to ETL, ETL applications, and ETL plugins.


.. |etl-creating| replace:: **Creating an ETL Application:**
.. _etl-creating: etl/creating.html

- |etl-creating|_ Covers using the system artifacts and ETL plugins included with CDAP to create an ETL application.


.. |etl-custom| replace:: **Creating Custom ETL Plugins:**
.. _etl-custom: etl/custom.html

- |etl-custom|_ Intended for developers writing custom ETL plugins.


.. |etl-plugins| replace:: **ETL Plugins:**
.. _etl-plugins: etl/plugins/index.html

- |etl-plugins|_ Details on ETL plugins and exploring available plugins using RESTful APIs.


.. |etl-third-party| replace:: **Using Third-Party Jars:**
.. _etl-third-party: etl/plugins/third-party.html

- |etl-third-party|_ Explains how to use a third-party JAR (such as a JDBC driver) as a plugin.


The **lifecycle of ETL Applications** is managed using CDAP's :ref:`Lifecycle HTTP RESTful API <http-restful-api-lifecycle>`.

.. rubric:: Data Quality Application

The goal of the **Data Quality Application** is to provide users with an extensible CDAP application to help them
determine the quality of their data. Users can assess the quality of their data using its out-of-the-box
functionality and libraries. The application can be extended with custom aggregation functions and queried with a
RESTful API to obtain the results of the quality metric computations.

.. |data-quality-guide| replace:: **Data Quality Application:**
.. _data-quality-guide: data-quality/index.html

- |data-quality-guide|_ Guide to creating and operating the application, with an end-to-end example.

