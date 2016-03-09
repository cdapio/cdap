.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cdap-apps-etl-overview:

============
ETL Overview 
============

.. toctree::
    :hidden:
    :glob:

    **

A Quick Intro to ETL
====================

Most data infrastructures are front ended with an ETL system (Extract-Transform-Load). The
purpose of the system is to:

- Extract data from one or more sources;
- Transform the data to fit the operational needs; and
- Load the data into a desired destination.

ETL usually comprises a source, zero or more transformations and one or more sinks, in what is called
an *ETL pipeline:*

.. image:: ../_images/etl-pipeline.png
   :width: 6in
   :align: center


.. rubric:: System Artifacts, ETL Applications, and ETL Plugins 

An *Artifact* is a blueprint that |---| with a configuration file |---| is used to create an *Application*.

CDAP provides system artifacts |---| ``cdap-etl-batch`` and ``cdap-etl-realtime`` |---|
which are used to create applications that perform ETL in either batch or real time, and
consist of a variety of sources, transformations, and sinks that are packaged together.

A third system artifact, ``cdap-etl-lib``, provides common resources for the other two system artifacts.

The batch sources can write to any batch sinks that are available and real-time sources can
write to any real-time sinks. Transformations work with either sinks or sources. Transformations
can use *validators* to test data and check that it follows user-specified rules.

The section :doc:`ETL Plugins <hydrator-plugins/index>` lists the available sources, sinks, and transformations (transforms).


.. rubric:: ETL Applications

ETL applications are created by preparing a configuration that specifies the ETL artifact and
which source, transformations (transforms), and sinks are used to create the application. The
configuration can either be written as a JSON file or, in the case of the CDAP UI,
specified in-memory.

**Batch applications** can be scheduled to run periodically using a cron expression and can read
data from batch sources using a MapReduce job. The batch application then performs any
optional transformations before writing to one or more batch sinks.

**Real time applications** are designed to poll sources periodically to fetch the data, perform any
optional transformations, and then write to one or more real-time sinks.


.. rubric:: ETL Plugins

The sources, transformations, and sinks are generically called plugins. Plugins provide a
way to extend the functionality of existing artifacts. An application can be created with
existing plugins or, if the user wishes, they can write their own source, transform, and
sink plugins to add their own. You can write your own validator using functions supplied in
either the :ref:`CoreValidator <cdap-apps-etl-plugins-shared-core-validator>` or implement your own.


Application and Plugin Details
==============================

.. |etl-creating| replace:: **Creating an ETL Application:**
.. _etl-creating: creating.html

- |etl-creating|_ Covers using the system artifacts and ETL plugins included with CDAP to create an ETL application.


.. |etl-operations| replace:: **Application Lifecycle Management:**
.. _etl-operations: ../../reference-manual/http-restful-api/lifecycle.html

- |etl-operations|_ Manage ETL Applications using CDAP's :ref:`Lifecycle HTTP RESTful API <http-restful-api-lifecycle>`.


.. |etl-custom| replace:: **Creating Custom ETL Plugins:**
.. _etl-custom: custom.html

- |etl-custom|_ Intended for developers writing custom ETL plugins.

.. |etl-plugins| replace:: **ETL Plugins:**
.. _etl-plugins: hydrator-plugins/index.html

- |etl-plugins|_ Details on ETL plugins and exploring available plugins using RESTful APIs.
