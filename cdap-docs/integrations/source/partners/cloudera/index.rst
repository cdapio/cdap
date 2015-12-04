.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _cloudera-index:

========
Cloudera
========

Overview
========

.. toctree::
   :maxdepth: 1
   
    Ingestion and Exploration <ingesting>

.. include:: /_includes/cloudera-certified.rst
  
Today, Hadoop is frequently used as an offline analytics tool – generating insights that
are then deployed operationally in other systems.  Greater business value is generated
when organizations turn their data analytics directly into action.  Cask and Cloudera
refer to this approach as operational analytics; applications that incorporate data
analytics are referred to as data applications.  The goal of the Cloudera-Cask partnership
is to help customers overcome the challenges in building data applications and accelerate
the value creation from operational analytics.

Utilizing CDAP on Cloudera Enterprise Data Hub (EDH) is a seamless experience. CDAP is
integrated with Cloudera Manager, enabling customers to install, update, and monitor CDAP
directly within the Cloudera Manager user interface. CDAP provides automation for
ingestion and exploration of data in Cloudera Impala.  Rather than writing a MapReduce
program to transform data into the Impala file format and scheduling periodic
transformation jobs in a separate system such as Oozie, developers can just issue a few
simple commands and either batch or streaming data will automatically be ingested into
Impala and available for high-performance SQL queries.

The Cask™ Data Application Platform (CDAP) integrates with the `Cloudera Manager
<http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__.
Configurations that include Cloudera Manager can be easily configured to ingest data into
a cluster, specify schema, or run interactive queries using Impala with CDAP for faster
results.

**CDAP** |bold-version| is certified on Cloudera 5.


.. |cloudera-install| replace:: **Configuring and Installing:**
.. _cloudera-install: ../../../admin-manual/installation/cloudera/index.html

- |cloudera-install|_ Configuring and installing CDAP using **Cloudera Manager** *(Administration Manual)*

.. |cloudera-ingesting| replace:: **Ingesting and Exploration:**
.. _cloudera-ingesting: ingesting.html

- |cloudera-ingesting|_ Ingest and explore data using **Impala**


.. rubric:: CDAP and Cloudera Architecture Schematic

.. figure:: /_images/cloudera-integration-arch.png
   :figwidth: 100%
   :width: 800px
   :align: center
   :class: bordered-image

