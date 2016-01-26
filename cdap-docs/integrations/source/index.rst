.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _integrations:
 
============
Integrations
============


.. rubric:: Cloudera

.. |cloudera-introduction| replace:: **Introduction:**
.. _cloudera-introduction: partners/cloudera/index.html

- |cloudera-introduction|_ Utilizing CDAP on **Cloudera Enterprise Data Hub**


.. |cloudera-install| replace:: **Configuring and Installing:**
.. _cloudera-install: ../admin-manual/installation/cloudera.html

- |cloudera-install|_ Configuring and installing CDAP using **Cloudera Manager** *(Administration Manual)*


.. |cloudera-ingesting| replace:: **Ingestion and Exploration:**
.. _cloudera-ingesting: partners/cloudera/ingesting.html

- |cloudera-ingesting|_ Ingesting and exploring data using **Impala**


.. |cloudera-faq| replace:: **FAQ:**
.. _cloudera-faq: ../faqs/cloudera-manager..html

.. - |cloudera-faq|_ for Cloudera and Impala


.. rubric:: Ambari

.. |ambari| replace:: **Configuring and Installing:**
.. _ambari: ../admin-manual/installation/ambari.html

- |ambari|_ Configuring and installing CDAP using **Ambari** *(Administration Manual)*


.. rubric:: MapR

.. |mapr| replace:: **Configuring and Installing:**
.. _mapr: ../admin-manual/installation/mapr.html

- |mapr|_ Configuring and installing CDAP on **MapR** *(Administration Manual)*


.. rubric:: BI Tools: Accessing CDAP Datasets through Business Intelligence Tools

Most Business Intelligence tools can integrate with relational databases using JDBC
drivers. They often include drivers to connect to standard databases such as MySQL or
PostgreSQL. Most tools allow the addition of non-standard JDBC drivers.

Two business intelligence tools |---| :ref:`SquirrelSQL <squirrel-integration>` and 
:ref:`Pentaho Data Integration <pentaho-integration>` |---| are covered, explaining connecting
them to a running CDAP instance and interacting with CDAP datasets.

The example :ref:`cdap-bi-guide` includes the use of Pentaho.

.. |jdbc| replace:: **CDAP JDBC Driver:**
.. _jdbc: jdbc.html

- |jdbc|_ A JDBC driver provided with CDAP to make **integrations with external programs**
  and third-party BI (business intelligence) tools easier.


.. |pentaho| replace:: **Pentaho Data Integration:**
.. _pentaho: pentaho.html

- |pentaho|_ An advanced, open source **business intelligence tool** that can execute
  transformations of data.


.. |squirrel| replace:: **SquirrelSQL:**
.. _squirrel: squirrel.html

- |squirrel|_ A simple JDBC client which **executes SQL queries** against many different relational databases.


