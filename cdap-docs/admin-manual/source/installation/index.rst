.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _installation-index:

============
Installation
============

.. toctree::
   :maxdepth: 1
   
    Apache Ambari (HDP) <ambari/index>
    Cloudera Manager (CDH) <cloudera/index>
    Hadoop <hadoop/index>
    MapR <mapr/index>

**Distribution-specific** instructions are available, plus instructions for **generic Apache Hadoop** clusters:

.. |ambari| replace:: **Apache Ambari (HDP):**
.. _ambari: ambari/index.html

- |ambari|_ Installing CDAP on `HDP (Hortonworks Data Platform) <http://hortonworks.com/>`__ clusters
  managed with `Apache Ambari <https://ambari.apache.org/>`__.

.. |cloudera| replace:: **Cloudera Manager (CDH):**
.. _cloudera: cloudera/index.html

- |cloudera|_ Installing CDAP on `CDH (Cloudera Data Hub) <http://www.cloudera.com/>`__ clusters
  managed with `Cloudera Manager
  <http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__.

.. |hadoop| replace:: **Hadoop:**
.. _hadoop: hadoop/index.html

- |hadoop|_ Installing CDAP on Hadoop systems, either **generic Apache Hadoop** distributions, 
  CDH clusters *not* managed with Cloudera Manager, or HDP clusters *not* managed with Apache Ambari.

.. |mapr| replace:: **MapR:**
.. _mapr: mapr/index.html

- |mapr|_ Installing CDAP on `MapR systems <https://www.mapr.com>`__.

.. |sdk| replace:: **CDAP Standalone SDK:**
.. _sdk: ../../developers-manual/getting-started/standalone/index.html

- |sdk|_ Installing the CDAP Standalone SDK on Linux, MacOS, and Windows systems. *(Developers' Manual)*


.. rubric:: Putting CDAP into Production

The Cask Data Application Platform (CDAP) can be run in different modes: in-memory mode
for unit testing, Standalone CDAP for testing on a developer's laptop, and Distributed
CDAP for staging and production.

Regardless of the runtime edition, CDAP is fully functional and the code you develop never
changes. However, performance and scale are limited when using in-memory or standalone
CDAPs.


.. _in-memory-data-application-platform:

.. rubric:: In-memory CDAP

The in-memory CDAP allows you to easily run CDAP for use in unit tests. In this mode, the
underlying Big Data infrastructure is emulated using in-memory data structures and there
is no persistence. The CDAP UI is not available in this mode.


.. rubric:: Standalone CDAP

The Standalone CDAP allows you to run the entire CDAP stack in a single Java Virtual
Machine on your local machine and includes a local version of the CDAP UI. The
underlying Big Data infrastructure is emulated on top of your local file system. All data
is persisted.

The Standalone CDAP by default binds to the localhost address, and is not available for
remote access by any outside process or application outside of the local machine.

See :ref:`Getting Started <getting-started-index>` and the *Cask Data Application Platform
SDK* for information on how to start and manage your Standalone CDAP.


.. rubric:: Distributed Data Application Platform

The Distributed CDAP runs in fully distributed mode. In addition to the system components
of the CDAP, distributed and highly available deployments of the underlying Hadoop
infrastructure are included. Production applications should always be run on a Distributed
CDAP.

To learn more about getting your own Distributed CDAP, see `Cask Products
<http://cask.co/products>`__.

