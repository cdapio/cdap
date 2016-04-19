.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

:section-numbering: true

.. _admin-installation-packages:

==================================
Manual Installation using Packages
==================================

.. include:: ../_includes/installation/installation-steps-images.txt

.. rubric:: Notes

This section describes installing CDAP on Hadoop clusters that are:

- Generic Apache Hadoop distributions; 
- CDH (Cloudera Data Hub) clusters *not managed* with Cloudera Manager; or
- HDP (Hortonworks Data Platform) clusters *not managed* with Apache Ambari.

Cloudera Manager (CDH), Apache Ambari (HDP), and MapR distributions should be installed
with our other :ref:`distribution instructions <installation-index>`.

- As CDAP depends on HDFS, YARN, HBase, ZooKeeper, and (optionally) Hive and Spark, it must be installed
  on cluster host(s) with full client configurations for these dependent services. 

- The CDAP Master Service must be co-located on a cluster host with an HDFS client, a YARN
  client, an HBase client, and |---| optionally |---| Hive or Spark clients.

- Note that these clients are redundant if you are co-locating the CDAP Master  
  on a cluster host (or hosts, in the case of a deployment with high availability) with
  actual services, such as the HDFS Namenode, the YARN resource manager, or the HBase
  Master.
  
- You can download the `Hadoop client <http://hadoop.apache.org/releases.html#Download>`__ 
  and `HBase client <http://www.apache.org/dyn/closer.cgi/hbase/>`__ libraries, and then
  install them on the hosts running CDAP services. No Hadoop or HBase services need be running.

- All services run as the ``'cdap'`` user installed by the package manager.


Preparing the Cluster
=============================
Please review the :ref:`Software Prerequisites <admin-manual-software-requirements>`, 
as a configured Hadoop, HBase, and Hive (plus an optional Spark client) needs to be configured on the
node(s) where CDAP will run. 

.. Hadoop Configuration
.. --------------------
.. include:: ../_includes/installation/hadoop-configuration.txt

.. HDFS Permissions
.. ----------------
.. include:: ../_includes/installation/hdfs-permissions.txt

  
Downloading and Distributing Packages
=====================================

Preparing Package Managers
--------------------------

.. include:: /../target/_includes/packages-installation.rst
    :start-after: .. _packages-preparing-package-managers:
    :end-before: .. end_install-debian-using-apt


Installing CDAP Services
========================

.. include:: /../target/_includes/packages-installation.rst
    :start-after: .. _packages-package-installation-title:


.. |display-distribution| replace:: Package Managers (RPM/Debian)

.. |hdfs-user| replace:: ``yarn``

.. include:: /../target/_includes/packages-configuration.rst
    :end-before: .. configuration-enabling-kerberos:


.. Starting CDAP Services
.. ======================

.. include:: /../target/_includes/packages-starting.rst

.. _packages-verification:


Verification
============

.. include:: /_includes/installation/smoke-test-cdap.txt


Advanced Topics
===============

.. _packages-configuration-security:

.. Enabling Perimeter Security
.. ---------------------------
.. include:: /../target/_includes/packages-configuration.rst
    :start-after: .. _packages-configuration-eps:

.. _packages-configuration-enabling-kerberos:

.. Enabling Kerberos
.. -----------------
.. include:: /../target/_includes/packages-configuration.rst
    :start-after: .. configuration-enabling-kerberos:
    :end-before: .. _packages-configuration-eps:

.. _packages-highly-available:

CDAP HA setup
-------------
Repeat the installation steps on additional boxes. The configuration settings (in
``cdap-site.xml``, *property:value*) needed to support high-availability are:

- ``kafka.seed.brokers``: ``127.0.0.1:9092,.../${root.namespace}`` 
  
  - Kafka brokers list (comma-separated), followed by ``/${root.namespace}``
  
- ``kafka.default.replication.factor``: 2

  - Used to replicate Kafka messages across multiple machines to prevent data loss in 
    the event of a hardware failure.
  - The recommended setting is to run at least two Kafka servers.
  - Set this to the number of Kafka servers.
