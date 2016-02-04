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

Preparing the Cluster
=============================
Please review the :ref:`Software Prerequisites <admin-manual-software-requirements>`, 
as a configured Hadoop, HBase, and Hive (plus an optional Spark client) needs to be configured on the
node(s) where CDAP will run. 

.. Node.js Installation
.. --------------------
.. include:: /../target/_includes/packages-installation.rst
    :start-after: .. _packages-install-node-js:
    :end-before: .. _packages-install-packaging:

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
