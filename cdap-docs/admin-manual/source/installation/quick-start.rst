.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _installation-quick-start:

============================================
Installation Quick Start
============================================

These instructions will take you from determining your deployment architecture, installing
CDAP on a Hadoop cluster, through the running of a verification application in CDAP.

.. highlight:: console

Deployment
-------------

- Determine your deployment architecture, following the :ref:`suggested setups <deployment-architectures>`.
- Check that you have the :ref:`required hardware <install-hardware-requirements>`.

Software Prerequisites
----------------------

Install:

- Java runtime (`JDK or JRE version 1.7.xx or version 1.8.xx <http://www.java.com/en/download/manual.jsp>`__)
  on CDAP and Hadoop nodes; we recommend the Oracle JDK. 
- Set the JAVA_HOME environment variable (:ref:`details <install-java-runtime>`).
- `Node.js <http://nodejs.org>`__ on CDAP nodes (:ref:`details <install-node.js>`).
- Hadoop and HBase (and possibly Hive) environment to run against (:ref:`details <install-hadoop-hbase>`).
- CDAP nodes require Hadoop and HBase client installation and configuration. 
  **Note:** No Hadoop services need to be running.

Preparing the Cluster
----------------------

.. "To prepare your cluster"

.. include:: installation.rst 
   :start-after: .. _install-preparing-the-cluster:
   :end-before:  .. _install-preparing-the-cluster-defaults:

If your cluster is not setup with these defaults, you'll need to 
:ref:`edit your CDAP configuration <quick-start-configuration>` once you have downloaded and installed
the packages, and prior to starting services.

- For a *Secure Hadoop* installation, see the discussion in :ref:`the configuration guide
  <configuration-secure-hadoop>`.
  
- The ``ulimit`` for the cluster should already be configured correctly for HBase; if not, 
  details are covered in :ref:`the configuration guide <configuration-ulimit>`.
  
- The CDAP user should be able to write temp files; if not, see the instructions in 
  :ref:`the configuration guide <configuration-tmp-files>`.

- **Note:** ZooKeeper's ``maxClientCnxns`` must be raised from its default.  We suggest setting it to zero
  (unlimited connections). As each YARN container launched by CDAP makes a connection to ZooKeeper, 
  the number of connections required is a function of usage.


Configuring Package Managers
----------------------------

.. include:: installation.rst 
   :start-after: .. _install-rpm-using-yum:

.. _quick-start-configuration:

.. "Configuration title"

.. include:: configuration.rst 
   :start-after: .. _configuration-central:
   :end-before:  .. _configuration-alternatives:

.. "To configure your particular installation"

.. include:: configuration.rst 
   :start-after: .. _configuration-options:
   :end-before:  .. _configuration-explore-service:

.. "Starting Services title"

.. include:: configuration.rst 
   :start-after: .. _configuration-starting-services:
   :end-before:  .. _configuration-highly-available:

.. "Verification"

.. include:: configuration.rst 
   :start-after: .. _configuration-verification:
   :end-before:  .. _configuration-upgrade:
