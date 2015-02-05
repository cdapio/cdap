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

- Java runtime (`JDK or JRE version 1.6.xx or 1.7.xx <http://www.java.com/en/download/manual.jsp>`__)
  on CDAP and Hadoop nodes. Set the JAVA_HOME environment variable. (:ref:`details <install-java-runtime>`)
- `Node.js <http://nodejs.org>`__ on CDAP nodes. (:ref:`details <install-node.js>`)
- Hadoop and HBase (and possibly Hive) environment to run against. (:ref:`details <install-hadoop-hbase>`)
- CDAP nodes require Hadoop and HBase client installation and configuration. 
  **Note:** No Hadoop services need to be running.

Preparing the Cluster
----------------------

.. "To prepare your cluster"

.. include:: ../../build/_includes/installation.rst 
   :start-after: .. _install-preparing-the-cluster:
   :end-before:  .. _install-preparing-the-cluster-defaults:

If your cluster is not setup with these defaults, you'll need to 
:ref:`edit your CDAP configuration <quick-start-configuration>` once you have downloaded and installed
the packages, and prior to starting services.

- For a *Secure Hadoop* installation, see the discussion in :ref:`the installation guide
  <install-secure-hadoop>`.
  
- The ``ulimit`` for the cluster should already be configured correctly for HBase; if not, 
  details are covered in :ref:`the installation guide <install-ulimit>`.
  
- The CDAP user should be able to write temp files; if not, see the instructions in 
  :ref:`the installation guide <install-tmp-files>`.

- **Note:** Zookeeper's ``maxClientCnxns`` must be raised from its default.  We suggest setting it to zero
  (unlimited connections). As each YARN container launched by CDAP makes a connection to Zookeeper, 
  the number of connections required is a function of usage.


Configuring Package Managers
----------------------------

.. include:: ../../build/_includes/installation.rst 
   :start-after: .. _install-rpm-using-yum:
   :end-before:  .. _install-configuration:


.. _quick-start-configuration:

.. "Configuration title"

.. include:: ../../build/_includes/installation.rst 
   :start-after: .. _install-configuration:
   :end-before:  .. _install-alternatives:

.. "To configure your particular installation"

.. include:: ../../build/_includes/installation.rst 
   :start-after: .. _install-configuration-options:
   :end-before:  .. _install-configuring-explore-service:

.. "Starting Services title"

.. include:: ../../build/_includes/installation.rst 
   :start-after: .. _install-starting-services:
   :end-before:  .. _install-highly-available:

.. "Verification"

.. include:: ../../build/_includes/installation.rst 
   :start-after: .. _install-verification:
   :end-before:  .. _install-upgrade:
