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

- Determine your deployment architecture, following :ref:`the suggested setups <deployment-architectures>`.
- Check that you have :ref:`the required hardware <install-hardware-requirements>`.

Software Prerequisites
----------------------

Install:

- Java runtime (`JDK or JRE version 1.6.xx or 1.7.xx <http://www.java.com/en/download/manual.jsp>`__)
  on CDAP and Hadoop nodes. Set the JAVA_HOME environment variable. (:ref:`details <install-java-runtime>`)
- `Node.js <http://nodejs.org>`__ on CDAP nodes. (:ref:`details <install-node.js>`)
- Hadoop and HBase (and possibly Hive) environment to run against. (:ref:`details <install-hadoop-hbase>`)

Preparing the Cluster
----------------------

.. "To prepare your cluster"

.. include:: installation.rst 
   :start-line: 196
   :end-line:   203

- For a *Secure Hadoop* installation, see the discussion in :ref:`the installation guide
  <install-secure-hadoop>`.
  
- The ``ulimit`` for the cluster should already be configured correctly for HBase; if not, 
  details are covered in :ref:`the installation guide <install-ulimit>`.

Preparing Package Managers
--------------------------

.. include:: installation.rst 
   :start-line: 245
   :end-line:   299

Configuration
-------------

.. "When you install the CDAP base package"

.. include:: installation.rst 
   :start-line: 229
   :end-line:   232
   
.. "To make alterations"

.. include:: installation.rst 
   :start-line: 305
   :end-line:   357

.. "Starting Services"

.. include:: installation.rst 
   :start-line: 403
   :end-line:   414

Making CDAP Highly-available
---------------------------------
Repeat these steps on additional boxes.  The configurations needed to support high-availability are:

- ``kafka.seed.brokers``: ``127.0.0.1:9092,...`` 
  
  - Kafka brokers list (comma separated)
  
- ``kafka.default.replication.factor``: 2

  - Used to replicate Kafka messages across multiple machines to prevent data loss in 
    the event of a hardware failure.
  - The recommended setting is to run at least two Kafka servers.
  - Set this to the number of Kafka servers.

.. "Verification"

.. include:: installation.rst 
   :start-line: 417
   :end-line:   439
