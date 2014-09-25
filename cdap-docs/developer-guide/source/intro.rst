.. :author: Cask Data, Inc.
   :description: Introduction to the Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

==================================================
Introduction to the Cask Data Application Platform
==================================================

The Cask |(TM)| Data Application Platform (CDAP) is an open source project for distributed data
and applications. 

CDAP is a layer of software running on top of Hadoop |(TM)| platforms such as the
Cloudera Enterprise Data Hub or the Hortonworks Data Platform. CDAP provides three essential elements:

- Virtualization of data in the Hadoop environment through logical representations of underlying
  data;
- Virtualization of applications through application containers; and
- Services and tools that enable faster application creation in development and higher degrees of
  operational control in production.

CDAP exposes developer APIs (Application Programming Interfaces) for creating applications
and accessing core CDAP services. CDAP defines and implements a diverse collection of services that land
applications and data on existing Hadoop infrastructure such as HBase, HDFS, YARN, MapReduce,
Hive, and Spark.

This document is your complete guide to the Cask Data Application Platform: it helps you get
started and set up your development environment; explains how CDAP works; teaches how to
develop and test applications with CDAP; and describes how to virtualize an existing Hadoop application.

It has a complete reference to CDAP programming APIs and client interfaces, with descriptions
of how to install, monitor and diagnose fully distributed CDAP in a Hadoop cluster.


Where to Go Next
================
Now that you've had an introduction to CDAP, take a look at:

- :ref:`get-started`,
  which guides you through installing the CDAP SDK, setting up your development environment,
  and building and running an example application.

.. |(TM)| unicode:: U+2122 .. trademark sign
   :ltrim:
