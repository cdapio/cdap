.. meta::
    :author: Cask Data, Inc.
    :description: Introduction to the Cask Data Application Platform
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _developer-index:

==================================================
CDAP Developers’ Manual
==================================================

.. rubric:: Introduction to the Cask Data Application Platform

The Cask |(TM)| Data Application Platform (CDAP) is an open source application development
platform for the Hadoop ecosystem that provides developers with data and application
virtualization to accelerate application development, address a broader range of real-time
and batch use cases, and deploy applications into production while satisfying enterprise
requirements.

CDAP is a layer of software running on top of Apache |(TM)| Hadoop |(R)| platforms such as the
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

These documents are your complete manual to the Cask Data Application Platform: they help
you get started and set up your development environment; explain how CDAP works; and teach
how to develop and test CDAP applications.

It includes a complete reference to CDAP programming APIs and client interfaces, with instructions
on the installation, monitoring and diagnosing fully distributed CDAP in a Hadoop cluster.


.. |getting-started| replace:: **Getting Started:**
.. _getting-started: getting-started/index.html

- |getting-started|_ **A quick, hands-on introduction to CDAP,**  which guides you through
  installing the CDAP SDK, setting up your development environment, starting and stopping CDAP, 
  and building and running example applications.
  

.. |overview| replace:: **Overview:**
.. _overview: overview/index.html

- |overview|_ Covers the **overall architecture and technology behind CDAP,** including
  the virtualization of data and applications, CDAP concepts, components and their
  interactions, and the anatomy of a Big Data application.


.. |building-blocks| replace:: **Building Blocks:**
.. _building-blocks: building-blocks/index.html

- |building-blocks|_ This section covers **the two core virtualizations in the Cask Data
  Application Platform (CDAP): Data and Applications.** Data virtualizations are grouped into
  Streams and Datasets. Application virtualizations are grouped into Flows, MapReduce,
  Spark, Workflows, and Services. Details are provided on working with these abstractions
  to build Big Data applications.


.. |security| replace:: **Security:**
.. _security: security/index.html

- |security|_ CDAP supports securing clusters using perimeter security. Configuration
  and client authentication are covered in this section.


.. |testing| replace:: **Testing and Debugging:**
.. _testing: testing/index.html

- |testing|_ CDAP has a **test framework** that developers can use with their applications, **tools and procedures
  for debugging ,** and recommendations on **packaging your completed application** prior to deployment.


.. |ingesting-tools| replace:: **Ingesting Data:**
.. _ingesting-tools: ingesting-tools/index.html

- |ingesting-tools|_ CDAP comes with a number of tools to make a developer’s life easier. These
  tools help with **ingesting data into CDAP:** using Java or Python, the bulk ingestion of files,
  a daemon to tail local files and an Apache Flume Sink implementation.


.. |advanced| replace:: **Advanced Topics:**
.. _advanced: advanced/index.html

- |advanced|_ Covers **advanced topics on CDAP** that will be of interest to
  developers who want a deeper dive into CDAP, with presentations on the Data Exploration
  of Datasets, Client Authentication for Secure CDAP Instances, and Best Practices for
  CDAP development.


.. |troubleshooting| replace:: **Troubleshooting:**
.. _troubleshooting: troubleshooting.html

- |troubleshooting|_ Selected examples of potential problems and possible resolutions.
  

.. |(TM)| unicode:: U+2122 .. trademark sign
   :ltrim:

.. |(R)| unicode:: U+00AE .. registered trademark sign
   :ltrim:
