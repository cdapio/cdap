.. :author: Cask Data, Inc.
   :description: Introduction to the Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

==================================================
Introduction to the Cask Data Application Platform
==================================================

The Cask |(TM)| Data Application Platform (CDAP) is an open source project for distributed data
and applications. 

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

These documents are your complete guide to the Cask Data Application Platform: they help you get
started and set up your development environment; explain how CDAP works; and teach the
development and testing of CDAP applications.

It includes a complete reference to CDAP programming APIs and client interfaces, with instructions
on the installation, monitoring and diagnosing fully distributed CDAP in a Hadoop cluster.


Where to Go Next
================

.. |getting-started| replace:: **Getting Started:**
.. _getting-started: getting-started/index.html

- |getting-started|_ **For a quick, hands-on introduction to CDAP,** see :doc:`Getting Started with CDAP,
  <getting-started/index>` which guides you through installing the CDAP SDK, setting up your
  development environment, and building and running an example application.


.. |overview| replace:: **Overview:**
.. _overview: overview/index.html

- |overview|_ **The overall architecture and technology behind CDAP** is covered in :doc:`CDAP
  Overview <overview/index>`, including the virtualization of data and applications,
  CDAP concepts, components and their interactions, and the anatomy of a basic
  Big Data application.


.. |components| replace:: **CDAP Components:**
.. _components: components/index.html

- |components|_ This section covers **the two core virtualizations in the Cask Data
  Application Platform (CDAP): Data and Applications.** Data virtualizations are grouped into
  Streams and Datasets. Application virtualizations are grouped into Flows, MapReduce,
  Spark, Workflows, and Services. Details are provided on working with these abstractions
  to build Big Data applications.


.. |examples| replace:: **Examples, Guide and Tutorials:**
.. _examples: examples/index.html

- |examples|_ **Examples included with the SDK** range from a simple introductory *Hello
  World* to more elaborate examples such as the *Purchase Application* that use all of the
  CDAP components. A series of **How-To Guides**, designed to be completed in less than 30
  minutes each, covers topics such as data ingestion, data storage, and data processing; and a set of
  **Tutorials,** designed to be completed in anywhere from two to three hours each, gives
  in-depth case studies of web analytics, network analytics, social analytics and
  recommendation systems. 


.. |under-the-hood| replace:: **Under-the-Hood of CDAP:**
.. _under-the-hood: under-the-hood/index.html

- |under-the-hood|_ Covers **advanced topics on CDAP** that will be of interest to
  developers who want a deeper dive into CDAP, with presentations on the transaction system,
  data exploration, streams, and best practices fro CDP development.


.. |apis| replace:: **API References:**
.. _apis: apis/index.html

- |apis|_ documents four APIs of CDAP: **the HTTP RESTful API, the Java APIs (Javadocs), the Java
  Client API and the Command-Line Interface;** the latter provides methods for interacting with the 
  CDAP server from within a shell.


.. |testing| replace:: **Testing, Debugging, and Packaging:**
.. _testing: testing/index.html

- |testing|_ CDAP has Test Framework that developers can use with their applications, debugging procedures
  and tools, and recommendations on packaging your completed application prior to deployment.


.. |tools| replace:: **Tools:**
.. _tools: tools/index.html

- |tools|_ CDAP comes with a number of tools to make a developer’s life easier. These
  tools help with interacting with applications, ingesting data into CDAP, and fetching
  access tokens.


.. |user-interface| replace:: **User Interface:**
.. _user-interface: user-interface.html

- |user-interface|_ This section described the **CDAP Console**, the browser-based management application
  that allows you to control, manage and explore a CDAP instance, be it the Standalone SDK or a distributed
  cluster with hundreds of nodes.


.. |admin| replace:: **Administration:**
.. _admin: administration/index.html

- |admin|_ covers **installation, configuration, security setup, and putting CDAP into production and
  monitoring.** An appendix covers the XML file used to configure the CDAP security configuration.


.. |ops| replace:: **Operations:**
.. _ops: operations/index.html

- |ops|_ The :doc:`Operations Guide <operations/index>` covers 
  **security, monitoring, logging, metrics, runtime arguments, and scaling instances.** An
  appendix covers the XML file used to configure the CDAP installation.


.. |remaining| replace:: **Remaining Sections:**

- |remaining| The final documentation sections cover :doc:`Troubleshooting, <troubleshooting>`
  :doc:`Release Notes, <release-notes>`
  the :doc:`Licenses and Software Dependencies, <licenses/index>` 
  :doc:`Glossary, <glossary>` and a :doc:`FAQ. <faq/index>`
   

.. |(TM)| unicode:: U+2122 .. trademark sign
   :ltrim:

.. |(R)| unicode:: U+00AE .. registered trademark sign
   :ltrim:
