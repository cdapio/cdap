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

.. |case-study| replace:: **Case Study:**
.. _case-study: case-study.html

- |case-study|_ **A complete application case study** is presented in the :doc:`CDAP Application Case
  Study: <case-study>` it covers WISE, the Web Insights Engine, a CDAP application that
  performs analytics on web logs. The case study covers everything from the overall concept,
  underlying implementation, to a hands-on description of installing and running the
  application.

.. |arch| replace:: **Concepts and Architecture:**
.. _arch: arch.html

- |arch|_ **The overall architecture and technology behind CDAP** are covered in the :doc:`CDAP
  Concepts and Architecture <arch>`, including virtualization of data and applications,
  programming interfaces, CDAP components and their interactions, and the anatomy of a basic
  Big Data application.

.. |dev-guide| replace:: **Developer Guide:**
.. _dev-guide: dev-guide.html

- |dev-guide|_ **The Developer Guide** :doc:`CDAP Developer Guide <dev-guide>` is the reference manual
  with detailed explanations and background of CDAP APIs, components and programming
  techniques.

.. |apps-packs| replace:: **Apps and Packs:**
.. _apps-packs: apps-packs.html

- |apps-packs|_ :doc:`CDAP Applications and Packs <dev-guide>` covers **available data applications and
  libraries built with CDAP** that can be downloaded
  and used as a starting point for development.

.. |apis| replace:: **APIs and Clients:**
.. _apis: apis.html

- |apis|_ :doc:`CDAP HTTP RESTful API and Clients <apis>` documents **the HTTP RESTful API, the Java
  Client API and the Command-Line Interface;** the latter provides methods fro interacting with the 
  CDAP server from within a shell.
  
.. |javadoc| replace:: **Javadocs:**
.. _javadoc: javadoc/index.html

- |javadoc|_ :doc:`Javadocs <javadocs/index>` is the reference to **the Java APIs** that developers can
  :use in building their CDAP applications.

.. |tools| replace:: **Tools:**
.. _tools: tools.html

- |tools|_ **Testing, Debugging, Ingesting and Authentication Tools** are covered in :doc:`CDAP
  Available Tools. <tools>`

.. |admin| replace:: **Administration and Operations:**
.. _admin: admin.html

- |admin|_ The :doc:`Administration and Operations Guide <admin>` covers everything from
  **installation, configuration, security, monitoring, troubleshooting, through to putting
  CDAP into production,** using the CDAP Console, logging, metrics, and runtime arguments.
  Two appendices cover the XML files used to configure CDAP installations.

.. |remaining| replace:: **Remaining Sections:**

- |remaining| The final documentation sections cover the :doc:`Licenses and Software Dependencies,
  <licenses/index>` :doc:`Releases Notes, <release-notes>` and a :doc:`FAQ. <faq/index>`
   

.. |(TM)| unicode:: U+2122 .. trademark sign
   :ltrim:

.. |(R)| unicode:: U+00AE .. registered trademark sign
   :ltrim:
