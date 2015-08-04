.. meta::
    :author: Cask Data, Inc.
    :description: Introduction to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-relations: true

:hide-global-toc: true

.. _documentation-index:

==================================================
CDAP Documentation v\ |version|
==================================================

.. .. rubric:: Introduction to the Cask Data Application Platform

The Cask |(TM)| Data Application Platform (CDAP) is an integrated, open source application
development platform for the Hadoop ecosystem that provides developers with data and
application abstractions to simplify and accelerate application development, address a
broader range of real-time and batch use cases, and deploy applications into production
while satisfying enterprise requirements.

CDAP is a layer of software running on top of Apache Hadoop |(R)| platforms such as
the Cloudera Enterprise Data Hub, the Hortonworks |(R)| Data Platform, or 
the MapR Distribution. CDAP provides these essential capabilities:

- Abstraction of data in the Hadoop environment through logical representations of underlying
  data;
- Portability of applications through decoupling underlying infrastructures;
- Services and tools that enable faster application creation in development;
- Integration of the components of the Hadoop ecosystem into a single platform; and
- Higher degrees of operational control in production through enterprise best practices.

CDAP exposes developer APIs (Application Programming Interfaces) for creating applications
and accessing core CDAP services. CDAP defines and implements a diverse collection of services that land
applications and data on existing Hadoop infrastructure such as HBase, HDFS, YARN, MapReduce,
Hive, and Spark.

These documents are your complete reference to the Cask Data Application Platform: they help
you get started and set up your development environment; explain how CDAP works; and teach
how to develop and test CDAP applications.

It includes the CDAP programming APIs and client interfaces, with instructions
on the installation, monitoring and diagnosing fully distributed CDAP in a Hadoop cluster.


.. |introduction| replace:: **Introduction to CDAP:**
.. _introduction: introduction/index.html

- |introduction|_

  - **Simple Access to Powerful Technology:** A comparison between using the current 
    technologies available from the Hadoop ecosystem and using CDAP


.. |developers-manual| replace:: **Developers’ Manual:**
.. _developers-manual: developers-manual/index.html

- |developers-manual|_

  - **Getting Started Developing:** A quick, hands-on introduction to developing with CDAP
  - **Overview:** The overall architecture and technology behind CDAP
  - **Building Blocks:** The two core abstractions in CDAP: *Data* and *Applications*, and their components
  - **Security:** Perimeter security, configuration and client authentication
  - **Testing and Debugging:** Test framework plus tools and practices for debugging your applications
  - **Ingesting Data:** Different techniques for ingesting data into CDAP
  - **Advanced Topics:** Best practices for CDAP development, adding a custom logback


.. |application-templates| replace:: **Application Templates:**
.. _application-templates: application-templates/index.html

- |application-templates|_

  - **Big Data without Big Development:** How to use CDAP "out-of-the-box" to solve problems and use cases 
  - **Introduction to Application Templates:** Applications that are reusable through configuration and
    extensible through plugins 
  - **ETL:** Making performing ETL possible without writing code 
  - **Creating Custom ETL Plugins:** For developers of custom ETL plugins 
  
  
.. |admin-manual| replace:: **Administration Manual:**
.. _admin-manual: admin-manual/index.html

- |admin-manual|_ 

  - **Installation:** Putting CDAP into production, with installation, configuration, security setup, and monitoring
  - **Operations:** Logging, metrics, runtime arguments, scaling instances, resource
    guarantees, transaction service maintenance, troubleshooting and introduces the CDAP UI


.. |integrations| replace:: **Integrations:**
.. _integrations: integrations/index.html

- |integrations|_ 

  - **Cloudera:** Integrating CDAP into Cloudera, using Cloudera Manager, and running interactive queries with Impala
  - **JDBC:** The CDAP JDBC driver, included with CDAP
  - **Pentaho:** *Pentaho Data Integration*, a business intelligence tool that can be used with CDAP
  - **Squirrel:** *SquirrelSQL*, a simple JDBC client that can be integrated with CDAP


.. |examples-manual| replace:: **Examples, Guides, and Tutorials:**
.. _examples-manual: examples-manual/index.html

- |examples-manual|_

  - **Examples:** Included with the :ref:`CDAP SDK <getting-started-index>`, they range from a simple introductory to more elaborate examples
  - **How-To Guides:** Designed to be completed in 15-30 minutes, these guides provide quick, hands-on instructions
  - **Tutorials:** Designed to be completed in 2-3 hours, these tutorials provide deeper, in-context explorations
  - **CDAP Apps and Packs Repository:** data applications built using CDAP and useful building blocks for your data applications


.. |reference-manual| replace:: **Reference Manual:**
.. _reference-manual: reference-manual/index.html

- |reference-manual|_ 

  - **Command Line Interface API:** Methods for interacting with a CDAP instance from within a shell
  - **HTTP RESTful API:** HTTP interface for a multitude of purposes
  - **Java Client API:** Methods for interacting with CDAP from external Java applications
  - **Javadocs:** The Java APIs for writing CDAP applications
  - **Trademarks, Licenses, and Dependencies:** Trademark and License information for CDAP and lists of CDAP dependent packages


.. |release-notes| replace:: **Release Notes:**
.. _release-notes: reference-manual/release-notes.html

- |release-notes|_ Notes for current and previous CDAP versions


.. |glossary| replace:: **Glossary:**
.. _glossary: reference-manual/glossary.html

- |glossary|_ Definitions of terms and phrases used in CDAP


.. |faq| replace:: **FAQ:**
.. _faq: reference-manual/faq.html

- |faq|_ Answers to questions you might have about CDAP


.. |search| replace:: **Search:**
.. _search: search.html

- |search|_ Search this documentation using *Quick Search*
