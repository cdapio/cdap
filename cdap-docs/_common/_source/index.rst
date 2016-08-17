.. meta::
    :author: Cask Data, Inc.
    :description: Introduction to the Cask Data Application Platform
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

:hide-relations: true

:hide-global-toc: true

.. _documentation-index:

==================================================
CDAP Documentation v\ |version|
==================================================

.. .. rubric:: Introduction to the Cask Data Application Platform

The Cask |(TM)| Data Application Platform (CDAP) is an integrated, open source application
development platform for the Apache Hadoop |(R)| ecosystem that provides developers with data and
application abstractions to simplify and accelerate application development, address a
broader range of real-time and batch use cases, and deploy applications into production
while satisfying enterprise requirements.

CDAP is a layer of software running on top of Hadoop platforms such as
the Cloudera Enterprise Data Hub, the Hortonworks |(R)| Data Platform, or 
the MapR Distribution. CDAP provides these essential capabilities:

- Abstraction of data in the Hadoop environment through logical representations of underlying
  data;
- Portability of applications through decoupling underlying infrastructures;
- Services and tools that enable faster application creation in development;
- Integration of the components of the Hadoop ecosystem into a single platform; and
- Higher degrees of operational control in production through enterprise best practices.

CDAP exposes developer APIs (Application Programming Interfaces) for creating applications
and accessing core CDAP services. CDAP defines and implements a diverse collection of
services that support applications and data on existing Hadoop infrastructure such as
HBase, HDFS, YARN, MapReduce, Hive, and Spark.

These documents are your complete reference to the Cask Data Application Platform: they help
you get started and set up your development environment; explain how CDAP works; and teach
how to develop and test CDAP applications.

It includes the CDAP programming APIs and client interfaces, with instructions
on the installation, monitoring and diagnosing fully distributed CDAP in a Hadoop cluster.
CDAP extensions provide additional capabilities, such as *Cask Hydrator* and *Cask Tracker*.

.. role:: link-black

.. default-role:: link-black

.. |introduction| replace:: **Introduction to CDAP:**
.. _introduction: introduction/index.html

.. |intro-link-black| replace:: `Introduction to CDAP:`
.. _intro-link-black: introduction/index.html

- |introduction|_

  - |intro-link-black|_ A comparison between using the current 
    technologies available from the Hadoop ecosystem and using CDAP


.. |developers-manual| replace:: **Developers’ Manual:**
.. _developers-manual: developers-manual/index.html

.. |dev-man-gsd-black| replace:: `Getting Started Developing:`
.. _dev-man-gsd-black: developers-manual/getting-started/index.html

.. |dev-man-o-black| replace:: `Overview:`
.. _dev-man-o-black: developers-manual/overview/index.html

.. |dev-man-bb-black| replace:: `Building Blocks:`
.. _dev-man-bb-black: developers-manual/building-blocks/index.html

.. |dev-man-s-black| replace:: `Security:`
.. _dev-man-s-black: developers-manual/security/index.html

.. |dev-man-tad-black| replace:: `Testing and Debugging:`
.. _dev-man-tad-black: developers-manual/testing/index.html

.. |dev-man-id-black| replace:: `Ingesting Data:`
.. _dev-man-id-black: developers-manual/ingesting-tools/index.html

.. |dev-man-at-black| replace:: `Advanced Topics:`
.. _dev-man-at-black: developers-manual/advanced/index.html

- |developers-manual|_

  - |dev-man-gsd-black|_ A quick, hands-on introduction to developing with CDAP
  - |dev-man-o-black|_ The overall architecture, abstractions, modes, and components behind CDAP
  - |dev-man-bb-black|_ The two core abstractions in CDAP: *Data* and *Applications*, and their components
  - |dev-man-s-black|_ Perimeter security, configuration and client authentication
  - |dev-man-tad-black|_ Test framework plus tools and practices for debugging your applications
  - |dev-man-id-black|_ Different techniques for ingesting data into CDAP
  - |dev-man-at-black|_ Best practices for CDAP development, class loading in CDAP, adding a custom logback


.. |admin-manual| replace:: **Administration Manual:**
.. _admin-manual: admin-manual/index.html

.. |admin-man-i-black| replace:: `Installation:`
.. _admin-man-i-black: admin-manual/installation/index.html

.. |admin-man-s-black| replace:: `Security:`
.. _admin-man-s-black: admin-manual/security.html

.. |admin-man-o-black| replace:: `Operations:`
.. _admin-man-o-black: admin-manual/operations/index.html

.. |admin-man-a-black| replace:: `Appendices:`
.. _admin-man-a-black: admin-manual/appendices/index.html

- |admin-manual|_ 

  - |admin-man-i-black|_ Putting CDAP into production, with installation, configuration and upgrading for
    different distributions
  - |admin-man-s-black|_ CDAP supports securing clusters using a perimeter security model
  - |admin-man-o-black|_ Logging, monitoring, metrics, runtime arguments, scaling instances, resource
    guarantees, transaction service maintenance, and introduces the CDAP UI
  - |admin-man-a-black|_ Covers the CDAP installation and security configuration files


.. |integrations| replace:: **Integrations:**
.. _integrations: integrations/index.html

.. |integ-man-c-black| replace:: `Cloudera:`
.. _integ-man-c-black: integrations/partners/cloudera/index.html

.. |integ-man-j-black| replace:: `JDBC:`
.. _integ-man-j-black: integrations/jdbc.html

.. |integ-man-o-black| replace:: `ODBC:`
.. _integ-man-o-black: integrations/odbc.html

.. |integ-man-p-black| replace:: `Pentaho:`
.. _integ-man-p-black: integrations/pentaho.html

.. |integ-man-s-black| replace:: `Squirrel:`
.. _integ-man-s-black: integrations/squirrel.html

- |integrations|_ 

  - |integ-man-c-black|_ Integrating CDAP into Cloudera, using Cloudera Manager, running interactive queries with Impala, and
    bridging CDAP Metadata with Cloudera's data management tool, Navigator
  - |integ-man-j-black|_ The CDAP JDBC driver, included with CDAP
  - |integ-man-o-black|_ The CDAP ODBC driver available for CDAP
  - |integ-man-p-black|_ *Pentaho Data Integration*, a business intelligence tool that can be used with CDAP
  - |integ-man-s-black|_ *SquirrelSQL*, a simple JDBC client that can be integrated with CDAP


.. |examples-manual| replace:: **Examples, Guides, and Tutorials:**
.. _examples-manual: examples-manual/index.html

.. |ex-man-e-black| replace:: `Examples:`
.. _ex-man-e-black: examples-manual/examples/index.html

.. |ex-man-htg-black| replace:: `How-To Guides:`
.. _ex-man-htg-black: examples-manual/how-to-guides/index.html

.. |ex-man-t-black| replace:: `Tutorials:`
.. _ex-man-t-black: examples-manual/tutorials/index.html

.. |ex-man-capr-black| replace:: `CDAP Apps and Packs Repository:`
.. _ex-man-capr-black: examples-manual/apps-packs.html

- |examples-manual|_

  - |ex-man-e-black|_ Included with the :ref:`CDAP SDK <getting-started-index>`, they range from a simple introductory to more elaborate examples
  - |ex-man-htg-black|_ Designed to be completed in 15-30 minutes, these guides provide quick, hands-on instructions
  - |ex-man-t-black|_ Designed to be completed in 2-3 hours, these tutorials provide deeper, in-context explorations
  - |ex-man-capr-black|_ data applications built using CDAP and useful building blocks for your data applications


.. |reference-manual| replace:: **Reference Manual:**
.. _reference-manual: reference-manual/index.html

.. |ref-man-clia-black| replace:: `Command Line Interface API:`
.. _ref-man-clia-black: reference-manual/cli-api.html

.. |ref-man-hra-black| replace:: `HTTP RESTful API:`
.. _ref-man-hra-black: reference-manual/cli-api.html

.. |ref-man-jca-black| replace:: `Java Client API:`
.. _ref-man-jca-black: reference-manual/java-client-api.html

.. |ref-man-j-black| replace:: `Javadocs:`
.. _ref-man-j-black: reference-manual/javadocs/index.html

.. |ref-man-tld-black| replace:: `Trademarks, Licenses, and Dependencies:`
.. _ref-man-tld-black: reference-manual/licenses/index.html

- |reference-manual|_ 

  - |ref-man-clia-black|_ Methods for interacting with a CDAP instance from within a shell
  - |ref-man-hra-black|_ HTTP interface for a multitude of purposes
  - |ref-man-jca-black|_ Methods for interacting with CDAP from external Java applications
  - |ref-man-j-black|_ The Java APIs for writing CDAP applications
  - |ref-man-tld-black|_ Trademark and License information for CDAP and lists of CDAP dependent packages


.. |faqs| replace:: **FAQs:**
.. _faqs: faqs/index.html

.. |faqs-gc-black| replace:: `General Questions`
.. _faqs-gc-black: faqs/general.html

.. |faqs-ci-black| replace:: `CDAP Installation`
.. _faqs-ci-black: faqs/cdap.html

.. |faqs-s-black| replace:: `Startup`
.. _faqs-s-black: faqs/cdap.html

.. |faqs-cm-black| replace:: `Cloudera Manager`
.. _faqs-cm-black: faqs/cloudera-manager.html

.. |faqs-h-black| replace:: `Hydrator`
.. _faqs-h-black: faqs/hydrator.html

.. |faqs-ar-black| replace:: `Additional Resources:`
.. _faqs-ar-black: faqs/index.html#additional-resources


- |faqs|_ Answers to questions you might have about CDAP

  - |faqs-gc-black|_
  - |faqs-ci-black|_ and |faqs-s-black|_ 
  - |faqs-cm-black|_
  - |faqs-h-black|_ 
  - |faqs-ar-black|_ Other resources for solving problems
  
..   - **Applications** 
..   - **User Interface** 
..   - **Databases and Transactions** 


.. |release-notes| replace:: **Release Notes:**
.. _release-notes: reference-manual/release-notes.html

- |release-notes|_ Notes for current and previous CDAP versions


.. |glossary| replace:: **Glossary:**
.. _glossary: reference-manual/glossary.html

- |glossary|_ Definitions of terms and phrases used in CDAP


.. |search| replace:: **Search:**
.. _search: search.html

- |search|_ Search this documentation using *Quick Search*


.. |cdap-extensions-black| replace:: `CDAP Extensions:`
.. _cdap-extensions-black: index.html#cdap-extensions

.. |cask-hydrator| replace:: **Cask Hydrator:**
.. _cask-hydrator: hydrator-manual/index.html

.. |cask-tracker| replace:: **Cask Tracker:**
.. _cask-tracker: tracker-manual/index.html

.. _cdap-extensions:

- |cdap-extensions-black|_

  - |cask-hydrator|_ Creating data pipelines without writing code, including details on
    developing and packaging custom plugins

  - |cask-tracker|_ A CDAP extension that captures metadata for datasets, streams, and
    stream views

