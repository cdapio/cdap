.. meta::
    :author: Cask Data, Inc.
    :description: Introduction to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-relations: true
:hide-global-toc: true
:link-only-global-toc: true

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
and accessing core CDAP services. CDAP defines and implements a diverse collection of
services that support applications and data on existing Hadoop infrastructure such as
HBase, HDFS, YARN, MapReduce, Hive, and Spark.

CDAP can be run in different modes: in-memory mode for unit testing and continuous
integration pipelines, standalone CDAP for testing and development, or
distributed CDAP for staging and production. Regardless of the runtime mode, CDAP is
fully-functional and the code you develop never changes.

These documents are your complete reference to the Cask Data Application Platform: they help
you get started and set up your development environment; explain how CDAP works; and teach
how to develop and test CDAP applications.

It includes the CDAP programming APIs and client interfaces, with instructions
on the installation, monitoring and diagnosing fully distributed CDAP in a Hadoop cluster.

.. _modes-data-application-platform:

.. .. rubric:: Putting CDAP into Production

Putting CDAP into Production
============================
The Cask Data Application Platform (CDAP) can be run in different modes: **In-memory CDAP**
for unit testing and continuous integration pipelines, **Standalone CDAP** for testing and
development on a developer's laptop, or **Distributed CDAP** for staging and production.

Regardless of the runtime mode, CDAP is fully-functional and the code you develop never
changes. However, performance and scale are limited when using in-memory or standalone
CDAPs. CDAP Applications are developed against the CDAP APIs, making the switch between
these modes seamless. An application developed using a given mode can easily run in
another mode.


.. _in-memory-data-application-platform:

.. rubric:: In-Memory CDAP

The **In-Memory CDAP** allows you to easily run CDAP for use in unit tests and continuous
integration (CI) pipelines. In this mode, the underlying Big Data infrastructure is
emulated using in-memory data structures and there is no persistence. The CDAP UI is not
available in this mode. 

See :ref:`test-cdap` for information and examples on using this mode.

**Features:**

- Purpose-built for writing unit tests and CI pipelines
- Mimics storage technologies as in-memory data structures; for example, 
  `Java NavigableMap <http://docs.oracle.com/javase/7/docs/api/java/util/NavigableMap.html>`__
- Uses Java Threads as the processing abstraction (via Apache Twill)


.. _standalone-data-application-platform:

.. rubric:: Standalone CDAP

The **Standalone CDAP** allows you to run the entire CDAP stack in a single Java Virtual
Machine on your local machine and includes a local version of the CDAP UI. The
underlying Big Data infrastructure is emulated on top of your local file system. All data
is persisted.

The Standalone CDAP by default binds to the localhost address, and is not available for
remote access by any outside process or application outside of the local machine.

See :ref:`Getting Started <getting-started-index>` and the *Cask Data Application Platform
SDK* for information on how to start and manage your Standalone CDAP.

**Features:**

- Designed to run in a standalone environment, for development and testing
- Uses LevelDB/Local File System as the storage technology
- Uses Java Threads as the processing abstraction (via Apache Twill)


.. _distributed-data-application-platform:

.. rubric:: Distributed Data Application Platform

The **Distributed CDAP** runs in fully distributed mode. In addition to the system components
of the CDAP, distributed and highly available (HA) deployments of the underlying Hadoop
infrastructure are included. Production applications should always be run on a Distributed
CDAP.

See the instructions for either a :ref:`distribution-specific <installation-index>` or 
:ref:`generic Apache Hadoop <hadoop-index>` cluster for more information.

**Features:**

- A production, staging, and QA mode; runs in a distributed environment
- Currently uses Apache HBase and HDFS as the underlying storage technology
- Uses Apache Yarn Containers as the processing abstraction (via Apache Twill)
