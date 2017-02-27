.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

:hide-toc: true

.. _building-blocks:

===============
Building Blocks
===============

.. toctree::
   :maxdepth: 1
   
    Core Abstractions <core>
    Applications <applications>
    Streams <streams>
    Datasets <datasets/index>
    Views <views>
    Flows and Flowlets <flows-flowlets/index>
    MapReduce Programs<mapreduce-programs>
    Plugins <plugins>
    Schedules <schedules>
    Secure Keys <secure-keys>
    Services <services>
    Spark Programs <spark-programs>
    Workers <workers>
    Workflows <workflows>
    Artifacts <artifacts>
    Program Lifecycle <program-lifecycle>
    Namespaces <namespaces>
    Transaction System <transaction-system>
    Transactional Messaging System <transactional-messaging-system>

.. toctree::
   :hidden:

   mapreduce-jobs
   spark-jobs

This section covers the :doc:`core abstractions <core>` in the Cask Data Application Platform
(CDAP): **Data** and **Applications.**

An :doc:`Application <applications>` is a collection of application building blocks that read and
write data through the data abstraction layer in CDAP.

**Data abstractions** include:

- :doc:`Streams <streams>`
- :doc:`Datasets <datasets/index>`
- :doc:`Views <views>`

**Applications** are composed from these program building blocks:

- :doc:`Flows and Flowlets <flows-flowlets/index>`
- :doc:`MapReduce Programs <mapreduce-programs>`
- :doc:`Plugins <plugins>`
- :doc:`Schedules <schedules>`
- :doc:`Services <services>`
- :doc:`Spark Programs <spark-programs>`
- :doc:`Workers <workers>`
- :doc:`Workflows <workflows>`

**Additional abstractions** include:

- An :doc:`Artifact <artifacts>` is a jar file that packages the Java Application class, as well
  as any other classes and libraries needed to create and run an Application. 

- All of the program building blocks follow a :doc:`Program Lifecycle <program-lifecycle>`.

- :ref:`Metadata <metadata>` |---| consisting of **properties** (a list of key-value pairs)
  or **tags** (a list of keys) |---| can be set for artifacts, applications, programs, datasets, streams, and views.
  These can be retrieved and searched, and the metadata used to discover CDAP entities.
  Access of these entities is tracked, and you can view the :ref:`lineage <metadata-lineage>` of datasets and streams.
  With a lineage diagram, you can then drill down into the metadata of its nodes. 

- :ref:`Audit Logging <audit-logging>` provides a chronological ledger containing evidence of operations or
  changes on CDAP entities. This information can be used to capture a trail of the activities that 
  determined the state of an entity at a given point in time.

- A :doc:`Namespace <namespaces>` is a logical grouping of application and data in CDAP.
  Conceptually, namespaces can be thought of as a partitioning of a CDAP instance.
  All applications and data live in an explicit CDAP namespace.

- The :doc:`Transaction System <transaction-system>` is an essential service
  that provides ACID (*atomicity, consistency, isolation,* and *durability*) guarantees,
  critical in applications where data accuracy is required.

- The :doc:`Transactional Messaging System <transactional-messaging-system>` is a CDAP service
  that provides a "publish-and-subscribe" messaging system that understands transactions.
  
- :doc:`Secure Keys <secure-keys>` allows users to store and retrieve sensitive information such
  as passwords from secure and encrypted storage.

For a **high-level view of the concepts** of the Cask Data Application Platform, see the
platform :doc:`overview. </overview/index>`

For information beyond this section, see the :ref:`Javadocs <reference:javadocs>` and
the code in the :ref:`examples <examples-index>` directory, both of which are available at the
`Cask.co resources page, <http://cask.co/resources>`_ as well as in your CDAP SDK
installation directory.
