.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _building-blocks:

============================================
Building Blocks
============================================

.. toctree::
   :maxdepth: 1
   
    Core Abstractions <core>
    Applications <applications>
    Streams <streams>
    Datasets <datasets/index>
    Flows and Flowlets <flows-flowlets/index>
    MapReduce Programs<mapreduce-programs>
    Schedules <schedules>
    Services <services>
    Spark Programs <spark-programs>
    Workers <workers>
    Workflows <workflows>
    Plugins <plugins>
    Artifacts <artifacts>
    Metadata and Lineage <metadata-lineage>
    Namespaces <namespaces>
    Transaction System <transaction-system>

This section covers the :doc:`core abstractions <core>` in the Cask Data Application Platform
(CDAP): **Data** and **Applications.**

An :doc:`Application <applications>` is a collection of application building blocks that read and
write data through the data abstraction layer in CDAP.

**Data abstractions** include:

- :doc:`Streams <streams>`
- :doc:`Datasets <datasets/index>`

**Applications** are composed from these building blocks:

- :doc:`Flows (and flowlets) <flows-flowlets/index>`
- :doc:`MapReduce Programs <mapreduce-programs>`
- :doc:`Schedules <schedules>`
- :doc:`Services <services>`
- :doc:`Spark Programs <spark-programs>`
- :doc:`Workers <workers>`
- :doc:`Workflows <workflows>`
- :doc:`Plugins <plugins>`

An :doc:`Artifact <artifacts>` is a jar file that packages the Java Application class, as well
as any other classes and libraries needed to create and run an Application. 

:doc:`Metadata <metadata-lineage>` |---| consisting of **properties** (a list of key-value pairs)
or **tags** (a list of keys) |---| can be set for datasets, streams, and applications.
These can be retrieved and searched, and the metadata used to discover CDAP entities.
Access of these entities is tracked, and you can view the :doc:`lineage <metadata-lineage>` of datasets and streams.
With a lineage diagram, you can then drill down into the metadata of its nodes. 

A :doc:`Namespace <namespaces>` is a logical grouping of application and data in CDAP.
Conceptually, namespaces can be thought of as a partitioning of a CDAP instance.
All applications and data live in an explicit CDAP namespace.

Additionally, the :doc:`Transaction System <transaction-system>` is an essential service
that provides ACID (*atomicity, consistency, isolation,* and *durability*) guarantees,
critical in applications where data accuracy is required.

For a high-level view of the concepts of the Cask Data Application Platform, see the
platform :doc:`overview. </overview/index>`

For information beyond this section, see the :ref:`Javadocs <reference:javadocs>` and
the code in the :ref:`examples <examples-index>` directory, both of which are available at the
`Cask.co resources page, <http://cask.co/resources>`_ as well as in your CDAP SDK
installation directory.
