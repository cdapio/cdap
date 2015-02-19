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
    Workflows <workflows>
    Schedules <schedules>
    Spark Programs <spark-programs>
    Procedures (Deprecated) <procedures>
    Services <services>
    Namespaces <namespaces>
    Transaction System <transaction-system>

This section covers the :doc:`core abstractions <core>` in the Cask Data Application Platform
(CDAP)—**Data** and **Applications.**

An :doc:`Application <applications>` is a collection of application building blocks that read—and
write—through the data abstraction layer in CDAP.

**Data abstractions** include:

- :doc:`Streams <streams>`
- :doc:`Datasets <datasets/index>`

**Applications** are composed from these building blocks:

- :doc:`Flows (and Flowlets) <flows-flowlets/index>`
- :doc:`MapReduce Programs <mapreduce-programs>`
- :doc:`Workflows <workflows>`
- :doc:`Schedules <schedules>`
- :doc:`Spark Programs <spark-programs>`
- :doc:`Procedures (Deprecated) <procedures>`
- :doc:`Services <services>`

A :doc:`Namespace <namespaces>` is a physical grouping of application and data in CDAP.
Conceptually, namespaces can be thought of as a partitioning of a CDAP instance.
All applications and data live in an explicit CDAP namespace.

Additionally, the :doc:`Transaction System <transaction-system>` is an essential service
that provides ACID (*atomicity, consistency, isolation,* and *durability*) guarantees,
critical in applications where data accuracy is required.

For a high-level view of the concepts of the Cask Data Application Platform, see the
platform :doc:`Overview. </overview/index>`

For information beyond this section, see the :ref:`Javadocs <reference:javadocs>` and
the code in the :ref:`Examples <examples-index>` directory, both of which are available at the
`Cask.co resources page, <http://cask.co/resources>`_ as well as in your CDAP SDK
installation directory.
