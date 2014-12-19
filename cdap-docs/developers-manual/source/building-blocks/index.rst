.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

:hide-toc: true

.. _building-blocks:

============================================
Building Blocks
============================================

.. toctree::
   :maxdepth: 1
   
    Core Virtualizations <core>
    Applications <applications>
    Streams <streams>
    Datasets <datasets/index>
    Flows and Flowlets <flows-flowlets/index>
    MapReduce Jobs<mapreduce-jobs>
    Workflows <workflows>
    Spark Programs <spark-programs>
    Procedures <procedures>
    Services <services>
    Transaction System <transaction-system>

This section covers the :doc:`core virtualizations <core>` in the Cask Data Application Platform
(CDAP)—**Data** and **Applications.**

An :doc:`Application <applications>` is a collection of application virtualizations that read from—and
write to—the data virtualization layer in CDAP.

**Data virtualizations** include:

- :doc:`Streams <streams>`
- :doc:`Datasets <datasets/index>`

**Application virtualizations** include:

- :doc:`Flows (and Flowlets) <flows-flowlets/index>`
- :doc:`MapReduce Jobs <mapreduce-jobs>`
- :doc:`Workflows <workflows>`
- :doc:`Spark Programs <spark-programs>`
- :doc:`Procedures <procedures>`
- :doc:`Services <services>`

Additionally, the :doc:`Transaction System <transaction-system>` is an essential service
that provides ACID (*atomicity, consistency, isolation,* and *durability*) guarantees,
critical in applications where data accuracy is required.

For a high-level view of the concepts of the Cask Data Application Platform, see the
platform :doc:`Overview. </overview/index>`

For information beyond this section, see the :ref:`Javadocs <reference:javadocs>` and
the code in the :ref:`Examples <examples-index>` directory, both of which are available at the
`Cask.co resources page, <http://cask.co/resources>`_ as well as in your CDAP SDK
installation directory.
