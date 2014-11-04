.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _building-blocks:

============================================
Building Blocks
============================================

.. toctree::
   :maxdepth: 1
   
    Core Virtualizations <core>
    Application <applications>
    Streams <streams>
    Datasets <datasets/index>
    Flows and Flowlets <flows-flowlets/index>
    MapReduce Jobs<mapreduce-jobs>
    Workflows <workflows>
    Spark Jobs <spark-jobs>
    Procedures <procedures>
    Services <services>
    Transaction System <transaction-system>

This section covers the two core virtualizations in the Cask Data Application Platform
(CDAP)—Data and Applications. 

**Data virtualizations** are grouped into Streams and Datasets. 

**Application virtualizations** are grouped into Flows (and Flowlets), MapReduce Jobs,
Workflows, Spark Jobs, Procedures, and Services.  

The **Transaction System** is an essential service that provides ACID (atomicity,
consistency, isolation, and durability) guarantees, critical in applications where data
accuracy is required.

This diagram gives an outline of these components, and how they and other CDAP components
relate in a Hadoop installation: 

.. image:: ../_images/architecture_diag.png
   :width: 7in
   :align: center

For a high-level view of the concepts of the Cask Data Application Platform,
please see the platform :doc:`Overview. </overview/index>`

For information beyond this section, see the :ref:`Javadocs <reference:javadocs>` and
the code in the :ref:`Examples <examples-index>` directory, both of which are available at the
`Cask.co resources page, <http://cask.co/resources>`_ as well as in your CDAP SDK
installation directory.
