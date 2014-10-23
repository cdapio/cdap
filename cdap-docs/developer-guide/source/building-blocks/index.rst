.. :author: Cask Data, Inc.
   :copyright: Copyright © 2014 Cask Data, Inc.

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
(CDAP)—Data and Applications. Data virtualizations are grouped into Streams and
Datasets. Application virtualizations are grouped into Flows, MapReduce, Spark, Workflows,
and Services.  

The Transaction System is an essential service that provides ACID (atomicity,
consistency, isolation, and durability) guarantees, useful in applications where data
accuracy is critical.


.. image:: ../_images/architecture_diag.png
   :width: 5in
   :align: center

For a high-level view of the concepts of the Cask Data Application Platform,
please see the platform :doc:`Overview. </overview/index>`

For more information beyond this section, see the :ref:`Javadocs <reference:javadocs>` and
the code in the :ref:`Examples <examples-index>` directory, both of which are on the `Cask.co
<http://cask.co>`_ `resources page <http://cask.co/resources>`_ as well as in your CDAP
installation directory.
