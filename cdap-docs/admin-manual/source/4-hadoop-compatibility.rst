.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _admin-manual-hadoop-compatibility-matrix:

========================
Hadoop/HBase Environment
========================

For a Distributed CDAP cluster, version |release|, you must install these Hadoop components:

+---------------+-------------------+-----------------------------------------------------+
| Component     | Source            | Supported Versions                                  |
+===============+===================+=====================================================+
| **HDFS**      | Apache Hadoop     | 2.0.2-alpha through 2.6.0                           |
+               +-------------------+-----------------------------------------------------+
|               | CDH               | 5.0.0 through 5.4.x                                 |
+               +-------------------+-----------------------------------------------------+
|               | HDP               | 2.0 through 2.3                                     |
+               +-------------------+-----------------------------------------------------+
|               | MapR              | 4.1 and 5.0 (with MapR-FS)                          |
+---------------+-------------------+-----------------------------------------------------+
| **YARN**      | Apache Hadoop     | 2.0.2-alpha through 2.6.0                           |
+               +-------------------+-----------------------------------------------------+
|               | CDH               | 5.0.0 through 5.4.x                                 |
+               +-------------------+-----------------------------------------------------+
|               | HDP               | 2.0 through 2.3                                     |
+               +-------------------+-----------------------------------------------------+
|               | MapR              | 4.1 and 5.0                                         |
+---------------+-------------------+-----------------------------------------------------+
| **HBase**     | Apache            | 0.96.x, 0.98.x, and 1.0.x                           |
+               +-------------------+-----------------------------------------------------+
|               | CDH               | 5.0.0 through 5.4.x                                 |
+               +-------------------+-----------------------------------------------------+
|               | HDP               | 2.0 through 2.3                                     |
+               +-------------------+-----------------------------------------------------+
|               | MapR              | 4.1 and 5.0 (with Apache HBase)                     |
+---------------+-------------------+-----------------------------------------------------+
| **ZooKeeper** | Apache            | Version 3.4.3 through 3.4.5                         |
+               +-------------------+-----------------------------------------------------+
|               | CDH               | 5.0.0 through 5.4.x                                 |
+               +-------------------+-----------------------------------------------------+
|               | HDP               | 2.0 through 2.3                                     |
+               +-------------------+-----------------------------------------------------+
|               | MapR              | 4.1 and 5.0                                         |
+---------------+-------------------+-----------------------------------------------------+
| **Hive**      | Apache            | Version 0.12.0 through 0.13.1                       |
+               +-------------------+-----------------------------------------------------+
|               | CDH               | 5.0.0 through 5.4.x                                 |
+               +-------------------+-----------------------------------------------------+
|               | HDP               | 2.0 through 2.3                                     |
+               +-------------------+-----------------------------------------------------+
|               | MapR              | 4.1 and 5.0                                         |
+---------------+-------------------+-----------------------------------------------------+

**Note:** Component versions shown in this table are those that we have tested and are
confident of their suitability and compatibility. Later versions of components may work,
but have not necessarily have been either tested or confirmed compatible.

**Note:** Certain CDAP components need to reference your *Hadoop*, *HBase*, *YARN* (and
possibly *Hive*) cluster configurations by adding your configuration to their class paths.

**Note:** ZooKeeper's ``maxClientCnxns`` must be raised from its default.  We suggest setting it to zero
(unlimited connections). As each YARN container launched by CDAP makes a connection to ZooKeeper, 
the number of connections required is a function of usage.

**Note:** *Hive 0.12* is not supported for :ref:`secure cluster configurations <admin-security>`.
