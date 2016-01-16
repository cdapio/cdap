.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _admin-manual-hadoop-compatibility-matrix:

====================
Hadoop Compatibility
====================

Before installing the CDAP components, you must first install (or have access to) a Hadoop
cluster with *HDFS*, *YARN*, *HBase*, and *ZooKeeper*. In order to use the ad-hoc querying
capabilities of CDAP (Explore), you will also need *Hive*. All CDAP components can be installed on
the same boxes as your Hadoop cluster, or on separate boxes that can connect to the Hadoop
services.

CDAP depends on these services being present on the cluster. There are **core
dependencies,** which must be running for CDAP system services to operate correctly, and
**optional dependencies,** which may be required for certain functionality or program types.

The host(s) running the CDAP Master service must have the HDFS, YARN, and HBase clients
installed as CDAP uses the command line clients of these for initialization and their
connectivity information for external service dependencies. Also, CDAP currently requires
Internet access on the CDAP service nodes (or until the issue `CDAP-3957
<https://issues.cask.co/browse/CDAP-3957>` is resolved).

**Core Dependencies**

- **HDFS:** The backing file system for distributed storage
- **YARN:** For running system services in containers on cluster NodeManagers
- **HBase:** For system runtime storage and queues
- **MapReduce2:** For batch operations in workflows and data exploration
- **ZooKeeper:** For service discovery and leader election

**Optional Dependencies**

- **Hive:** For data exploration using SQL queries via the CDAP Explore system service
- **Spark:** For running Spark programs within CDAP applications


.. rubric:: Hadoop/HBase Environment

For a Distributed CDAP cluster, version |release|, you must install these Hadoop components:

+---------------+---------------------------------+---------------------------------------+
| Component     | Source                          | Supported Versions                    |
+===============+=================================+=======================================+
| **HDFS**      | Apache Hadoop                   | 2.0.2-alpha through 2.6.0             |
+               +---------------------------------+---------------------------------------+
|               | Cloudera Data Hub (CDH)         | 5.0.0 through 5.5.0                   |
+               +---------------------------------+---------------------------------------+
|               | Hortonworks Data Platform (HDP) | 2.0 through 2.3                       |
+               +---------------------------------+---------------------------------------+
|               | MapR                            | 4.1 and 5.0 (with MapR-FS)            |
+---------------+---------------------------------+---------------------------------------+
| **YARN**      | Apache Hadoop                   | 2.0.2-alpha through 2.6.0             |
+               +---------------------------------+---------------------------------------+
|               | Cloudera Data Hub (CDH)         | 5.0.0 through 5.5.0                   |
+               +---------------------------------+---------------------------------------+
|               | Hortonworks Data Platform (HDP) | 2.0 through 2.3                       |
+               +---------------------------------+---------------------------------------+
|               | MapR                            | 4.1 and 5.0                           |
+---------------+---------------------------------+---------------------------------------+
| **HBase**     | Apache                          | 0.96.x, 0.98.x, and 1.0               |
+               +---------------------------------+---------------------------------------+
|               | Cloudera Data Hub (CDH)         | 5.0.0 through 5.5.0 (1.0-cdh5.5.0)    |
+               +---------------------------------+---------------------------------------+
|               | Hortonworks Data Platform (HDP) | 2.0 through 2.3                       |
+               +---------------------------------+---------------------------------------+
|               | MapR                            | 4.1 and 5.0 (with Apache HBase)       |
+---------------+---------------------------------+---------------------------------------+
| **ZooKeeper** | Apache                          | Version 3.4.3 through 3.4.5           |
+               +---------------------------------+---------------------------------------+
|               | Cloudera Data Hub (CDH)         | 5.0.0 through 5.5.0                   |
+               +---------------------------------+---------------------------------------+
|               | Hortonworks Data Platform (HDP) | 2.0 through 2.3                       |
+               +---------------------------------+---------------------------------------+
|               | MapR                            | 4.1 and 5.0                           |
+---------------+---------------------------------+---------------------------------------+
| **Hive**      | Apache                          | Version 0.12.0 through 1.1.0          |
+               +---------------------------------+---------------------------------------+
|               | Cloudera Data Hub (CDH)         | 5.0.0 through 5.5.0                   |
+               +---------------------------------+---------------------------------------+
|               | Hortonworks Data Platform (HDP) | 2.0 through 2.3                       |
+               +---------------------------------+---------------------------------------+
|               | MapR                            | 4.1 and 5.0                           |
+---------------+---------------------------------+---------------------------------------+

**Note:** Component versions shown in this table are those that we have tested and are
confident of their suitability and compatibility. Later versions of components may work,
but have not necessarily have been either tested or confirmed compatible.

**Note:** Certain CDAP components need to reference your *Hadoop*, *YARN*, *HBase*, and
*Hive* cluster configurations by adding those configurations to their class paths.

**Note:** *Hive 0.12* is not supported for :ref:`secure cluster configurations <admin-security>`.

To be moved:

**Note:** ZooKeeper's ``maxClientCnxns`` must be raised from its default.  We suggest setting it to zero
(unlimited connections). As each YARN container launched by CDAP makes a connection to ZooKeeper, 
the number of connections required is a function of usage.
