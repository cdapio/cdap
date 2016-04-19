.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _admin-manual-hadoop-compatibility-matrix:

====================
Hadoop Compatibility
====================

Before installing the CDAP components, you must first install (or have access to) a Hadoop
cluster with *HBase*, *HDFS*, *YARN*, and *ZooKeeper*. *Hive* and *Spark* are optional
components; Hive is required to enable CDAP's ad-hoc querying capabilities (*CDAP
Explore*) and Spark is required if a CDAP application uses the Spark program.

All CDAP components can be installed on the same boxes as your Hadoop cluster, or on
separate boxes that can connect to the Hadoop services.

CDAP depends on these services being present on the cluster. There are **core
dependencies,** which must be running for CDAP system services to operate correctly, and
**optional dependencies,** which may be required for certain functionality or program types.

The host(s) running the CDAP Master service must have the HBase, HDFS, and YARN clients
installed, as CDAP uses the command line clients of these for initialization and their
connectivity information for external service dependencies. If Hadoop system services are
also running on the same hosts as the CDAP services, they will already have these clients
installed.

CDAP currently requires Internet access on the CDAP service nodes (or until the issue
`CDAP-3957 <https://issues.cask.co/browse/CDAP-3957>` is resolved).

**Core Dependencies**

- **HBase:** For system runtime storage and queues
- **HDFS:** The backing file system for distributed storage
- **YARN:** For running system services in containers on cluster NodeManagers
- **MapReduce2:** For batch operations in workflows and data exploration (included with YARN)
- **ZooKeeper:** For service discovery and leader election

**Optional Dependencies**

- **Hive:** For data exploration using SQL queries via the CDAP Explore system service
- **Spark:** For running Spark programs within CDAP applications


.. rubric:: Hadoop/HBase Environment

For a Distributed CDAP cluster, version |release|, you **must** install these Hadoop components:

.. |cdh-version|       replace:: 5.0.0 through 5.7.0
.. |cdh-version-hbase| replace:: 5.0.0 through 5.7.0 (1.2-cdh5.7.0)
.. |hdp-version|       replace:: 2.0 through 2.4
.. |mapr-version|      replace:: 4.1 through 5.1

+----------------+---------------------------------+---------------------------------------+
| Component      | Source                          | Supported Versions                    |
+================+=================================+=======================================+
| **Hadoop**     | *various*                       | 2.0 and higher                        |
+----------------+---------------------------------+---------------------------------------+
| **HBase**      | Apache                          | 0.96.x, 0.98.x, and 1.0               |
+                +---------------------------------+---------------------------------------+
|                | Cloudera Data Hub (CDH)         | |cdh-version-hbase|                   |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-version|                         |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-version| (with Apache HBase)    |
+----------------+---------------------------------+---------------------------------------+
| **HDFS**       | Apache Hadoop                   | 2.0.2-alpha through 2.6.0             |
+                +---------------------------------+---------------------------------------+
|                | Cloudera Data Hub (CDH)         | |cdh-version|                         |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-version|                         |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-version| (with MapR-FS)         |
+----------------+---------------------------------+---------------------------------------+
| **YARN** and   | Apache Hadoop                   | 2.0.2-alpha through 2.6.0             |
+ **MapReduce2** +---------------------------------+---------------------------------------+
|                | Cloudera Data Hub (CDH)         | |cdh-version|                         |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-version|                         |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-version|                        |
+----------------+---------------------------------+---------------------------------------+
| **ZooKeeper**  | Apache                          | Version 3.4.3 through 3.4.5           |
+                +---------------------------------+---------------------------------------+
|                | Cloudera Data Hub (CDH)         | |cdh-version|                         |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-version|                         |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-version|                        |
+----------------+---------------------------------+---------------------------------------+

For a Distributed CDAP cluster, version |release|, you *can* (optionally) install these
Hadoop components, as required:

+----------------+---------------------------------+---------------------------------------+
| Component      | Source                          | Supported Versions                    |
+================+=================================+=======================================+
| **Hive**       | Apache                          | Version 0.12.0 through 1.1.0          |
+                +---------------------------------+---------------------------------------+
|                | Cloudera Data Hub (CDH)         | |cdh-version|                         |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-version|                         |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-version|                        |
+----------------+---------------------------------+---------------------------------------+
| **Spark**      | Apache                          | Versions 1.2.x through 1.6.x          |
+                +---------------------------------+---------------------------------------+
|                | Cloudera Data Hub (CDH)         | |cdh-version|                         |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-version|                         |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-version|                        |
+----------------+---------------------------------+---------------------------------------+

**Note:** Component versions shown in these tables are those that we have tested and are
confident of their suitability and compatibility. Later versions of components may work,
but have not necessarily have been either tested or confirmed compatible.

**Note:** Certain CDAP components need to reference your *Hadoop*, *YARN*, *HBase*, and
*Hive* cluster configurations by adding those configurations to their class paths.

**Note:** *Hive 0.12* is not supported for :ref:`secure cluster configurations <admin-security>`.
