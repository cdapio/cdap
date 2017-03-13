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

.. _admin-manual-hadoop-compatibility-matrix-required:

For a Distributed CDAP cluster, version |release|, you **must** install these Hadoop components
(see notes following the tables):

.. |cdh-title| replace:: Cloudera Distribution of Apache Hadoop (CDH)
.. |note-4| replace:: :ref:`(Note 4) <admin-manual-hadoop-compatibility-matrix-note-4>`

+----------------+---------------------------------+---------------------------------------+
| Component      | Source                          | Supported Versions                    |
+================+=================================+=======================================+
| **Hadoop**     | *various*                       | 2.0 and higher                        |
+----------------+---------------------------------+---------------------------------------+
| **HBase**      | Apache                          | 0.98.x and 1.2                        |
+                +---------------------------------+---------------------------------------+
|                | |cdh-title|                     | |cdh-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-versions| (with Apache HBase)   |
+                +---------------------------------+---------------------------------------+
|                | Amazon Hadoop (EMR)             | |emr-versions| (with Apache HBase)    |
+----------------+---------------------------------+---------------------------------------+
| **HDFS**       | Apache Hadoop                   | 2.0.2-alpha through 2.6               |
+                +---------------------------------+---------------------------------------+
|                | |cdh-title|                     | |cdh-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-versions| (with MapR-FS)        |
+                +---------------------------------+---------------------------------------+
|                | Amazon Hadoop (EMR)             | |emr-versions|                        |
+----------------+---------------------------------+---------------------------------------+
| **YARN** and   | Apache Hadoop                   | 2.0.2-alpha through 2.7               |
+ **MapReduce2** +---------------------------------+---------------------------------------+
|                | |cdh-title|                     | |cdh-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-versions|                       |
+                +---------------------------------+---------------------------------------+
|                | Amazon Hadoop (EMR)             | |emr-versions|                        |
+----------------+---------------------------------+---------------------------------------+
| **ZooKeeper**  | Apache                          | Version 3.4.3 through 3.4             |
+                +---------------------------------+---------------------------------------+
|                | |cdh-title|                     | |cdh-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-versions|                       |
+                +---------------------------------+---------------------------------------+
|                | Amazon Hadoop (EMR)             | |emr-versions|                        |
+----------------+---------------------------------+---------------------------------------+

.. _admin-manual-hadoop-compatibility-matrix-optional:

For a Distributed CDAP cluster, version |release|, you *can* (optionally) install these
Hadoop components, as required:

+----------------+---------------------------------+---------------------------------------+
| Component      | Source                          | Supported Versions                    |
+================+=================================+=======================================+
| **Hive**       | Apache                          | Version 0.12.0 through 1.2.x          |
+                +---------------------------------+---------------------------------------+
|                | |cdh-title|                     | |cdh-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-versions|                       |
+                +---------------------------------+---------------------------------------+
|                | Amazon Hadoop (EMR)             | |emr-versions|                        |
+----------------+---------------------------------+---------------------------------------+
| **Spark**      | Apache                          | Versions 1.2.x through 1.6.x          |
+                +---------------------------------+---------------------------------------+
|                | |cdh-title|                     | |cdh-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | Hortonworks Data Platform (HDP) | |hdp-versions| |note-4|               |
+                +---------------------------------+---------------------------------------+
|                | MapR                            | |mapr-versions|                       |
+                +---------------------------------+---------------------------------------+
|                | Amazon Hadoop (EMR)             | |emr-versions|                        |
+----------------+---------------------------------+---------------------------------------+

**Note 1:** Component versions shown in these tables are those that we have tested and are
confident of their suitability and compatibility. Later versions of components may work,
but have not necessarily been either tested or confirmed compatible.

**Note 2:** Certain CDAP components need to reference your *Hadoop*, *YARN*, *HBase*, and
*Hive* cluster configurations by adding those configurations to their class paths.

**Note 3:** *Hive 0.12* is not supported for :ref:`secure cluster configurations <admin-security>`.

.. _admin-manual-hadoop-compatibility-matrix-note-4:

**Note 4:** An upcoming release of CDAP (scheduled for CDAP 4.3) will drop support for all
versions older than `CDH 5.4.11
<https://www.cloudera.com/documentation/other/security-bulletins/topics/csb_all_product_issues.html#tsb_175>`__
or `HDP 2.5.0.0 
<http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.5.0/bk_release-notes/content/ch_relnotes_v250.html>`__
due to an `Apache Hadoop Privilege Escalation Vulnerability 
<https://web.nvd.nist.gov/view/vuln/detail?vulnId=CVE-2016-5393>`__.
