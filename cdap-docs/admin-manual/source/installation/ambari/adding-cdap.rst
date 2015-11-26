.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.
    
.. _ambari-adding-cdap:

===========================
Adding CDAP to Your Cluster
===========================

Dependencies
============
CDAP depends on certain services being present on the cluster. There are **core
dependencies,** which must be running for CDAP system services to operate correctly, and
**optional dependencies,** which may be required for certain functionality or program types.

The host running the CDAP Master service must have the HDFS, YARN, and HBase clients
installed, as CDAP uses the command line clients of these for initialization and their
connectivity information for external service dependencies. Also, CDAP currently requires
Internet access on the CDAP service nodes until the issues `CDAP-3957
<https://issues.cask.co/browse/CDAP-3957>`__ or `AMBARI-13456
<https://issues.apache.org/jira/browse/AMBARI-13456>`__ are resolved.

Core Dependencies
=================
- **HDFS:** used as the backing file system for distributed storage
- **MapReduce2:** used for batch operations in workflows and data exploration
- **YARN:** used for running system services in containers on cluster NodeManagers
- **HBase:** used for system runtime storage and queues
- **ZooKeeper:** used for service discovery and leader election

Optional Dependencies
=====================
- **Hive:** used for data exploration using SQL queries via CDAP Explore system service
- **Spark:** used for running Spark programs within CDAP applications
