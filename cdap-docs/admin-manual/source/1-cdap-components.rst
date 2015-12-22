.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _admin-manual-cdap-components:

===============
CDAP Components
===============

These are the CDAP components:

- **CDAP Master:** Service for managing runtime, lifecycle and resources of CDAP applications (package *cdap-master*);
- **CDAP Router:** Service supporting REST endpoints for CDAP (package *cdap-gateway*);
- **CDAP UI:** User interface for managing CDAP applications (package *cdap-ui*);
- **CDAP Kafka:** Metrics and logging transport service, using an embedded version of *Kafka* (package *cdap-kafka*); and
- **CDAP Authentication Server:** Performs client authentication for CDAP when security is enabled (package *cdap-security*).

Before installing the CDAP components, you must first install (or have access to) a Hadoop cluster
with *HDFS*, *YARN*, *HBase*, and *ZooKeeper*. In order to use the ad-hoc querying capabilities
of CDAP, you will also need *Hive*. All CDAP components can be installed on the
same boxes as your Hadoop cluster, or on separate boxes that can connect to the Hadoop services.

Our recommended installation is to use two boxes for the CDAP components; the
`hardware requirements <#hardware-requirements>`__ are relatively modest,
as most of the work is done by the Hadoop cluster. These two
boxes provide high availability; at any one time, one of them is the leader
providing services while the other is a follower providing failover support.
The section :ref:`admin-manual-install-deployment-architectures` 
illustrates both a minimal (single host) and the high availability deployments.

Some CDAP components run on YARN, while others orchestrate “containers” in the Hadoop cluster.
The CDAP Router service starts a router instance on each of the local boxes and instantiates
one or more gateway instances on YARN as determined by the gateway service configuration.

In the section :ref:`admin-manual-system-requirements`, we list the specific
:ref:`hardware <admin-manual-hardware-requirements>`,
:ref:`memory and core requirements <admin-manual-memory-core-requirements>`,
:ref:`network <admin-manual-network-requirements>`, and
:ref:`prerequisite software <admin-manual-software-requirements>` requirements
that need to be met and completed before installation of the CDAP components.
