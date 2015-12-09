.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _admin-manual-cdap-components:

===============
CDAP Components
===============

These are the CDAP components:

- **CDAP UI:** User interface—the *Console*—for managing CDAP applications;
- **CDAP Router:** Service supporting REST endpoints for CDAP;
- **CDAP Master:** Service for managing runtime, lifecycle and resources of CDAP applications;
- **CDAP Kafka:** Metrics and logging transport service, using an embedded version of *Kafka*; and
- **CDAP Authentication Server:** Performs client authentication for CDAP when security is enabled.

Before installing the CDAP components, you must first install a Hadoop cluster
with *HDFS*, *YARN*, *HBase*, and *ZooKeeper*. In order to use the ad-hoc querying capabilities
of CDAP, you will also need *Hive*. All CDAP components can be installed on the
same boxes as your Hadoop cluster, or on separate boxes that can connect to the Hadoop services.

Our recommended installation is to use two boxes for the CDAP components; the
`hardware requirements <#hardware-requirements>`__ are relatively modest,
as most of the work is done by the Hadoop cluster. These two
boxes provide high availability; at any one time, one of them is the leader
providing services while the other is a follower providing failover support.

Some CDAP components run on YARN, while others orchestrate “containers” in the Hadoop cluster.
The CDAP Router service starts a router instance on each of the local boxes and instantiates
one or more gateway instances on YARN as determined by the gateway service configuration.

We have specific
`hardware <#hardware-requirements>`_,
`network <#network-requirements>`_ and
`prerequisite software <#software-prerequisites>`_ requirements detailed under
`system requirements <#system-requirements>`__
that need to be met and completed before installation of the CDAP components.
