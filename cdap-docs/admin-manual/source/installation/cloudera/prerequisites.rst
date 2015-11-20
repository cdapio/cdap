.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cloudera-configuring:

====================================
Cloudera Manager: CDAP Prerequisites
====================================

.. rubric:: Roles and Dependencies

The CDAP CSD consists of four mandatory roles:

- Master
- Gateway/Router
- Kafka-Server
- UI

and an optional role |---| Security Auth Service |---| plus a Gateway client configuration. 

CDAP depends on HBase, YARN, HDFS, ZooKeeper, and |---| optionally |---| Hive. It must
also be placed on a cluster host with full client configurations for these dependent
services. Therefore, CDAP roles must be colocated on a cluster host with at least an HDFS
Gateway, Yarn Gateway, HBase Gateway, and |---| optionally |---| a Hive Gateway. Note that
Gateways are redundant if colocating CDAP on cluster hosts with actual services, such as
the HBase Master, Yarn Resourcemanager, or HDFS Namenode.

All services run as the 'cdap' user installed by the parcel.


.. rubric:: Prerequisites

#. Node.js (we recommend any version of `Node.js <https://nodejs.org/>`__ |node-js-version|.) 
   must be installed on the node(s) where the UI
   role instance will run. You can download the appropriate version of Node.js from `nodejs.org
   <http://nodejs.org/dist/>`__.

#. ZooKeeper's ``maxClientCnxns`` must be raised from its default.  We suggest setting it to zero
   (unlimited connections). As each YARN container launched by CDAP makes a connection to ZooKeeper, 
   the number of connections required is a function of usage.

#. For Kerberos-enabled Hadoop clusters:

   - The 'cdap' user needs to be granted HBase permissions to create tables.
     In an HBase shell, enter::
     
      > grant 'cdap', 'ACRW'

   - The 'cdap' user must be able to launch YARN containers, either by adding it to the YARN
     ``allowed.system.users`` or by adjusting ``min.user.id``.

#. Ensure YARN is configured properly to run MapReduce programs.  Often, this includes
   ensuring that the HDFS ``/user/yarn`` directory exists with proper permissions.

#. Ensure that YARN has sufficient memory capacity by lowering the default minimum container 
   size. Lack of YARN memory capacity is the leading cause of apparent failures that we
   see reported.

