.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cloudera-configuring:

===================
Preparing the Roles
===================

Roles and Dependencies
======================
The CDAP CSD (Custom Service Descriptor) consists of four mandatory roles:

- Master
- Gateway/Router
- Kafka-Server
- UI

with a fifth optional role (Security Auth Service) plus a Gateway client configuration. 

As CDAP depends on HDFS, YARN, HBase, ZooKeeper, and (optionally) Hive, it must be placed
on a cluster host with full client configurations for these dependent services. 

CDAP roles must be colocated on a cluster host with at least an HDFS Gateway, a Yarn
Gateway, an HBase Gateway, and (optionally) a Hive Gateway. Note that these Gateways are
redundant if you are colocating CDAP on cluster hosts with actual services, such as the
HDFS Namenode, Yarn Resourcemanager, or HBase Master.

All services run as the ``'cdap'`` user installed by the parcel.


Prerequisites
=============
#. Node.js must be installed on the node(s) where the UI role instance will run. 
   We recommend any version of `Node.js <https://nodejs.org/>`__ |node-js-version|; in
   particular, we recommend |recommended_node_js_version|. You can download an appropriate
   version of Node.js from `nodejs.org <http://nodejs.org/dist/>`__. Detailed
   instructions on installing Node.js :ref:are available <admin-manual-install-node.js>`.

#. ZooKeeper's ``maxClientCnxns`` must be raised from its default.  We suggest setting it to zero
   (unlimited connections). As each YARN container launched by CDAP makes a connection to ZooKeeper, 
   the number of connections required is a function of usage. You can make this change using Cloudera Manager to
   `modify the ZooKeeper configuration properties <http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cm_mc_mod_configs.html>`__.

#. Ensure YARN is configured properly to run MapReduce programs.  Often, this includes
   ensuring that the HDFS ``/user/yarn`` directory exists with proper permissions.

#. Ensure that YARN has sufficient memory capacity by lowering the default minimum container 
   size (controlled by the property ``yarn.scheduler.minimum-allocation-mb``). Lack of
   YARN memory capacity is the leading cause of apparent failures that we see reported.
   We recommend starting with these settings:
   
   - ``yarn.nodemanager.delete.debug-delay-sec``: 43200
   - ``yarn.nodemanager.resource.memory-mb``: Adjust if you need to raise memory per nodemanager
   - ``yarn.nodemanager.resource.cpu-vcores``: Adjust if you need to raise vcores per nodemanager
   - ``yarn.scheduler.minimum-allocation-mb``: 512 mb
   
   You can make these changes `using Cloudera Manager <http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cm_mc_mod_configs.html>`__.
   You will be prompted to restart the stale services after making changes.
    
#. For Kerberos-enabled Hadoop clusters:

   - The ``'cdap'`` user needs to be granted HBase permissions to create tables.
     In an HBase shell (``$ hbase shell``), enter::
     
      > grant 'cdap', 'RWCA'

   - The ``'cdap'`` user must be able to launch YARN containers, either by adding it to the YARN
     ``allowed.system.users`` or by adjusting the YARN ``min.user.id`` to include the ``cdap`` user.
     (Search for the YARN configuration ``allowed.system.users`` in Cloudera Manager, and then add
     the ``cdap`` user to the whitelist.)

