.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.

.. _installation-replication:

================
CDAP Replication
================

This document lists the detailed steps required for setting up CDAP replication, where one
CDAP cluster (a *master*) is replicated to one or more additional CDAP *slave* clusters.

**Note:** As described below, CDAP must have :ref:`invalid transaction list pruning disabled 
<installation-replication-disable-invalid-transaction-list-pruning>`, as this cannot be
used with replication.

**These steps should be reviewed (and the `Cluster Setup`_ completed) prior to starting CDAP.**

.. _installation-replication-cluster-setup:

Cluster Setup
=============

CDAP replication relies on the cluster administrator setting up replication on `HBase`_,
`HDFS`_, `Hive`_, and `Kafka`_.

- It is assumed that CDAP is only running on the master cluster.
- It is assumed that **you have not started CDAP before any of these steps**.

HBase
-----
- Install the relevant ``cdap-hbase-compat`` package on all HBase nodes of your cluster in order
  to use the replication status coprocessors. Note that due to HBase limitations, these
  coprocessors cannot be used on HBase 0.96 or 0.98. 
  
  Available "compat" packages are:

  - ``cdap-hbase-compat-1.0``
  - ``cdap-hbase-compat-1.0-cdh``
  - ``cdap-hbase-compat-1.0-cdh5.5.0``
  - ``cdap-hbase-compat-1.1``
  - ``cdap-hbase-compat-1.2-cdh5.7.0``
  
  *Note:* For Cloudera Manager, all of these packages will be installed in your "Parcel Directory"
  and as described below, you will add the appropriate one to your ``HBASE_CLASSPATH``.

.. highlight:: xml

- Modify ``hbase-site.xml`` on all HBase nodes to enable HBase replication, and to use the
  CDAP replication status coprocessors::

    <property>
      <name>hbase.replication</name>
      <value>true</value>
    </property>
    <property>
      <name>hbase.coprocessor.regionserver.classes</name>
      <value>co.cask.cdap.data2.replication.LastReplicateTimeObserver</value>
    </property>
    <property>
      <name>hbase.coprocessor.wal.classes</name>
      <value>co.cask.cdap.data2.replication.LastWriteTimeObserver</value>
    </property>

.. highlight:: console

- Modify ``hbase-env.sh`` on all HBase nodes to include the HBase coprocessor in the classpath::

    export HBASE_CLASSPATH="$HBASE_CLASSPATH:/<cdap-home>/<hbase-compat-version>/coprocessor/*"
    
    # <cdap-home> will vary depending on your distribution and installation
    # For Cloudera Manager/CDH: it is "${PARCEL_ROOT}/CDAP" where ${PARCEL_ROOT} is your configured "Parcel Directory"
    # Ambari/HDP, MapR, packages: it is "/opt/cdap"
    #
    # <hbase-compat-version> is the HBase package compatible with the distribution
 
    # For example, if you are on a Cloudera Manager cluster with CDH 5.5.x:
    export HBASE_CLASSPATH="$HBASE_CLASSPATH:/opt/cloudera/parcels/CDAP/hbase-compat-1.0-cdh5.5.0/coprocessor/*"

- Restart HBase master and regionservers.
- Enable replication from master to slave::

    master_hbase_shell> add_peer '[slave-name]', '[slave-zookeeper-quorum]:/[slave-zk-node]'
 
    # For example:
    master_hbase_shell> add_peer 'slave', 'slave.example.com:2181:/hbase'

- Enable replication from slave to master::

    slave_hbase_shell> add_peer '[master-name]', '[master-zookeeper-quorum]:/[master-zk-node]'
 
    # For example:
    slave_hbase_shell> add_peer 'master', 'master.example.com:2181:/hbase'
    
- Confirm that HBase replication is working::

    master_hbase_shell> create 'repltest', 'f'
    
    slave_hbase_shell> create 'repltest', 'f'
    
    master_hbase_shell> enable_table_replication 'repltest'
    
    slave_hbase_shell> alter 'repltest', { 'NAME' => 'f', 'REPLICATION_SCOPE' => 1 }
    
    master_hbase_shell> put 'repltest', 'masterrow', 'f:v1', 'v1'
    
    slave_hbase_shell> put 'repltest', 'slaverow', 'f:v1', 'v1'
    
    master_hbase_shell> scan 'repltest'
    
    slave_hbase_shell> scan 'repltest'

HDFS
----
Set up HDFS replication using the solution provided by your distribution. HDFS does not
have true replication, but it is usually achieved by scheduling regular ``distcp`` jobs.

Hive
----
Set up replication for the database backing your Hive Metastore. Note that this will
simply replicate the Hive metadata |---| which tables exist, table metadata, etc. |---|
but not the data itself. It is assumed you will not be running Hive queries on the slave
until after a manual failover occurs.

For example, to setup MySQL 5.7 replication, follow the steps described at 
`Setting Up Binary Log File Position Based Replication 
<https://dev.mysql.com/doc/refman/5.7/en/replication-howto.html>`__.
             
Kafka
-----
Set up replication for the Kafka brokers you are using. *Kafka MirrorMaker* is the most
common solution. See `Mirroring data between clusters 
<https://kafka.apache.org/documentation.html#basic_ops_mirror_maker>`__ and
`Kafka mirroring (MirrorMaker) 
<https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27846330>`__
for additional information.

.. _installation-replication-cdap-setup:

CDAP Setup
==========
CDAP requires that you provide an extension that will perform HBase-related DDL operations
on both clusters instead of only on a single cluster. To create the extension, you must
implement the ``HBaseDDLExecutor`` class. Details on implementing this class, a sample
implementation, and example files are available in the :ref:`Appendix: HBaseDDLExecutor
<appendix-hbase-ddl-executor>`.

CDAP must have :ref:`invalid transaction list pruning disabled 
<installation-replication-disable-invalid-transaction-list-pruning>`, as this cannot be
used with replication.

.. highlight:: console

To deploy your extension (once compiled and packaged as a JAR file, such as
*my-extension.jar*), run these steps on **both** your master and slave clusters.
These steps assume ``<cdap-home>`` is ``/opt/cdap``:

1. Create an extension directory, such as::

    $ mkdir -p /opt/cdap/master/ext/hbase/repl
    
#. Copy your JAR to the directory::

    $ cp my-extension.jar /opt/cdap/master/ext/hbase/repl/

   .. highlight:: xml

#. Modify ``cdap-site.xml`` to use your implementation of ``HBaseDDLExecutor``::

    <property>
      <name>hbase.ddlexecutor.extension.dir</name>
      <value>/opt/cdap/master/ext/hbase</value>
    </property>

#. Modify ``cdap-site.xml`` with any properties required by your executor. Any property prefixed
   with ``cdap.hbase.spi.hbase.`` will be available through the
   ``HBaseDDLExecutorContext`` object passed into your executor's initialize method::

    <property>
      <name>cdap.hbase.spi.hbase.zookeeper.quorum</name>
      <value>slave.example.com:2181/cdap</value>
    </property>
    <property>
      <name>cdap.hbase.spi.hbase.zookeeper.session.timeout</name>
      <value>60000</value>
    </property>
    <property>
      <name>cdap.hbase.spi.hbase.cluster.distributed</name>
      <value>true</value>
    </property>
    <property>
      <name>cdap.hbase.spi.hbase.bulkload.staging.dir</name>
      <value>/tmp/hbase-staging</value>
    </property>
    <property>
      <name>cdap.hbase.spi.hbase.replication</name>
      <value>true</value>
    </property>

   .. _installation-replication-disable-invalid-transaction-list-pruning:

#. Modify ``cdap-site.xml`` to **disable invalid transaction list pruning,** as it cannot
   be used with replication::

    <property>
      <name>data.tx.prune.enable</name>
      <value>false</value>
      <description>
        Enable invalid transaction list pruning
      </description>
    </property>

   .. highlight:: console

#. Before starting CDAP on the master cluster, run a command on the slave cluster to load
   the HBase coprocessors required by CDAP onto the slave's HDFS::
   
    [slave] $ cdap setup coprocessors

#. Start CDAP on the master cluster::

    [master] $ cdap master start

.. highlight:: console

Manual Failover Procedure
=========================
To manually failover from the master to a slave cluster, follow these steps:

1. Stop all CDAP programs on the master cluster
#. Stop CDAP on the master cluster
#. Copy any HDFS files that have not yet been copied using either your distro's solution or ``distcp``
#. Run the CDAP replication status tool to retrieve the cluster state::

    [master] $ cdap run co.cask.cdap.data.tools.ReplicationStatusTool -m -o /tmp/master_state

#. Copy the master state onto your slave cluster::

    [master] $ scp /tmp/master_state <slave>:/tmp/master_state

#. Verify that replication has copied the required data onto the slave::

    [slave] $ cdap run co.cask.cdap.data.tools.ReplicationStatusTool -i /tmp/master_state
    ...
    Master and Slave Checksums match. HDFS Replication is complete.
    HBase Replication is complete.

#. Run Hive's ``metatool`` to update the locations for the Hive tables::

    [slave] $ hive --service metatool -updateLocation hdfs://[slave-namenode-host]:[slave-namenode-port] \
                 hdfs://[master-namenode-host]:[master-namenode-port] \
                 -tablePropKey avro.schema.url -serdePropKey avro.schema.url

#. Start CDAP on the slave::

    [slave] $ cdap master start
