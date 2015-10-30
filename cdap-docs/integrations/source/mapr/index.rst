.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _mapr-configuring:

=======================================
Configuring and Installing CDAP on MapR
=======================================

.. highlight:: console

This section will assist you with installing CDAP on `MapR <https://www.mapr.com>`__. CDAP
can be installed on a MapR cluster by following the :ref:`manual instructions for installing
CDAP <install>`. These notes are an addition to those steps.

Note that as described in the :ref:`Software Prerequisites <install-software-requirements>`, 
a configured Hadoop and HBase (plus an optional Hive client) needs to be configured
on the node(s) where CDAP will run.

- To configure a MapR Hadoop client, see the MapR documentation on `Setting Up the Client
  <http://doc.mapr.com/display/MapR/Setting+Up+the+Client>`__.

- To configure a MapR HBase client, see the MapR documentation on `Installing HBase on a Client
  <http://doc.mapr.com/display/MapR/Installing+HBase#InstallingHBase-HBaseonaClientInstallingHBaseonaClient>`__.

- To configure a MapR Hive client, see the MapR documentation on `Installing Hive
  <http://doc.mapr.com/display/MapR/Installing+Hive>`__.

A typical client node should have the ``mapr-client``, ``mapr-hbase``, and ``mapr-hive`` packages installed, and can
be configured using the MapR `configure.sh <http://doc.mapr.com/display/MapR41/configure.sh>`__ utility.

As in all installations, the ``kafka.log.dir`` may need to be created locally.

MapR does not provide a configured ``yarn.application.classpath`` by default. CDAP requires that an additional
entry |---| ``/opt/mapr/lib/*`` |---| be appended to the ``yarn.application.classpath`` setting in ``yarn-site.xml``.
The default ``yarn.application.classpath`` for Linux with this additional entry appended is (reformatted to fit)::

  $HADOOP_CONF_DIR, 
  $HADOOP_COMMON_HOME/share/hadoop/common/*, 
  $HADOOP_COMMON_HOME/share/hadoop/common/lib/*, 
  $HADOOP_HDFS_HOME/share/hadoop/hdfs/*, 
  $HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*, 
  $HADOOP_YARN_HOME/share/hadoop/yarn/*, 
  $HADOOP_YARN_HOME/share/hadoop/yarn/lib/*, 
  $HADOOP_COMMON_HOME/share/hadoop/mapreduce/*, 
  $HADOOP_COMMON_HOME/share/hadoop/mapreduce/lib/*, 
  /opt/mapr/lib/*

**Note:** Since MapR might not dereference the Hadoop variables (such as
``$HADOOP_CONF_DIR``) correctly, we recommend specifying their full paths instead.
