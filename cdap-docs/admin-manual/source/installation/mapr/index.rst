.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _mapr-index:

====
MapR
====

.. toctree::

    Installation <installation>
    Configuration <configuration>
    Starting and Verification <starting-verification.rst>
    Upgrading CDAP <upgrading>

This section is to help you install the Cask Data Application Platform (CDAP) on
`MapR systems <https://www.mapr.com>`__.

For other distributions, managers, or formats:

- For Cloudera Manager, follow :ref:`these instructions <cloudera-index>`.
- For Apache Ambari, follow :ref:`these instructions <ambari-index>`.
- For generic Apache Hadoop distributions, CDH clusters *not* managed with
  Cloudera Manager, or HDP clusters *not* managed with Ambari, follow :ref:`these instructions <hadoop-index>`.
- For the CDAP Standalone SDK, follow :ref:`these instructions <standalone-index>`. *(Developers' Manual)*

These instructions cover the steps to install CDAP on MapR systems:

.. |installation| replace:: **Installation:**
.. _installation: installation.html

.. |configuration| replace:: **Configuration:**
.. _configuration: configuration.html

.. |verification| replace:: **Starting and Verification:**
.. _verification: starting-verification.html

- |installation|_ Covers the system, network, and software requirements, packaging
  options, and instructions for installation of the CDAP components so they work with your
  existing MapR cluster.
- |configuration|_ Covers the configuration options of the CDAP installation.
- |verification|_ Starting the services, (optionally) making CDAP
  highly-available, running a health check, and verifying the installation.

There are specific instructions available for `upgrading existing CDAP installations <upgrading.html>`__.

-------


As described in the :ref:`Software Prerequisites <mapr-install-software-requirements>`, 
a configured Hadoop and HBase (plus an optional Hive client) needs to be configured
on the node(s) where CDAP will run.

If colocating CDAP on cluster hosts with actual services, such as the MapR CLDB, Yarn
ResourceManager, or HBase Master, then the client configurations will already be in place.
Note that Gateways are redundant if colocating CDAP on cluster hosts with actual services,
such as the HBase Master, Yarn Resourcemanager, or HDFS Namenode.

- To configure a MapR client, see the MapR documentation on `Setting Up the Client
  <http://doc.mapr.com/display/MapR/Setting+Up+the+Client>`__.

- To configure a MapR HBase client, see the MapR documentation on `Installing HBase on a Client
  <http://doc.mapr.com/display/MapR/Installing+HBase#InstallingHBase-HBaseonaClientInstallingHBaseonaClient>`__.

- To configure a MapR Hive client, see the MapR documentation on `Installing Hive
  <http://doc.mapr.com/display/MapR/Installing+Hive>`__.

A typical client node should have the ``mapr-client``, ``mapr-hbase``, and ``mapr-hive`` packages installed, and can
be configured using the MapR `configure.sh <http://doc.mapr.com/display/MapR/configure.sh>`__ utility.

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
