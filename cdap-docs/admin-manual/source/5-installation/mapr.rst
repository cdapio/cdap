.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _mapr-index:

====
MapR
====

This section is to help you install, configure, and start CDAP on existing
`MapR Converged Data Platform <https://www.mapr.com>`__ clusters.

These instructions assume that you are familiar with MapR, and already have a cluster with
it installed and running. The cluster must meet CDAP's :ref:`hardware, network, and
software requirements <admin-manual-system-requirements>` before you install CDAP.

Setting Up Clients
==================

As described in the :ref:`Software Prerequisites <admin-manual-software-requirements>`, 
a configured Hadoop and HBase (plus an optional Hive client) needs to be configured on the
node(s) where CDAP will run.

If colocating CDAP on cluster hosts with actual services, such as the MapR CLDB, Yarn
ResourceManager, or HBase Master, then the client configurations will already be in place.

- To configure a MapR client, see the MapR documentation on `Setting Up the Client
  <http://doc.mapr.com/display/MapR/Setting+Up+the+Client>`__.

- To configure a MapR HBase client, see the MapR documentation on `Installing HBase on a Client
  <http://doc.mapr.com/display/MapR/Installing+HBase#InstallingHBase-HBaseonaClientInstallingHBaseonaClient>`__.

- To configure a MapR Hive client, see the MapR documentation on `Installing Hive
  <http://doc.mapr.com/display/MapR/Installing+Hive>`__.

A typical client node should have the ``mapr-client``, ``mapr-hbase``, and ``mapr-hive``
packages installed, and can be configured using the MapR `configure.sh
<http://doc.mapr.com/display/MapR/configure.sh>`__ utility.


Setting Up the CDAP Repos
=========================

Preparing Package Managers
--------------------------

.. include:: /../target/_includes/mapr-1-installation.rst
    :start-after: .. _mapr-preparing-package-managers:
    :end-before: .. end_install-debian-using-apt


Component Installation
======================

.. include:: /../target/_includes/mapr-1-installation.rst
    :start-after: .. _mapr-package-installation-title:
    :end-before: .. _mapr-preparing-cluster:


Preparing the Cluster
---------------------

.. highlight:: console
   
To prepare your cluster so that CDAP can write to its default namespace,
create a top-level ``/cdap`` directory in MapRFS, owned by the MapRFS user ``cdap``::

  $ su mapr
  $ hadoop fs -mkdir -p /cdap && hadoop fs -chown cdap /cdap

In the CDAP packages, the default property ``hdfs.namespace`` is ``/cdap`` and the default property
``hdfs.user`` is ``yarn``.

Also, create a ``tx.snapshot`` subdirectory::

  $ su mapr
  $ hadoop fs -mkdir -p /cdap/tx.snapshot && hadoop fs -chown cdap /cdap/tx.snapshot

**Note:** If you have customized the property ``data.tx.snapshot.dir`` in your 
:ref:`CDAP configuration <appendix-cdap-site.xml>`, use that value instead.


.. |display-distribution| replace:: MapR

.. |hdfs-user| replace:: ``cdap``

.. include:: /../target/_includes/mapr-2-configuration.rst
    :end-before: #. Depending on your installation, you may need to set these properties:

#. CDAP requires that an additional entry |---| ``/opt/mapr/lib/*`` |---| 
   be appended to the ``yarn.application.classpath`` setting of ``yarn-site.xml``. The
   default ``yarn.application.classpath`` for Linux with this additional entry appended is
   (reformatted to fit)::

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
    
   **Notes:** 
   
   - Since MapR might not dereference the Hadoop variables (such as ``$HADOOP_CONF_DIR``)
     correctly, we recommend specifying their full paths instead of the variables we have
     included here.

   - MapR does not, by default, provide a configured ``yarn.application.classpath``, and
     you will need to add this entry to ``yarn-site.xml``. If you install using `Chef
     <https://www.getchef.com>`__, that file and entry is created automatically, but not
     with dereferenced Hadoop variables.

#. Depending on your installation, you may need to set these properties:

.. include:: /../target/_includes/mapr-2-configuration.rst
    :start-after: #. Depending on your installation, you may need to set these properties:
    :end-before: .. _mapr-configuration-security:
    
As in all installations, the ``kafka.log.dir`` may need to be created locally. If you
configure ``kafka.log.dir`` (or any of the other settable parameters) to a particular
directory, you need to make sure that **the directory exists** and that it **is writable**
by the CDAP user.

.. _mapr-configuration-security:

.. include:: /../target/_includes/mapr-2-configuration.rst
    :start-after: .. _mapr-configuration-security:
    :end-before: .. _mapr-configuration-hdp:


.. include:: /../target/_includes/mapr-3-starting.rst


.. 
.. .. toctree::
.. 
..     Setting Up Clients <mapr-1-setting-up>
..     Preparing Package Managers <mapr-2-preparing>
..     Component Installation <mapr-3-installation>
..     Preparing the Cluster <mapr-4-preparing-cluster>
..     Additional Configurations <mapr-5-configurations>
..     Starting Services <mapr-6-starting>
..     Upgrading CDAP <mapr-7-upgrading>

.. 
.. .. |mapr-setting-up| replace:: **Setting Up Clients:**
.. .. _mapr-setting-up: mapr-1-setting-up.html
.. 
.. .. |mapr-preparing| replace:: **Preparing Package Managers:**
.. .. _mapr-preparing: mapr-2-preparing.html
.. 
.. .. |mapr-installation| replace:: **Component Installation:**
.. .. _mapr-installation: mapr-3-installation.html
.. 
.. .. |mapr-preparing-cluster| replace:: **Preparing the Cluster:**
.. .. _mapr-preparing-cluster: mapr-4-preparing-cluster.html
.. 
.. .. |mapr-configurations| replace:: **Additional Configurations:**
.. .. _mapr-configurations: mapr-5-configurations.html
.. 
.. .. |mapr-starting| replace:: **Starting Services:**
.. .. _mapr-starting: mapr-6-starting.html
.. 
.. .. |mapr-upgrading| replace:: **Upgrading CDAP:**
.. .. _mapr-upgrading: mapr-7-upgrading.html
.. 
.. - |mapr-setting-up|_ Steps for setting up individual client nodes in a MapR cluster.
.. - |mapr-preparing|_ Required so that the package managers can download the components.
.. - |mapr-installation|_ Installs CDAP on each box of the cluster.
.. - |mapr-preparing-cluster|_ Required steps before starting up CDAP.
.. - |mapr-configurations|_ Covers the configuration options and requirements of the CDAP installation, secure Hadoop,
..   security, use of temp directories, and the CDAP user.
.. - |mapr-starting|_ Once installation and configuration is complete, how to start CDAP services on the cluster and make them highly-available.
.. - |mapr-upgrading|_ Upgrading a CDAP installation that was installed and managed with MapR.


