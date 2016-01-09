.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _package-managers-index:

=============================
Package Managers (RPM/Debian)
=============================

.. 
.. .. toctree::
.. 
..     Setting Up the Cluster <package-managers-1-setting-up>
..     Preparing Package Managers <package-managers-2-preparing>
..     Component Installation <package-managers-3-installation>
..     Additional Configurations <package-managers-4-configurations>
..     Starting Services <package-managers-5-starting>
..     Upgrading CDAP <package-managers-6-upgrading>

This section is to help you install CDAP on Hadoop clusters that are:

- Generic Apache Hadoop distributions; 
- CDH clusters *not managed* with Cloudera Manager; or
- HDP clusters *not managed* with Apache Ambari.

Cloudera Manager (CDH), Apache Ambari (HDP), and MapR distributions should be installed
with our other :ref:`distribution instructions <installation-index>`.

These instructions cover the steps to install, configure, and start CDAP. They assume
that you are familiar with Hadoop, and already have a cluster with it installed and running.

The cluster must meet CDAP's :ref:`hardware, network, and software requirements
<admin-manual-system-requirements>` before you install CDAP.

.. 
.. .. |pm-setting-up| replace:: **Setting Up the Cluster:**
.. .. _pm-setting-up: package-managers-1-setting-up.html
.. 
.. .. |pm-preparing| replace:: **Preparing Package Managers:**
.. .. _pm-preparing: package-managers-2-preparing.html
.. 
.. .. |pm-installation| replace:: **Component Installation:**
.. .. _pm-installation: package-managers-3-installation.html
.. 
.. .. |pm-configurations| replace:: **Additional Configurations:**
.. .. _pm-configurations: package-managers-4-configurations.html
.. 
.. .. |pm-starting| replace:: **Starting Services:**
.. .. _pm-starting: package-managers-5-starting.html
.. 
.. .. |pm-upgrading| replace:: **Upgrading CDAP:**
.. .. _pm-upgrading: package-managers-6-upgrading.html
.. 
.. - |pm-setting-up|_ Steps for setting up individual client nodes in a Hadoop cluster.
.. - |pm-preparing|_ Required so that the package managers can download the components.
.. - |pm-installation|_ Installs CDAP on each box of the cluster using RPM or Debian.
.. - |pm-configurations|_ Covers the configuration options and requirements of the CDAP installation, secure Hadoop,
..   security, use of temp directories, and the CDAP user.
.. - |pm-starting|_ Once installation is complete, how to start CDAP on the cluster and make it highly-available.
.. - |pm-upgrading|_ Upgrading an existing CDAP installation using a package manager.


Setting Up the Cluster
======================

As described in the :ref:`Software Prerequisites <admin-manual-software-requirements>`, 
a configured Hadoop and HBase (plus an optional Hive client) needs to be configured on the
node(s) where CDAP will run.

.. If colocating CDAP on cluster hosts with actual services, such as the MapR CLDB, Yarn
.. ResourceManager, or HBase Master, then the client configurations will already be in place.
.. 
.. - To configure a MapR client, see the MapR documentation on `Setting Up the Client
..   <http://doc.mapr.com/display/MapR/Setting+Up+the+Client>`__.
.. 
.. - To configure a MapR HBase client, see the MapR documentation on `Installing HBase on a Client
..   <http://doc.mapr.com/display/MapR/Installing+HBase#InstallingHBase-HBaseonaClientInstallingHBaseonaClient>`__.
.. 
.. - To configure a MapR Hive client, see the MapR documentation on `Installing Hive
..   <http://doc.mapr.com/display/MapR/Installing+Hive>`__.
.. 
.. A typical client node should have the ``mapr-client``, ``mapr-hbase``, and ``mapr-hive``
.. packages installed, and can be configured using the MapR `configure.sh
.. <http://doc.mapr.com/display/MapR/configure.sh>`__ utility.

.. include:: /../target/_includes/package-managers-1-installation.rst
  :end-before: .. _package-managers-install-packaging:
  
  
Setting Up the CDAP Repos
=========================

Preparing Package Managers
--------------------------

.. include:: /../target/_includes/package-managers-1-installation.rst
    :start-after: .. _package-managers-preparing-package-managers:
    :end-before: .. end_install-debian-using-apt


Component Installation
======================

.. include:: /../target/_includes/package-managers-1-installation.rst
    :start-after: .. _package-managers-package-installation-title:


.. |display-distribution| replace:: Package Managers (RPM/Debian)

.. |hdfs-user| replace:: ``yarn``

.. include:: /../target/_includes/package-managers-2-configuration.rst



.. include:: /../target/_includes/package-managers-3-starting.rst


