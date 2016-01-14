.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _package-managers-index:

=============================
Package Managers (RPM/Debian)
=============================

This section describes installing CDAP on Hadoop clusters that are:

- Generic Apache Hadoop distributions; 
- CDH clusters *not managed* with Cloudera Manager; or
- HDP clusters *not managed* with Apache Ambari.

Cloudera Manager (CDH), Apache Ambari (HDP), and MapR distributions should be installed
with our other :ref:`distribution instructions <installation-index>`.

These instructions cover the steps to install, configure, and start CDAP. They assume
that you are familiar with Hadoop, and already have a cluster with it installed and running.
The cluster must meet CDAP's :ref:`hardware, network, and software requirements
<admin-manual-system-requirements>` before you install CDAP.

Follow these steps:

1. **Setting Up the Cluster:** Prerequisite tasks prior to starting the installation
#. **Setting Up the CDAP Repos:** Preparing the package mangers to obtain the CDAP parcels
#. **Component Installation:** Installing the packages and then preparing the cluster for startup
#. **Additional Configurations:** Configuration settings required before starting CDAP
#. **Starting CDAP Services**

.. figure:: ../_images/distributions/distributions-cm-1.gif
   :height: 80px
   :align: center

Once you have completed the installation and started CDAP services,
you can then :ref:`verify the installation <admin-manual-verification>`.


Setting Up the Cluster
======================
Please review the :ref:`Software Prerequisites <admin-manual-software-requirements>`, 
as a configured Hadoop, HBase, and Hive (plus an optional Spark client) needs to be configured on the
node(s) where CDAP will run. :ref:`Node.js <admin-manual-software-requirements>` is required on all
nodes where CDAP services run.

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
