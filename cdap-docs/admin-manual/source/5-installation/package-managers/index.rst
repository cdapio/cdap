.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _package-managers-index:

=============================
Package Managers (RPM/Debian)
=============================

.. toctree::

    Setting-up the Cluster <package-managers-1-setting-up>
    Preparing Package Managers <package-managers-2-preparing>
    Component Installation <package-managers-3-installation>
    Additional Configurations <package-managers-4-configurations>
    Starting Services <package-managers-5-starting>
    Upgrading CDAP <package-managers-6-upgrading>

This section is to help you install the Cask Data Application Platform (CDAP) on Hadoop
systems that are:

- Generic Apache Hadoop distributions; 
- CDH clusters *not managed* with Cloudera Manager; or
- HDP clusters *not managed* with Ambari.

Other distributions should be installed with our other :ref:`instructions <installation-index>`.

These instructions cover the steps to install, configure, and start CDAP. They assume
that you are familiar with Hadoop, and already have a cluster with it installed and running.

.. |pm-setting-up| replace:: **Setting-up the Cluster:**
.. _pm-setting-up: package-managers-1-setting-up.html

.. |pm-preparing| replace:: **Preparing Package Managers:**
.. _pm-preparing: package-managers-2-preparing.html

.. |pm-installation| replace:: **Component Installation:**
.. _pm-installation: package-managers-3-installation.html

.. |pm-configurations| replace:: **Additional Configurations:**
.. _pm-configurations: package-managers-4-configurations.html

.. |pm-starting| replace:: **Starting Services:**
.. _pm-starting: package-managers-5-starting.html

.. |pm-upgrading| replace:: **Upgrading CDAP using Package Managers:**
.. _pm-upgrading: package-managers-6-upgrading.html

- |pm-setting-up|_ Steps for setting-up individual client nodes in a Hadoop cluster.
- |pm-preparing|_ Required so that the package managers can download the components.
- |pm-installation|_ Installs CDAP on each box of the cluster using RPM or Debian.
- |pm-configurations|_ Covers the configuration options and requirements of the CDAP installation, secure Hadoop,
  security, use of temp directories, and the CDAP user.
- |pm-starting|_ Once installation is complete, how to start CDAP on the cluster and make it highly-available.
- |pm-upgrading|_ Specific steps for upgrading existing CDAP installations.


