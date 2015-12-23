.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _mapr-index:

====
MapR
====

.. toctree::

    Setting-up MapR Clients <mapr-1-setting-up>
    Preparing Package Managers <mapr-2-preparing>
    Component Installation <mapr-3-installation>
    Additional Configurations <mapr-4-configurations>
    Starting Services <mapr-5-starting>
    Upgrading CDAP <mapr-6-upgrading>

This section is to help you install, configure, and start CDAP on existing `MapR systems
<https://www.mapr.com>`__.

These instructions assume that you are familiar with MapR, and already have a cluster with
it installed and running.s The cluster must meet CDAP's :ref:`hardware, network, and
software requirements <admin-manual-system-requirements>` before you install CDAP.

.. |mapr-setting-up| replace:: **Setting-up MapR Clients:**
.. _mapr-setting-up: mapr-1-setting-up.html

.. |mapr-preparing| replace:: **Preparing Package Managers:**
.. _mapr-preparing: mapr-2-preparing.html

.. |mapr-installation| replace:: **Component Installation:**
.. _mapr-installation: mapr-3-installation.html

.. |mapr-configurations| replace:: **Additional Configurations:**
.. _mapr-configurations: mapr-4-configurations.html

.. |mapr-starting| replace:: **Starting Services:**
.. _mapr-starting: mapr-5-starting.html

.. |mapr-upgrading| replace:: **Upgrading CDAP:**
.. _mapr-upgrading: mapr-6-upgrading.html

- |mapr-setting-up|_ Steps for setting-up individual client nodes in a MapR cluster.
- |mapr-preparing|_ Required so that the package managers can download the components.
- |mapr-installation|_ Installs CDAP on each box of the cluster.
- |mapr-configurations|_ Covers the configuration options and requirements of the CDAP installation, secure Hadoop,
  security, use of temp directories, and the CDAP user.
- |mapr-starting|_ Once installation is complete, how to start CDAP on the cluster and make it highly-available.
- |mapr-upgrading|_ Upgrading a CDAP installation that was installed and managed with MapR.

