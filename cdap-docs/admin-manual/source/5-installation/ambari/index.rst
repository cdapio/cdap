.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _ambari-index:

===================
Apache Ambari (HDP)
===================

.. toctree::

    Setting-up CDAP Repos <am-1-setting-up>
    Adding Dependencies for CDAP <am-2-dependencies>
    Installing using Service Wizard <am-3-service-wizard>
    Upgrading using Apache Ambari <am-4-upgrading>
   
This section is to help you install the Cask Data Application Platform (CDAP) on Hadoop
systems that are `HDP (Hortonworks Data Platform) <http://hortonworks.com/>`__ clusters
managed with `Apache Ambari <https://ambari.apache.org/>`__, the open source provisioning
system for HDP.

You use Ambari to integrate CDAP into a Hadoop cluster
by adding the `CDAP Ambari Services <https://github.com/caskdata/cdap-ambari-service>`__
to your Ambari Server. Once you have restarted your Ambari Server, you will able to
use the Ambari UI (Ambari Dashboard) to install, start, and manage CDAP on HDP clusters.

These instructions assume that you are familiar with Apache Ambari and HDP, and already
have a cluster with them installed and running.

.. |am-setting-up| replace:: **Setting-up CDAP Repos:**
.. _am-setting-up: am-1-setting-up.html

.. |am-dependencies| replace:: **Adding Dependencies for CDAP:**
.. _am-dependencies: am-2-dependencies.html

.. |am-service-wizard| replace:: **Installing CDAP:**
.. _am-service-wizard: am-3-service-wizard.html

.. |am-upgrading| replace:: **Upgrading CDAP:**
.. _am-upgrading: am-4-upgrading.html

- |am-setting-up|_ The prerequisite steps of preparing the Package Manager.
- |am-dependencies|_ The dependencies, both core and optional, required by CDAP.
- |am-service-wizard|_ Using the Ambari *Add Service* Wizard, and starting CDAP.
- |am-upgrading|_ Upgrading CDAP that was installed and managed with Apache Ambari.


.. rubric:: Notes

- Apache Ambari can only be used to add CDAP to an **existing** Hadoop cluster, one
  that already has the required services (Hadoop, Hive, etc.) installed.
- Ambari is for setting up HDP on bare clusters; it's not used for those with HDP already installed, where the
  original installation was **not** with Ambari.
- Though you can install CDAP with Apache Ambari, you **cannot** currently use Ambari to upgrade CDAP. 
  Instead, please see :ref:`package-managers-upgrading`.
- A number of features are planned for the future, but are **not** currently included in the
  CDAP Apache Ambari Service:
  
  - `a full smoke test of CDAP functionality after installation <https://issues.cask.co/browse/CDAP-4105>`__; 
  - `pre-defined alerts for CDAP services <https://issues.cask.co/browse/CDAP-4106>`__;
  - `CDAP component high-availability support <https://issues.cask.co/browse/CDAP-4107>`__;
  - `select CDAP metrics <https://issues.cask.co/browse/CDAP-4108>`__;
  - `support for Kerberos-enabled clusters <https://issues.cask.co/browse/CDAP-4109>`__; and
  - `integration with the CDAP Authentication Server <https://issues.cask.co/browse/CDAP-4110>`__.


