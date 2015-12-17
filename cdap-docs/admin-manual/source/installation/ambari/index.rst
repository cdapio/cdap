.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _ambari-index:

===================
Apache Ambari (HDP)
===================

.. toctree::

    Setting-up CDAP Repos <am-setting-up>
    Adding Dependencies for CDAP <am-dependencies>
    Installing using Service Wizard <am-service-wizard>
    Upgrading using Apache Ambari <am-upgrading>
   
This section is to help you install the Cask Data Application Platform (CDAP) on Hadoop
systems that are `HDP (Hortonworks Data Platform) <http://hortonworks.com/>`__ clusters
managed with `Apache Ambari <https://ambari.apache.org/>`__, the open source provisioning
system for HDP.

**Note:** 

- Apache Ambari can only be used to add CDAP to an *existing* Hadoop cluster, one
  that already has that the correct services (Hadoop, Hive, etc.) are installed.
- Ambari is for setting up HDP on bare clusters; it's not used for those with HDP already installed, where the
  original installation was **not** with Ambari.
- Though you can install CDAP with Apache Ambari, you currently cannot use Ambari to upgrade CDAP. 
  Instead, please see :ref:`package-managers-upgrading`.
- A number of features are planned for the future, but currently **not** included in the
  CDAP Apache Ambari Service:
  
  - `a full smoke test of CDAP functionality after installation <https://issues.cask.co/browse/CDAP-4105>`__; 
  - `pre-defined alerts for CDAP services <https://issues.cask.co/browse/CDAP-4106>`__;
  - `CDAP component high-availability support <https://issues.cask.co/browse/CDAP-4107>`__;
  - `select CDAP metrics <https://issues.cask.co/browse/CDAP-4108>`__;
  - `support for Kerberos-enabled clusters <https://issues.cask.co/browse/CDAP-4109>`__; and
  - `integration with the CDAP Authentication Server <https://issues.cask.co/browse/CDAP-4110>`__.


You can use Ambari to integrate CDAP into a Hadoop cluster
by adding the `CDAP Ambari Services <https://github.com/caskdata/cdap-ambari-service>`__
to your Ambari server. Once you have restarted your Ambari server, you will able to
use the Ambari UI (Ambari Dashboard) to install, start, and manage CDAP on Hadoop clusters.

These instructions cover the steps to install CDAP using Apache Ambari. It's assumed that
you are familiar with Ambari and HDP, and have a cluster with them installed and running.

.. |am-setting-up| replace:: **Setting-up CDAP Repos:**
.. _am-setting-up: am-setting-up.html

.. |am-dependencies| replace:: **Adding Dependencies for CDAP:**
.. _am-dependencies: am-dependencies.html

.. |am-service-wizard| replace:: **Installing CDAP:**
.. _am-service-wizard: am-service-wizard.html

.. |am-upgrading| replace:: **Upgrading CDAP:**
.. _am-upgrading: am-upgrading.html

- |am-setting-up|_ The prerequisite steps of preparing the Package Manager.
- |am-dependencies|_ The dependencies, both core and optional, required by CDAP.
- |am-service-wizard|_ Using the Ambari *Add Service* Wizard, and starting CDAP.
- |am-service-wizard|_ Upgrading CDAP that was installed and managed with Apache Ambari.
