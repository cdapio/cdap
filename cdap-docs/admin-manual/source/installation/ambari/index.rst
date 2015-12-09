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
    Configuring Directories and Permissions <am-configuring>

..    Upgrading using Ambari <am-upgrading>
 
 ..     Adding CDAP Service <prerequisites>
..     Adding CDAP <adding-cdap>
..     Installing CDAP <installing-cdap>
..     Roadmap <roadmap>
       
This section is to help you install the Cask Data Application Platform (CDAP) on Hadoop
systems that are `HDP (Hortonworks Data Platform) <http://hortonworks.com/>`__ clusters
managed with `Apache Ambari <https://ambari.apache.org/>`__, the open source provisioning
system for HDP.

You can use Ambari to integrate CDAP into a Hadoop cluster
by adding the `CDAP Ambari Services <https://github.com/caskdata/cdap-ambari-service>`__
to your Ambari server. Once you have restarted your server, you will able to
use the Ambari UI (Ambari Dashboard) to install, start, and manage CDAP on Hadoop clusters.

These instructions cover the steps to install CDAP using Apache Ambari:

.. |am-setting-up| replace:: **Setting-up CDAP Repos:**
.. _am-setting-up: am-setting-up.html

.. |am-dependencies| replace:: **Adding Dependencies for CDAP:**
.. _am-dependencies: am-dependencies.html

.. |am-service-wizard| replace:: **Installing CDAP:**
.. _am-service-wizard: am-service-wizard.html

.. |am-configuring| replace:: **Configuring Directories and Permissions:**
.. _am-configuring: am-configuring.html

- |am-setting-up|_ The prerequisite steps of preparing the Package Manager.
- |am-dependencies|_ The dependencies, both core and optional, required by CDAP.
- |am-service-wizard|_ Installing CDAP using the Ambari *Add Service* Wizard, and starting CDAP.
- |am-configuring|_ After installing, .
