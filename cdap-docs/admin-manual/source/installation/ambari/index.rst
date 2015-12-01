.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _ambari-index:

===================
Apache Ambari (HDP)
===================

.. toctree::

    Adding CDAP Service <prerequisites>
    Adding CDAP <adding-cdap>
    Installing CDAP <installing-cdap>
    Roadmap <roadmap>

This section is to help you install the Cask Data Application Platform (CDAP) on Hadoop
systems that are `HDP (Hortonworks Data Platform) <http://hortonworks.com/>`__ clusters
managed with `Apache Ambari <https://ambari.apache.org/>`__, the open source provisioning
system for HDP.

For other distributions, managers, or formats:

- For Cloudera Manager, follow :ref:`these instructions <cloudera-index>`.
- For generic Apache Hadoop distributions, CDH clusters *not* managed with
  Cloudera Manager, or HDP clusters *not* managed with Ambari, follow :ref:`these instructions <hadoop-index>`.
- For MapR, follow :ref:`these instructions <mapr-index>`.
- For the CDAP Standalone SDK, follow :ref:`these instructions <standalone-index>`. *(Developers' Manual)*

You can use Ambari to integrate CDAP into a Hadoop cluster
by adding the `CDAP Ambari Services <https://github.com/caskdata/cdap-ambari-service>`__
to your Ambari server. Once you have restarted your server, you will able to
use the Ambari UI (Ambari Dashboard) to install, start, and manage CDAP on Hadoop clusters.

These instructions cover the steps to integrate CDAP using Apache Ambari:

.. |prerequisites| replace:: **Adding the CDAP Service to Ambari:**
.. _prerequisites: prerequisites.html

.. |adding-cdap| replace:: **Adding CDAP to Your Cluster:**
.. _adding-cdap: adding-cdap.html

.. |installing-cdap| replace:: **Installing CDAP:**
.. _installing-cdap: installing-cdap.html

.. |roadmap| replace:: **Roadmap and Future Features:**
.. _roadmap: roadmap.html

- |prerequisites|_ The prerequisite steps of preparing the package manager.
- |adding-cdap|_ The dependencies, both core and optional, required by CDAP.
- |installing-cdap|_ Installing CDAP by running the Ambari *Add Service* Wizard, and starting CDAP.
- |roadmap|_ Additional features are planned for upcoming versions of the CDAP Ambari Service.
