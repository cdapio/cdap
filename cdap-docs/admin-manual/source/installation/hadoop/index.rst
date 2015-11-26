.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _hadoop-index:

======
Hadoop
======

.. toctree::

    Installation <installation>
    Configuration <configuration>
    Starting and Verification <starting-verification.rst>
    Upgrading CDAP <upgrading>

This section is to help you install the Cask Data Application Platform (CDAP) on Hadoop
systems, either generic Apache Hadoop distributions, CDH clusters *not* managed with
Cloudera Manager, or HDP clusters *not* managed with Ambari.

For other distributions, managers, or formats:

- For Cloudera Manager, follow :ref:`these instructions <cloudera-index>`.
- For Apache Ambari, follow :ref:`these instructions <ambari-index>`.
- For MapR, follow :ref:`these instructions <mapr-index>`.
- For the CDAP Standalone SDK, follow :ref:`these instructions <standalone-index>`. *(Developers' Manual)*

These instructions cover the steps to install CDAP on Hadoop systems:

.. |installation| replace:: **Installation:**
.. _installation: installation.html

.. |configuration| replace:: **Configuration:**
.. _configuration: configuration.html

.. |verification| replace:: **Starting and Verification:**
.. _verification: starting-verification.html

- |installation|_ Covers the system, network, and software requirements, packaging
  options, and instructions for installation of the CDAP components so they work with your
  existing Hadoop cluster.
- |configuration|_ Covers the configuration options of the CDAP installation.
- |verification|_ Starting the services, (optionally) making CDAP
  highly-available, running a health check, and verifying the installation.

There are specific instructions available for `upgrading existing CDAP installations <upgrading.html>`__.
