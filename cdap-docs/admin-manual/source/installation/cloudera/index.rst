.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _admin-cloudera-index:

======================
Cloudera Manager (CDH)
======================

.. toctree::

    Prerequisites <prerequisites>
    Installation <installation>
    Add Service Wizard <step-by-step-cloudera>
    Verification <verification>
    Upgrading CDAP <upgrading>

This section is to help you install the Cask Data Application Platform (CDAP) on Hadoop
systems that are CDH (Cloudera Data Hub) clusters managed with `Cloudera Manager
<http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__.

For other distributions, managers, or formats:

- For Apache Ambari, follow :ref:`these instructions <ambari-index>`.
- For generic Apache Hadoop distributions, CDH clusters *not* managed with
  Cloudera Manager, or HDP clusters *not* managed with Ambari, follow :ref:`these instructions <hadoop-index>`.
- For MapR, follow :ref:`these instructions <mapr-index>`.
- For the CDAP Standalone SDK, follow :ref:`these instructions <standalone-index>`. *(Developers' Manual)*

You can use `Cloudera Manager
<http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__ 
to integrate CDAP into a Hadoop cluster by downloading and installing a CDAP CSD (Custom
Service Descriptor). Once the CSD is installed, you will able to use Cloudera Manager to
install, start and manage CDAP on Hadoop clusters.

These instructions cover the steps to install CDAP using Cloudera Manager:

.. |prerequisites| replace:: **Prerequisites:**
.. _prerequisites: prerequisites.html

.. |installation-setup-startup| replace:: **Installation, Setup, and Startup:**
.. _installation-setup-startup: installation.html

.. |service-wizard| replace:: **Add Service Wizard:**
.. _service-wizard: step-by-step-cloudera.html

.. |verification| replace:: **Verification:**
.. _verification: verification.html

- |prerequisites|_ Preparing your Hadoop cluster for CDAP.
- |installation-setup-startup|_ Installing the CSD, running the *Add Service* Wizard, and starting CDAP.
- |service-wizard|_ Step-by-step instructions, if needed, for use with the wizard.
- |verification|_ Confirming that CDAP was installed and configured successfully.

There are specific instructions available for `upgrading existing CDAP installations <upgrading.html>`__.
See the 
