.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _admin-cloudera-index:

======================
Cloudera Manager (CDH)
======================

.. toctree::

    Preparing Roles <cm-preparing-roles>
    Adding Parcels <cm-adding-parcels>
    Installing CDAP Service <cm-step-by-step>
    Upgrading using CM <cm-upgrading>

..     Prerequisites <prerequisites>
..     Installation <installation>
..     Add Service Wizard <step-by-step-cloudera>
..     Verification <verification>
..     Upgrading CDAP <upgrading>


This section is to help you install the Cask Data Application Platform (CDAP) on Hadoop
systems that are CDH (Cloudera Data Hub) clusters managed with `Cloudera Manager
<http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__.

You can use `Cloudera Manager
<http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__ 
to integrate CDAP into a Hadoop cluster by downloading and installing a CDAP CSD (Custom
Service Descriptor). Once the CSD is installed, you will able to use Cloudera Manager to
install, start, and manage CDAP on Hadoop clusters.

These instructions cover the steps to install CDAP using Cloudera Manager. It's assumed
that you are familiar with Cloudera Manager, and have a cluster with it installed and
running.

.. |cm-preparing-roles| replace:: **Preparing Roles:**
.. _cm-preparing-roles: cm-preparing-roles.html

- |cm-preparing-roles|_ Preparing your Hadoop cluster for CDAP.

.. |cm-adding-parcels| replace:: **Adding Parcels:**
.. _cm-adding-parcels: cm-adding-parcels.html

- |cm-adding-parcels|_ Installing the CSD, running the *Add Service* Wizard, and starting CDAP.

.. |cm-installing-cdap-service| replace:: **Installing the CDAP Service:**
.. _cm-installing-cdap-service: cm-step-by-step.html

- |cm-installing-cdap-service|_ Step-by-step instructions, if needed, for use with the wizard.

.. |cm-upgrading| replace:: **Upgrading using Cloudera Manager:**
.. _cm-upgrading: cm-upgrading.html

- |cm-upgrading|_ Upgrading existing CDAP installations.
