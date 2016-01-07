.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

:hide-toc: true

.. _admin-cloudera-index:

======================
Cloudera Manager (CDH)
======================

.. toctree::

    Preparing Roles <cm-1-preparing-roles>
    Adding Parcels <cm-2-adding-parcels>
    Installing CDAP Service <cm-3-step-by-step>
    Upgrading CDAP <cm-4-upgrading>

This section is to help you install CDAP on Hadoop systems that are `CDH (Cloudera Data Hub)
<http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/
cdh_intro.html>`__ clusters managed with `Cloudera Manager (CM)
<http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/
cloudera-manager.html>`__.

You install CDAP into a CDH cluster by first downloading and installing a 
CSD (`Custom Service Descriptor <http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cm_mc_addon_services.html#concept_qbv_3jk_bn_unique_1>`__) 
for CDAP. Once the CDAP CSD is installed and you have restarted your Cloudera Manager Server, you
will able to use Cloudera Manager to install, start, and manage CDAP on CDH clusters.

These instructions assume that you are familiar with Cloudera Manager and CDH, and already
have a cluster managed by Cloudera Manager with CDH installed and running. The cluster must meet CDAP's
:ref:`hardware, network, and software requirements <admin-manual-system-requirements>`
before you install CDAP.

.. |cm-preparing-roles| replace:: **Preparing Roles:**
.. _cm-preparing-roles: cm-1-preparing-roles.html

.. |cm-adding-parcels| replace:: **Adding Parcels:**
.. _cm-adding-parcels: cm-2-adding-parcels.html

.. |cm-installing-cdap-service| replace:: **Installing the CDAP Service:**
.. _cm-installing-cdap-service: cm-3-step-by-step.html

.. |cm-upgrading| replace:: **Upgrading CDAP:**
.. _cm-upgrading: cm-4-upgrading.html

- |cm-preparing-roles|_ Preparing your Hadoop cluster for CDAP.
- |cm-adding-parcels|_ Installing the CSD, running the *Add Service* Wizard, and starting CDAP.
- |cm-installing-cdap-service|_ Step-by-step instructions, if needed, for use with the wizard.
- |cm-upgrading|_ Upgrading a CDAP installation that was installed and managed with Cloudera Manager.