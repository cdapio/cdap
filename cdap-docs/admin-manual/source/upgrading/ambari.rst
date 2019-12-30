.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _admin-upgrading-ambari:

==================================
Upgrading CDAP using Apache Ambari
==================================


Upgrading CDAP
==============
Currently, CDAP **cannot** be upgraded by using Apache Ambari.

To upgrade CDAP installations that were installed and are managed with Apache Ambari, please
follow our instructions for upgrading CDAP installations that were installed with
packages, either RPM or Debian:

  :ref:`Upgrading CDAP via Package Managers <admin-upgrading-packages-upgrading-cdap>`


Upgrading Apache Ambari
=======================
Instructions for upgrading Apache Ambari are available at `Hortonworks
<http://docs.hortonworks.com/HDPDocuments/Ambari-2.4.0.1/bk_ambari-upgrade/content/upgrading_ambari.html>`__.


Upgrading HDP
=============

.. _hdp-release-specific-upgrade-notes:

These steps cover what to do when upgrading the version of HDP of an existing CDAP installation.

**Upgrade Steps**

.. highlight:: console

1. Upgrade CDAP to a version that will support the new HDP version, following the instructions for
   :ref:`Upgrading CDAP via Package Managers <admin-upgrading-packages-upgrading-cdap>`.

#. After upgrading CDAP, start CDAP and check that it is working correctly.

#. Using the CDAP UI, stop all CDAP applications and services.

#. Upgrade to the new version of HDP, following Hortonworks' documentation on `upgrading HDP
   <http://docs.hortonworks.com/HDPDocuments/Ambari-2.4.0.1/bk_ambari-upgrade/content/upgrading_hdp_stack.html>`__.

#. Once the upgrade has completed, check that all Ambari services have been restarted.
   Restart any, if required.

#. Check that CDAP has been restarted successfully. Restart, if required.
