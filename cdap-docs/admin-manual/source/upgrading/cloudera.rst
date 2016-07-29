.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _admin-upgrading-cloudera:

=====================================
Upgrading CDAP using Cloudera Manager
=====================================


.. _admin-upgrading-cloudera-upgrading-cdap:

Upgrading CDAP
==============
When upgrading an existing CDAP installation from a previous version, you will need
to make sure the CDAP table definitions in HBase are up-to-date.

**These steps will upgrade from CDAP** |bold-previous-short-version|\ **.x to**
|bold-version|\ **.** If you are on an earlier version of CDAP, please follow the
upgrade instructions for the earlier versions and upgrade first to
|previous-short-version|\.x before proceeding.

Upgrading CDAP Patch Release Versions
-------------------------------------
Upgrading between patch versions of CDAP refers to upgrading from one |short-version|\.x
version to a higher |short-version|\.x version.
When a new compatible CDAP parcel is released, it will be available via the Parcels page
in the Cloudera Manager UI.

Upgrading CDAP Major/Minor Release Versions
-------------------------------------------
Upgrading between major versions of CDAP (for example, from a |previous-short-version|\.x version 
to |short-version|\.x) involves the additional step of upgrading the
CSD. Upgrades between multiple Major/Minor
versions must be done consecutively, and a version cannot be skipped unless otherwise
noted.

The following is the generic procedure for all upgrades. These steps will stop CDAP,
update the installation, run an upgrade tool for the table definitions, and then restart
CDAP:

.. highlight:: console

#. Stop all flows, services, and other programs in all your applications.

#. Stop all CDAP services.

#. Ensure your installed version of the CSD matches the target version of CDAP. For
   example, CSD version 3.0.* is compatible with CDAP version 3.0.*.  Download `the latest
   version of the CSD <http://cask.co/downloads/#cloudera>`__.

#. Use the Cloudera Manager UI to download, distribute, and activate the target
   CDAP parcel version on all cluster hosts.

#. Before starting services, run the *CDAP Upgrade Tool* to upgrade CDAP. From the CDAP Service 
   page, select "Run CDAP Upgrade" from the Actions menu.

#. Start the CDAP services.  At this point it may be necessary to correct for any changes in
   the CSD.  For example, if new CDAP services were added or removed, you must add or
   remove role instances as necessary. Check the :ref:`release-specific upgrade notes
   <cloudera-release-specific-upgrade-notes>` below for any specific instructions.
   
#. After CDAP services have started, run the *CDAP Post-Upgrade Tasks* to perform any necessary
   upgrade steps against the running services.  From the CDAP Service page, select "Run CDAP
   Post-Upgrade Tasks."

#. To upgrade existing ETL applications created using the |previous-short-version|\.x versions of 
   the system artifacts, there are :ref:`separate instructions <cdap-apps-etl-upgrade>`.

#. You must recompile and then redeploy your applications prior to using them.


Upgrading CDH
=============

.. _cloudera-release-specific-upgrade-notes:

These steps cover what to do when upgrading the version of CDH of an existing CDAP installation.
As the different versions of CDH can use different versions of HBase, upgrading from
one version to the next can require that the HBase coprocessors be upgraded to the correct
version. The steps below will, if required, update the coprocessors appropriately.

**It is important to perform these steps as described, otherwise the coprocessors may not
get upgraded correctly and HBase regionservers may crash.** In the case where something
goes wrong, see these troubleshooting instructions for :ref:`problems while upgrading CDH
<faqs-cloudera-troubleshooting-upgrade-cdh>`.

**Upgrade Steps**

.. highlight:: console

1. Upgrade CDAP to a version that will support the new CDH version, following the usual
   :ref:`CDAP-Cloudera Manager upgrade procedure <admin-upgrading-cloudera-upgrading-cdap>`. 

#. After upgrading CDAP, start CDAP and check that it is working correctly.

#. Using Cloudera Manager (CM), stop all CDAP applications and services.

#. Disable all CDAP tables; from an HBase shell, run the command::

    > disable_all 'cdap.*'
    
#. Upgrade to the new version of CDH, following Cloudera's `documentation on upgrading
   <http://www.cloudera.com/documentation/enterprise/latest/topics/cm_mc_upgrading_cdh.html>`__.

#. Stop all CDAP services, as CM may have auto-started CDAP.

#. Run the *Post-CDH Upgrade Tasks* to upgrade CDAP for the new version of CDH. From the CDAP Service 
   page, select "Run Post-CDH Upgrade Tasks" from the Actions menu.

#. Enable all CDAP tables; from an HBase shell, run this command::

    > enable_all 'cdap.*'
    
#. Start CDAP using Cloudera Manager.
