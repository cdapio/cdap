.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _admin-upgrading-cloudera:

===================================
Upgrading CDAP and Cloudera Manager
===================================


.. _admin-upgrading-cloudera-cdap:

Upgrading CDAP
==============

Upgrading CDAP Patch Release Versions
-------------------------------------
When a new compatible CDAP parcel is released, it will be available via the Parcels page
in the Cloudera Manager UI.

#. Stop all flows, services, and other programs in all your applications.

#. Stop CDAP services.

#. Use the Cloudera Manager UI to download, distribute, and activate the parcel on all
   cluster hosts.

#. Start CDAP services.

Upgrading CDAP Major/Minor Release Versions
-------------------------------------------
Upgrading between major versions of CDAP involves the additional steps of upgrading the
CSD, and running the included CDAP Upgrade Tool. Upgrades between multiple Major/Minor
versions must be done consecutively, and a version cannot be skipped unless otherwise
noted.

The following is the generic procedure for Major/Minor version upgrades:

#. Stop all flows, services, and other programs in all your applications.

#. Stop all CDAP services.

#. Ensure your installed version of the CSD matches the target version of CDAP. For
   example, CSD version 3.0.* is compatible with CDAP version 3.0.*.  Download `the latest
   version of the CSD <http://cask.co/resources/#cdap-integrations>`__.

#. Use the Cloudera Manager UI to download, distribute, and activate the corresponding
   CDAP parcel version on all cluster hosts.

#. Before starting services, run the *CDAP Upgrade Tool* to upgrade CDAP. From the CDAP Service 
   page, select "Run CDAP Upgrade Tool" from the Actions menu.

#. Start the CDAP services.  At this point it may be necessary to correct for any changes in
   the CSD.  For example, if new CDAP services were added or removed, you must add or
   remove role instances as necessary. Check the :ref:`release-specific upgrade notes
   <cloudera-release-specific-upgrade-notes>` below for any specific instructions.
   
#. After CDAP services have started, run the *CDAP Post-Upgrade Tasks* to perform any necessary
   upgrade steps against the running services.  From the CDAP Service page, select "Run CDAP
   Post-Upgrade Tasks."

#. To upgrade existing ETL applications created using the 3.2.x versions of ``cdap-etl-batch`` or 
   ``cdap-etl-realtime``, there is are :ref:`separate instructions on doing so <cdap-apps-etl-upgrade>`.

#. You must recompile and then redeploy your applications prior to using them.


Upgrading CDH
=============

.. _cloudera-release-specific-upgrade-notes:

These steps cover upgrading the version of CDH of an existing CDAP installation.
As the different versions of CDH can use different versions of HBase, upgrading from
one version to the next can require that the HBase coprocessors be upgraded to the correct
version. The table below lists the different coprocessor package names managed by CDAP
for each version of CDH. If the version changes, you need to check that the version being
used has changed as described below.

+-------------+-------------------------------------+
| CDH Version | CDAP HBase Coprocessor Package Name |
+=============+=====================================+
| 5.5         | ``hbase10cdh550``                   |
+-------------+-------------------------------------+
| 5.4         | ``hbase10cdh``                      |
+-------------+-------------------------------------+
| 5.3         | ``hbase98``                         |
+-------------+-------------------------------------+
| 5.2         | ``hbase98``                         |
+-------------+-------------------------------------+
| 5.1         | |---|                               |
+-------------+-------------------------------------+

**For example:** CDH 5.3 ships with HBase 0.98 while CDH 5.4 ships with HBase 1.0. We support
CDH 5.4 as of CDAP 3.1.0 |---| however, upgrading the underlying CDH version is only supported
since CDAP 3.2.0. Therefore, before upgrading from CDH 5.3 to CDH 5.4, upgrade CDAP to version
3.2.0 or greater, following the normal upgrade procedure. Start CDAP at least once to make sure
it works properly, before you upgrade to CDH 5.4.

**It is important to perform these steps as described, otherwise the coprocessors may not
get upgraded correctly and HBase regionservers may crash.** In the case where something
goes wrong, see these troubleshooting instructions for :ref:`problems while upgrading CDH
<faqs-cloudera-troubleshooting-upgrade-cdh>`.

**Upgrade Steps**

1. Upgrade CDAP to a version that will support the new CDH version, following the usual
   :ref:`CDAP-Cloudera Manager upgrade procedure <admin-upgrading-cloudera-cdap>`. 

#. After upgrading CDAP, start CDAP and check that it is working correctly.

#. Using Cloudera Manager (CM), stop all CDAP application and services.

#. Disable all CDAP tables; from an HBase shell, run the command::

    > disable_all 'cdap.*'
    
#. Upgrade to the new version of CDH.

#. Stop all CDAP services, as CM will have (again) auto-started CDAP.

#. Run the *Post-CDH Upgrade Tasks* to upgrade CDAP for the new version of CDH. From the CDAP Service 
   page, select "Run Post-CDH Upgrade Tasks" from the Actions menu.
       
#. Check if the coprocessor JARs for all CDAP tables have been upgraded to the correct version
   as listed in the table above by checking that the coprocessor classnames are using the
   correct package |---| for example, if upgrading from CDH 5.3 to 5.4, the new
   coprocessor package is ``hbase10cdh`` and a classname using it would be
   ``co.cask.cdap.data2.transaction.coprocessor.hbase10cdh.DefaultTransactionProcessor``.
  
   Running this command in an HBase shell will give you table attributes::
  
    > describe 'cdap_system:app.meta'
    
   The resulting output will show the coprocessor classname; in this case, we are looking for
   the inclusion of ``hbase10cdh`` in the name::
  
    'cdap_system:app.meta', {TABLE_ATTRIBUTES => {coprocessor$1 =>
    'hdfs://server.example.com/cdap/cdap/lib/
    coprocessorb5cb1b69834de686a84d513dff009908.jar|co.cask.cdap.data2.transaction.
    coprocessor.hbase10cdh.DefaultTransactionProcessor|1073741823|', METADATA =>
    {'cdap.version' => '3.1.0...

   Note that some CDAP tables do not have any coprocessors. You only need to verify tables
   that **have** coprocessors.

#. Enable all CDAP tables; from an HBase shell, run this command::

    > enable_all 'cdap.*'
    
#. Start CDAP using Cloudera Manager.
