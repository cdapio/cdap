.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cloudera-upgrading:

=========================================================
Cloudera Manager: Upgrading an Existing CDAP Installation
=========================================================

Upgrading Patch Release versions
--------------------------------
When a new compatible CDAP parcel is released, it will be available via the Parcels page in the Cloudera Manager UI.

#. Stop all flows, services, and other programs in all your applications.

#. Stop CDAP services.

#. Use the Cloudera Manager UI to download, distribute, and activate the parcel on all cluster hosts.

#. Start CDAP services.

Upgrading Major/Minor Release versions
--------------------------------------
Upgrading between major versions of CDAP involves the additional steps of upgrading the CSD, and running the included
CDAP Upgrade Tool. Upgrades between multiple Major/Minor versions must be done consecutively, and a version cannot be
skipped unless otherwise noted.

The following is the generic procedure for Major/Minor version upgrades:

#. Stop all flows, services, and other programs in all your applications.

#. Stop CDAP services.

#. Ensure your installed version of the CSD matches the target version of CDAP. For example, CSD version 3.0.* is compatible
   with CDAP version 3.0.*.  Download the latest version of the CSD `here <http://cask.co/resources/#cdap-integrations>`__.

#. Use the Cloudera Manager UI to download, distribute, and activate the corresponding CDAP parcel version on all cluster
   hosts.

#. Before starting services, run the Upgrade Tool to update any necessary CDAP table definitions. From the CDAP Service
   page, select "Run CDAP Upgrade Tool" from the Actions menu.

#. Start the CDAP services.  At this point it may be necessary to correct for any changes in the CSD.  For example, if new CDAP services
   were added or removed, you must add or remove role instances as necessary. Check the
   :ref:`release-specific upgrade notes <cloudera-release-specific-upgrade-notes>` below for any specific instructions.

#. After CDAP services have started, run the Post-Upgrade tool to perform any necessary upgrade steps against the running services.  From the
   CDAP Service page, select "Run CDAP Post-Upgrade Tasks."

#. You must recompile and then redeploy your applications.

.. _cloudera-release-specific-upgrade-notes:

Upgrading CDH 5.3 to 5.4
------------------------
**Background:** CDH 5.3 ships with HBase 0.98 while CDH 5.4 ships with HBase 1.0. We support
CDH 5.4 as of CDAP 3.1.0 - however, upgrading the underlying CDH version is only supported
since CDAP 3.2.0. Therefore, before upgrading from CDH 5.3 to CDH 5.4, upgrade CDAP to version
3.2.0 or greater, following the normal upgrade procedure. Start CDAP at least once to make sure
it works properly, before you upgrade to CDH 5.4.

**It is important to perform these steps as described, otherwise the coprocessors may not
get upgraded correctly and HBase regionservers may crash.** In the case where something
goes wrong, see these troubleshooting instructions for :ref:`problems while upgrading CDH
<faqs-cloudera-troubleshooting-upgrade-cdh>`.

**Upgrade Steps**

1. If using Cloudera Manager, :ref:`stop all CDAP application and services
   <hadoop-upgrading>`, as Cloudera Manager will have auto-started CDAP
#. Disable all CDAP tables; from an HBase shell, run this command::

    > disable_all 'cdap.*'
    
#. Upgrade to CDH 5.4
#. :ref:`Stop all CDAP services <hadoop-upgrading>`, as CDH will have auto-started CDAP
#. Run the CDAP Upgrade Tool, as the user that runs CDAP Master (the CDAP user)::

    $ /opt/cdap/master/bin/svc-master run co.cask.cdap.data.tools.UpgradeTool upgrade_hbase
    
#. Check if the coprocessor JARs for all CDAP tables have been upgraded to CDH HBase 1.0,
   by checking that the coprocessor classnames are using the ``hbase10cdh`` package |---|
   for example, ``co.cask.cdap.data2.transaction.coprocessor.hbase10cdh.DefaultTransactionProcessor``
  
   Running this command in an HBase shell will give you table attributes::
  
    > describe 'cdap_system:app.meta'
    
   The resulting output will show the coprocessor classname::
  
    'cdap_system:app.meta', {TABLE_ATTRIBUTES => {coprocessor$1 =>
    'hdfs://server.example.com/cdap/cdap/lib/
    coprocessorb5cb1b69834de686a84d513dff009908.jar|co.cask.cdap.data2.transaction.
    coprocessor.hbase10cdh.DefaultTransactionProcessor|1073741823|', METADATA =>
    {'cdap.version' => '3.1.0...

   Note that some CDAP tables do not have any coprocessors. You only need to verify tables
   that have coprocessors.

#. Enable all CDAP tables; from an HBase shell, run this command::

    > enable_all 'cdap.*'
    
#. Start CDAP


Upgrading CDAP 2.8 to 3.0
-------------------------
**Note:** Apps need to be both recompiled and re-deployed.

When upgrading from 2.8.0 to 3.0.0, the CDAP Web-App role has been replaced by the CDAP-UI
role.  After starting the 3.0 services for the first time:

   - From the CDAP Instances page, select "Add Role Instances", and choose a host for the CDAP-UI role.

   - From the CDAP Instances page, check the CDAP-Web-App role, and select "Delete" from the Actions menu.
