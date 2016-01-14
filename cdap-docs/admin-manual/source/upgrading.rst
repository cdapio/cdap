.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _upgrading:

==============
Upgrading CDAP
==============

Choose an upgrade path based on your distribution:

- :ref:`Upgrading using Cloudera Manager <upgrading-using-cloudera-manager>`
- :ref:`Upgrading using Apache Ambari <upgrading-using-apache-ambari>`
- :ref:`Upgrading using MapR <upgrading-using-mapr>`
- :ref:`Upgrading using Package Managers (RPM/Debian) <upgrading-using-package-managers>`

.. _upgrading-using-cloudera-manager:

Using Cloudera Manager
======================

Upgrading Patch Release Versions
--------------------------------
When a new compatible CDAP parcel is released, it will be available via the Parcels page
in the Cloudera Manager UI.

#. Stop all flows, services, and other programs in all your applications.

#. Stop CDAP services.

#. Use the Cloudera Manager UI to download, distribute, and activate the parcel on all
   cluster hosts.

#. Start CDAP services.

Upgrading Major/Minor Release Versions
--------------------------------------
Upgrading between major versions of CDAP involves the additional steps of upgrading the
CSD, and running the included CDAP Upgrade Tool. Upgrades between multiple Major/Minor
versions must be done consecutively, and a version cannot be skipped unless otherwise
noted.

The following is the generic procedure for Major/Minor version upgrades:

#. Stop all flows, services, and other programs in all your applications.

#. Stop CDAP services.

#. Ensure your installed version of the CSD matches the target version of CDAP. For
   example, CSD version 3.0.* is compatible with CDAP version 3.0.*.  Download `the latest
   version of the CSD <http://cask.co/resources/#cdap-integrations>`__.

#. Use the Cloudera Manager UI to download, distribute, and activate the corresponding
   CDAP parcel version on all cluster hosts.

#. Before starting services, run the Upgrade Tool to update any necessary CDAP table
   definitions. From the CDAP Service page, select "Run CDAP Upgrade Tool" from the
   Actions menu.

#. Start the CDAP services.  At this point it may be necessary to correct for any changes in
   the CSD.  For example, if new CDAP services were added or removed, you must add or
   remove role instances as necessary. Check the :ref:`release-specific upgrade notes
   <cloudera-release-specific-upgrade-notes>` below for any specific instructions.
   
#. After CDAP services have started, run the Post-Upgrade tool to perform any necessary
   upgrade steps against the running services.  From the CDAP Service page, select "Run CDAP
   Post-Upgrade Tasks."

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

1. If using Cloudera Manager (CM), stop all CDAP application and services
   as CM will have auto-started CDAP::
   
    $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i stop ; done

#. Disable all CDAP tables; from an HBase shell, run the command::

    > disable_all 'cdap.*'
    
#. Upgrade to CDH 5.4.
#. Stop all CDAP services, as CM will have (again) auto-started CDAP::

    $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i stop ; done

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
   that **have** coprocessors.

#. Enable all CDAP tables; from an HBase shell, run this command::

    > enable_all 'cdap.*'
    
#. Start CDAP::

    $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i start ; done


Upgrading CDAP 2.8 to 3.0
-------------------------
**Note:** Apps need to be both recompiled and re-deployed.

When upgrading from 2.8.0 to 3.0.0, the CDAP Web-App role has been replaced by the CDAP-UI
role.  After starting the 3.0 services for the first time:

   - From the CDAP Instances page, select "Add Role Instances", and choose a host for the CDAP-UI role.

   - From the CDAP Instances page, check the CDAP-Web-App role, and select "Delete" from the Actions menu.


.. _upgrading-using-apache-ambari:

Using Apache Ambari
===================
Currently, CDAP **cannot** be upgraded by using Apache Ambari. 

To upgrade CDAP installations that were installed and are managed with Apache Ambari, please
follow our instructions for upgrading CDAP installations that were installed with a
Package Manager, either RPM or Debian:

  :ref:`upgrading-using-package-managers`


.. _upgrading-using-mapr:

Using MapR 
===========
Currently, CDAP **cannot** be upgraded by using the MapR Control System. 

To upgrade CDAP installations that were installed and are managed with MapR, please
follow our instructions for upgrading CDAP installations that were installed with a
Package Manager, either RPM or Debian:

  :ref:`upgrading-using-package-managers`


.. _upgrading-using-package-managers:

Using Package Managers (RPM/Debian)
===================================
When upgrading an existing CDAP installation from a previous version, you will need
to make sure the CDAP table definitions in HBase are up-to-date.

These steps will stop CDAP, update the installation, run an upgrade tool for the table definitions,
and then restart CDAP.

These steps will upgrade from CDAP 3.1.x to 3.2.x. If you are on an earlier version of CDAP,
please follow the upgrade instructions for the earlier versions and upgrade first to 3.1.x before proceeding.

.. highlight:: console

1. Stop all flows, services, and other programs in all your applications.

#. Stop all CDAP processes::

     $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i stop ; done

#. Update the CDAP file definition lists by running either of these methods:
 
   - On RPM using Yum:

     .. include:: _includes/installation/installation.txt 
        :start-after: Download the Cask Yum repo definition file:
        :end-before:  .. end_install-rpm-using-yum

   - On Debian using APT:

     .. include:: _includes/installation/installation.txt 
        :start-after: Download the Cask APT repo definition file:
        :end-before:  .. end_install-debian-using-apt

#. Update the CDAP packages by running either of these methods:

   - On RPM using Yum (on one line)::

       $ sudo yum install cdap cdap-gateway \
             cdap-hbase-compat-0.96 cdap-hbase-compat-0.98 cdap-hbase-compat-1.0 \
             cdap-hbase-compat-1.0-cdh cdap-hbase-compat-1.1 \
             cdap-kafka cdap-master cdap-security cdap-ui

   - On Debian using APT (on one line)::

       $ sudo apt-get install cdap cdap-gateway \
             cdap-hbase-compat-0.96 cdap-hbase-compat-0.98 cdap-hbase-compat-1.0 \
             cdap-hbase-compat-1.0-cdh cdap-hbase-compat-1.1 \
             cdap-kafka cdap-master cdap-security cdap-ui

#. If you are upgrading a secure Hadoop cluster, you should authenticate with ``kinit``
   as the user that runs CDAP Master (the CDAP user)
   before the next step (the running of the upgrade tool)::

     $ kinit -kt <keytab> <principal>

#. Run the upgrade tool, as the user that runs CDAP Master (the CDAP user)::

     $ /opt/cdap/master/bin/svc-master run co.cask.cdap.data.tools.UpgradeTool upgrade
     
   Note that once you have upgraded an instance of CDAP, you cannot reverse the process; down-grades
   to a previous version are not possible.
   
   The Upgrade Tool will produce output similar to the following, prompting you to continue with the upgrade:
   
    .. container:: highlight

      .. parsed-literal::    
    
        UpgradeTool - version |short-version|-xxxxx.

        upgrade - Upgrades CDAP to |short-version|
          The upgrade tool upgrades the following:
          1. User Datasets
              - Upgrades the coprocessor jars for tables
              - Migrates the metadata for PartitionedFileSets
          2. System Datasets
          3. UsageRegistry Dataset Type
          Note: Once you run the upgrade tool you cannot rollback to the previous version.
        Do you want to continue (y/n)
        y
        Starting upgrade ...

   You can run the tool in a non-interactive fashion by using the ``force`` flag, in which case
   it will run unattended and not prompt for continuing::
   
     $ /opt/cdap/master/bin/svc-master run co.cask.cdap.data.tools.UpgradeTool upgrade force

#. Restart the CDAP processes::

     $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i start ; done
