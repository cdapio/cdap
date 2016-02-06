.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _admin-upgrading-packages:

=============================
Upgrading CDAP using Packages
=============================

.. _admin-upgrading-packages-cdap:

Upgrading CDAP
==============
When upgrading an existing CDAP installation from a previous version, you will need
to make sure the CDAP table definitions in HBase are up-to-date.

These steps will stop CDAP, update the installation, run an upgrade tool for the table definitions,
and then restart CDAP.

**These steps will upgrade from CDAP 3.2.x to 3.3.x.** If you are on an earlier version of CDAP,
please follow the upgrade instructions for the earlier versions and upgrade first to 3.2.x before proceeding.

.. highlight:: console

1. Stop all flows, services, and other programs in all your applications.

#. Stop all CDAP processes::

     $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i stop ; done

#. Update the CDAP file definition lists by running either of these methods:
 
   - On RPM using Yum:

     .. include:: ../_includes/installation/installation.txt 
        :start-after: Download the Cask Yum repo definition file:
        :end-before:  .. end_install-rpm-using-yum

   - On Debian using APT:

     .. include:: ../_includes/installation/installation.txt 
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
     
#. To upgrade existing ETL applications created using the 3.2.x versions of ``cdap-etl-batch`` or 
   ``cdap-etl-realtime``, there is are :ref:`separate instructions on doing so <cdap-apps-etl-upgrade>`.

#. Restart the CDAP processes::

     $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i start ; done


Upgrading Hadoop
================

.. _admin-upgrading-packages-hadoop:

Upgrading Hadoop using Packages
-------------------------------

<TO BE COMPLETED>

.. _admin-upgrading-packages-hadoop-cdh:

Upgrading CDH
-------------
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
   :ref:`CDAP upgrade procedure for packages <admin-upgrading-packages-cdap>`. 

#. After upgrading CDAP, start CDAP and check that it is working correctly.

#. Stop all CDAP application and services::
   
    $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i stop ; done

#. Disable all CDAP tables; from an HBase shell, run the command::

    > disable_all 'cdap.*'
    
#. Upgrade to the new version of CDH.

#. Run the *Post-CDH Upgrade Tasks* |---| to upgrade CDAP for the new version of CDH |---| by running
   the *CDAP Upgrade Tool*, as the user that runs CDAP Master (the CDAP user)::

    $ /opt/cdap/master/bin/svc-master run co.cask.cdap.data.tools.UpgradeTool upgrade_hbase
    
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
    
#. Start CDAP::

    $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i start ; done


.. _admin-upgrading-packages-hadoop-hdp:

Upgrading HDP
-------------
<TO BE COMPLETED>


.. _admin-upgrading-packages-hadoop-mapr:

Upgrading MapR
--------------
<TO BE COMPLETED>
