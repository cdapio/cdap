.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _admin-upgrading-packages:

=============================
Upgrading CDAP using Packages
=============================

.. _admin-upgrading-packages-upgrading-cdap:

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

Upgrading CDAP Major/Minor Release Versions
-------------------------------------------
Upgrading between major/minor versions of CDAP refers to upgrading from a
|previous-short-version|\.x version to |short-version|\.x. Upgrades
between multiple major/minor versions must be done consecutively, and a
version cannot be skipped unless otherwise noted.

Upgrade Steps
-------------
These steps will stop CDAP, update the installation, run an upgrade tool for the table definitions,
and then restart CDAP:

.. highlight:: console

1. Stop all flows, services, and other programs in all your applications.

#. Stop all CDAP processes::

     $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i stop ; done

#. Update the CDAP repository definition by running either of these methods:
 
   - On RPM using Yum:

     .. include:: ../_includes/installation/installation.txt 
        :start-after: Download the Cask Yum repo definition file:
        :end-before:  .. end_install-rpm-using-yum

   - On Debian using APT:

     .. include:: ../_includes/installation/installation.txt 
        :start-after: Download the Cask APT repo definition file:
        :end-before:  .. end_install-debian-using-apt

#. Update the CDAP packages by running either of these methods:

   - On RPM using Yum::

       $ sudo yum upgrade 'cdap*'

   - On Debian using APT::

       $ sudo apt-get install --only-upgrade '^cdap.*'

#. Run the upgrade tool, as the user that runs CDAP Master (the CDAP user, indicated by ``<cdap-user>``)::

     $ sudo -u <cdap-user> /opt/cdap/master/bin/svc-master run co.cask.cdap.data.tools.UpgradeTool upgrade
     
   Note that once you have upgraded an instance of CDAP, you cannot reverse the process; down-grades
   to a previous version are not possible. Also, note that authorization is disabled in the *UpgradeTool*
   so that the ``cdap`` user can upgrade all users' data.
   
   The *UpgradeTool* will produce output similar to the following, prompting you to continue with the upgrade:
   
    .. container:: highlight

      .. parsed-literal::    
    
        UpgradeTool - version |version|-<build timestamp>.

        upgrade - Upgrades CDAP to |version|
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
   
     $ sudo -u <cdap-user> /opt/cdap/master/bin/svc-master run co.cask.cdap.data.tools.UpgradeTool upgrade force
     
#. Restart the CDAP processes::

     $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i start ; done

#. To upgrade existing data pipeline applications created using the |previous-short-version|\.x versions of
   system artifacts, there are :ref:`separate instructions on doing so <cask-hydrator-operating-upgrading-pipeline>`.


.. _admin-upgrading-packages-upgrading-hadoop:

Upgrading Hadoop
================
These tables list different versions of CDAP and the Hadoop distributions for which they are
supported. If your particular distribution is not listed here, you can determine its
components and from that determine which version of CDAP may be compatible. `Our blog
lists <http://blog.cask.co/2015/06/hadoop-components-versions-in-distros-matrix/>`__ the
different components of the common Hadoop distributions.

.. CDH
.. ---
.. include:: /installation/cloudera.rst
    :start-after: .. _cloudera-compatibility-matrix:
    :end-before: .. _cloudera-compatibility-matrix-end:

+----------------+-------------------------------+
|                |                               |
+----------------+-------------------------------+

.. HDP
.. ---
.. include:: /installation/ambari.rst
    :start-after: .. _ambari-compatibility-matrix:
    :end-before: .. _ambari-compatibility-matrix-end:

+----------------+-------------------------------+
|                |                               |
+----------------+-------------------------------+

.. MapR
.. ----
.. include:: /installation/mapr.rst
    :start-after: .. _mapr-compatibility-matrix:
    :end-before: .. _mapr-compatibility-matrix-end:


Upgrade Steps
-------------
These steps cover what to do when upgrading the version of Hadoop of an existing CDAP installation.
As the different versions of Hadoop can use different versions of HBase, upgrading from
one version to the next can require that the HBase coprocessors be upgraded to the correct
version. The steps below will, if required, update the coprocessors appropriately.

**It is important to perform these steps as described, otherwise the coprocessors may not
get upgraded correctly and HBase regionservers may crash.**

1. Upgrade CDAP to a version that will support the new Hadoop version, following the usual
   :ref:`CDAP upgrade procedure for packages <admin-upgrading-packages-upgrading-cdap>`. 

#. After upgrading CDAP, start CDAP and check that it is working correctly.

#. Stop all CDAP applications and services::
   
    $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i stop ; done

#. Disable all CDAP tables; from an HBase shell, run the command::

    > disable_all 'cdap.*'
    
#. Upgrade to the new version of Hadoop.

#. Run the *Post-Hadoop Upgrade Tasks* |---| to upgrade CDAP for the new version of Hadoop |---| by running
   the *CDAP Upgrade Tool*, as the user that runs CDAP Master (the CDAP user, indicated by ``<cdap-user>``)::

    $ sudo -u <cdap-user> /opt/cdap/master/bin/svc-master run co.cask.cdap.data.tools.UpgradeTool upgrade_hbase

#. Enable all CDAP tables; from an HBase shell, run this command::

    > enable_all 'cdap.*'
    
#. Restart CDAP::

    $ for i in `ls /etc/init.d/ | grep cdap` ; do sudo service $i start ; done

