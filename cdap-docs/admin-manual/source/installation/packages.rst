.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

:section-numbering: true

.. _admin-installation-packages:

==================================
Manual Installation using Packages
==================================

.. include:: ../_includes/installation/installation-steps-images.txt

.. rubric:: Notes

This section describes installing CDAP on Hadoop clusters that are:

- Generic Apache Hadoop distributions; 
- CDH (Cloudera Data Hub) clusters *not managed* with Cloudera Manager; or
- HDP (Hortonworks Data Platform) clusters *not managed* with Apache Ambari.

Cloudera Manager (CDH), Apache Ambari (HDP), and MapR distributions should be installed
with our other :ref:`distribution instructions <installation-index>`.

Preparing the Cluster
=============================
Please review the :ref:`Software Prerequisites <admin-manual-software-requirements>`, 
as a configured Hadoop, HBase, and Hive (plus an optional Spark client) needs to be configured on the
node(s) where CDAP will run. 

.. Node.js Installation
.. --------------------
.. include:: /../target/_includes/packages-installation.rst
    :start-after: .. _packages-install-node-js:
    :end-before: .. _packages-install-packaging:

.. Hadoop Configuration
.. --------------------
.. include:: ../_includes/installation/hadoop-configuration.txt

.. HDFS Permissions
.. ----------------
.. include:: ../_includes/installation/hdfs-permissions.txt

  
Downloading and Distributing Packages
=====================================

Preparing Package Managers
--------------------------

.. include:: /../target/_includes/packages-installation.rst
    :start-after: .. _packages-preparing-package-managers:
    :end-before: .. end_install-debian-using-apt


Installing CDAP Services
========================

.. include:: /../target/_includes/packages-installation.rst
    :start-after: .. _packages-package-installation-title:


.. |display-distribution| replace:: Package Managers (RPM/Debian)

.. |hdfs-user| replace:: ``yarn``

.. include:: /../target/_includes/packages-configuration.rst
    :end-before: .. configuration-enabling-kerberos:


.. Starting CDAP Services
.. ======================

.. include:: /../target/_includes/packages-starting.rst

.. _packages-verification:

Verification
============

.. include:: /_includes/installation/smoke-test-cdap.txt


Advanced Topics
===============

.. _packages-configuration-security:

.. Enabling Perimeter Security
.. ---------------------------
.. include:: /../target/_includes/packages-configuration.rst
    :start-after: .. _packages-configuration-eps:

.. _packages-configuration-enabling-kerberos:

.. Enabling Kerberos
.. -----------------
.. include:: /../target/_includes/packages-configuration.rst
    :start-after: .. configuration-enabling-kerberos:
    :end-before: .. _packages-configuration-eps:

.. _upgrading-using-packages:

Upgrading CDAP
--------------
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


.. _packages-highly-available:

CDAP HA setup
-------------
Repeat the installation steps on additional boxes. The configuration settings (in
``cdap-site.xml``, *property:value*) needed to support high-availability are:

- ``kafka.seed.brokers``: ``127.0.0.1:9092,.../${root.namespace}`` 
  
  - Kafka brokers list (comma-separated), followed by ``/${root.namespace}``
  
- ``kafka.default.replication.factor``: 2

  - Used to replicate Kafka messages across multiple machines to prevent data loss in 
    the event of a hardware failure.
  - The recommended setting is to run at least two Kafka servers.
  - Set this to the number of Kafka servers.
