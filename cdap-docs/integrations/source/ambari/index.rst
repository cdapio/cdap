.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _ambari-configuring:

============================================
Configuring and Installing CDAP using Ambari
============================================


Overview
========

.. highlight:: console

This section will familiarize you with CDAP's integration with `Apache Ambari
<https://ambari.apache.org/>`__, the open source provisioning system for `HDP (Hortonworks
Data Platform) <http://hortonworks.com/>`__.

You can use `Ambari <https://ambari.apache.org>`__ to integrate CDAP into a Hadoop cluster
by adding the `CDAP Ambari Services <https://github.com/caskdata/cdap-ambari-service>`__
to your Ambari server. Once you have restarted your server, you will able to
use the Ambari UI (Ambari Dashboard) to install, start and manage CDAP on Hadoop clusters.

These instructions cover the steps to integrate CDAP using Apache Ambari:

- **Prerequisites:** Adding the CDAP Service to Ambari.
- **Adding CDAP:** Adding CDAP to Your Cluster.
- **Installing CDAP:** Installing CDAP by running the *Add Service* Wizard, and starting CDAP.
- **Roadmap and Future Features:** Confirming that CDAP was installed and configured successfully.


Adding the CDAP Service to Ambari
=================================
To install CDAP on a cluster managed by Ambari, we have provided packages for
RHEL-compatible and Ubuntu systems, which are installed onto the Ambari management server.
These packages add CDAP to the list of available services which Ambari can install. 

To install the ``cdap-ambari-service`` package, first add the appropriate CDAP repository
to your system’s package manager by following :ref:`this procedure for installing a Cask
repository <install-preparing-the-cluster>` on your Ambari server.

The *repository version* must match the *CDAP version* which you’d like installed on your
cluster. To get the *CDAP 3.0 series,* you would install the *CDAP 3.0 repository.* The
default is to use CDAP 3.2, which has the widest compatibility with Ambari-supported
Hadoop distributions.

+---------------------------------------------------------+
| Supported Hortonworks Data Platform (HDP) Distributions |
+----------------+----------------------------------------+
| CDAP Version   | Hadoop Distributions                   |
+================+========================================+
| CDAP 3.0.x     | HDP 2.0, HDP 2.1                       |
+----------------+----------------------------------------+
| CDAP 3.1.x     | HDP 2.0, HDP 2.1, HDP 2.2              |
+----------------+----------------------------------------+
| CDAP 3.2.x     | HDP 2.0, HDP 2.1, HDP 2.2, HDP 2.3     |
+----------------+----------------------------------------+

**Note:** The CDAP Ambari service has been tested on Ambari Server 2.0 and 2.1, as
supplied from Hortonworks.


.. rubric:: Installing via APT

::

  $ sudo apt-get install -y cdap-ambari-service
  $ sudo ambari-server restart

.. rubric:: Installing via YUM

::

  $ sudo yum install -y cdap-ambari-service
  $ sudo ambari-server restart


Adding CDAP to Your Cluster
===========================

.. rubric:: Dependencies

CDAP depends on certain services being present on the cluster. There are **core
dependencies,** which must be running for CDAP system services to operate correctly, and
**optional dependencies,** which may be required for certain functionality or program types.

The host running the CDAP Master service must have the HDFS, YARN, and HBase clients
installed, as CDAP uses the command line clients of these for initialization and their
connectivity information for external service dependencies. Also, CDAP currently requires
Internet access on the CDAP service nodes until the issues `CDAP-3957
<https://issues.cask.co/browse/CDAP-3957>`__ or `AMBARI-13456
<https://issues.apache.org/jira/browse/AMBARI-13456>`__ are resolved.

.. rubric:: Core Dependencies

- **HDFS:** used as the backing file system for distributed storage
- **MapReduce2:** used for batch operations in workflows and data exploration
- **YARN:** used for running system services in containers on cluster NodeManagers
- **HBase:** used for system runtime storage and queues
- **ZooKeeper:** used for service discovery and leader election

.. rubric:: Optional Dependencies

- **Hive:** used for data exploration using SQL queries via CDAP Explore system service
- **Spark:** used for running Spark programs (on HDP 2.2+) within CDAP applications (CDAP 3.1 and later)

Installing CDAP
===============

1. In the Ambari UI (the Ambari Dashboard), start the **Add Service Wizard**.

   .. figure:: ../_images/integration-ambari/ss01-add-service.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Starting the *Add Service* Wizard

 
#. Select CDAP from the list and click *Next*. If there are core dependencies which are not
   installed on the cluster, Ambari will prompt you to install them.
 
   .. figure:: ../_images/integration-ambari/ss02-select-cdap.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Selecting *CDAP*
 
#. Next, we will assign CDAP services to hosts.

   CDAP consists of 4 daemons:
 
   - **Master:** coordinator service which launches CDAP system services into YARN
   - **Router:** serves HTTP endpoints for CDAP applications and REST API
   - **Kafka Server:** used for storing CDAP metrics and CDAP system service log data
   - **UI:** web interface to CDAP
 
   .. figure:: ../_images/integration-ambari/ss03-assign-masters.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Assigning Masters
 
   It is recommended to install all CDAP services onto an edge node (or the NameNode, for
   smaller clusters) such as in our example above. After selecting the master nodes, click
   *Next*.

#. Select hosts for the CDAP CLI client. This should be installed on every edge node on
   the cluster, or the same node as CDAP for smaller clusters.

   .. figure:: ../_images/integration-ambari/ss04-choose-clients.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Selecting hosts for *CDAP*
 
#. Click *Next* to continue with customizing CDAP.

#. On the **Customize Services** screen, click *Advanced* to bring up the CDAP configuration.
   Under *Advanced cdap-env*, you can configure heap sizes, and log and pid directories for the
   CDAP services which run on the edge nodes.

   .. figure:: ../_images/integration-ambari/ss05-config-cdap-env.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Customizing Services 1

#. Under *Advanced cdap-site*, you can configure all options for the operation and running
   of CDAP and CDAP applications.

   .. figure:: ../_images/integration-ambari/ss06-config-cdap-site.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Customizing Services 2

#. If you wish to use the CDAP Explore service (to use SQL to query CDAP data), you must: have
   Hive installed on the cluster; have the Hive client on the same host as CDAP; and set the
   ``explore.enabled`` option to true.

   .. figure:: ../_images/integration-ambari/ss07-config-enable-explore.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Enabling *CDAP Explore*

   For a complete explanation of these options, refer to the :ref:`CDAP documentation on
   cdap-site.xml <appendix-cdap-site.xml>`. After making any configuration changes, click
   *Next*.

#. Review the desired service layout and click *Deploy* to begin installing CDAP.

   .. figure:: ../_images/integration-ambari/ss08-review-deploy.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Summary of Services

#. Ambari will install CDAP and start the services.

   .. figure:: ../_images/integration-ambari/ss09-install-start-test.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Install, Start, and Test
      
#. After the services are installed and started, you will click *Next* to get to the
   Summary screen.

#. This screen shows a summary of the changes that were made to the cluster. No services
   should need to be restarted following this operation.

   .. figure:: ../_images/integration-ambari/ss10-post-install-summary.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Summary

#. Click *Complete* to complete the CDAP installation.

#. Now, you should see **CDAP** listed on the main summary screen for your cluster.

   .. figure:: ../_images/integration-ambari/ss11-main-screen.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Selecting *CDAP*

#. Selecting *CDAP* from the left, or choosing it from the Services drop-down menu, will take
   you to the CDAP service screen.

   .. figure:: ../_images/integration-ambari/ss12-cdap-screen.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** *CDAP* Service Screen
 
Congratulations! CDAP is now running on your cluster, managed by Ambari.


Roadmap and Future Features
===========================
CDAP integration with Ambari is still evolving and improving. Additional features are
planned for upcoming versions of the CDAP Ambari Service, including 
`a full smoke test of CDAP functionality after installation <https://issues.cask.co/browse/CDAP-4105>`__, 
`pre-defined alerts for CDAP services <https://issues.cask.co/browse/CDAP-4106>`__, 
`CDAP component high-availability support <https://issues.cask.co/browse/CDAP-4107>`__, 
`select CDAP metrics <https://issues.cask.co/browse/CDAP-4108>`__, 
`support for Kerberos-enabled clusters <https://issues.cask.co/browse/CDAP-4109>`__, and 
`integration with the CDAP Authentication Server <https://issues.cask.co/browse/CDAP-4110>`__.

The definition used to create the Ambari service is 
`open source <https://github.com/caskdata/cdap-ambari-service>`__; contributions, issues,
comments, and suggestions are always welcome and encouraged at `CDAP Issues <https://issues.cask.co/browse/CDAP>`__.
