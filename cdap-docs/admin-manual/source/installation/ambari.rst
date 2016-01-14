.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

:section-numbering: true

.. _ambari-index:

===================
Apache Ambari (HDP)
===================

This section describes installing CDAP on Hadoop systems that are `HDP (Hortonworks Data
Platform) <http://hortonworks.com/>`__ clusters managed with `Apache Ambari
<https://ambari.apache.org/>`__, the open source provisioning system for HDP.

You install CDAP into an HDP cluster by first adding the `CDAP Ambari Services
<https://github.com/caskdata/cdap-ambari-service>`__ to your Ambari Server. Once you have
restarted your Ambari Server, you will able to use the Ambari UI (Ambari Dashboard) to
install, start, and manage CDAP on HDP clusters.

These instructions assume that you are familiar with Apache Ambari and HDP, and already
have a cluster with them installed and running. The cluster must meet CDAP's
:ref:`hardware, network, and software requirements <admin-manual-system-requirements>`
before you install CDAP.

Follow these steps:

.. figure:: ../_images/steps/ambari.png
   :height: 80px
   :align: center
   

.. rubric:: Notes

- Apache Ambari can only be used to add CDAP to an **existing** Hadoop cluster, one
  that already has the required services (Hadoop: HDFS, YARN, HBase, ZooKeeper, and |---|
  optionally |---| Hive and Spark) installed.
- Ambari is for setting up HDP on bare clusters; it can't be used for clusters with HDP 
  already installed, where the original installation was **not** with Ambari.
- Though you can install CDAP with Apache Ambari, you **currently cannot use** Ambari to upgrade CDAP. 
  See :ref:`upgrading-using-package-managers` for how to upgrade CDAP servers managed with Ambari.
- These features are **currently not included** in the CDAP Apache Ambari Service (though they may in the future):
  
  - `Kerberos-enabled clusters <https://issues.cask.co/browse/CDAP-4109>`__ are currently not supported;
  - The CDAP Apache Ambari Service is not integrated with the `CDAP Authentication Server <https://issues.cask.co/browse/CDAP-4110>`__;
  - CDAP component `high-availability <https://issues.cask.co/browse/CDAP-4107>`__  is not supported;

- A number of features are currently planned to be added, including:

  - `pre-defined alerts <https://issues.cask.co/browse/CDAP-4106>`__  for CDAP services ;
  - select `CDAP metrics <https://issues.cask.co/browse/CDAP-4108>`__; and
  - a full `smoke test of CDAP functionality <https://issues.cask.co/browse/CDAP-4105>`__ after installation.


Setting Up the CDAP Repos
=========================

To install CDAP on a cluster managed by Ambari, we have available packages for
RHEL-compatible and Ubuntu systems, which you can install onto your Ambari management server.
These packages add CDAP to the list of available services which Ambari can install. 

.. highlight:: console

Preparing Ambari
----------------
To install the ``cdap-ambari-service`` package, first add the appropriate CDAP repository
to your system’s package manager by following the steps below. These steps will install a
Cask repository on your Ambari server.

The **repository version** (shown in the commands below as ``"cdap/``\ |literal-short-version|\ ``"``) must match the
**CDAP version** which you’d like installed on your cluster. To install the *CDAP 3.0 series,*
you would install the *CDAP 3.0 repository.* The default is to use **cdap/3.2**, which has the
widest compatibility with Ambari-supported Hadoop distributions.

Replace |---| in the commands that follow on this page |---| all references to ``"cdap/``\ |literal-short-version|\ ``"`` 
with the CDAP Repository from the list below that you would like to use:

+-----------------------------------------------------------------------+
| Supported Hortonworks Data Platform (HDP) Distributions               |
+----------------+-----------------+------------------------------------+
| CDAP Version   | CDAP Repository | Hadoop Distributions               |
+================+=================+====================================+
| CDAP 3.0.x     | ``cdap/3.0``    | HDP 2.0, HDP 2.1                   |
+----------------+-----------------+------------------------------------+
| CDAP 3.1.x     | ``cdap/3.1``    | HDP 2.0, HDP 2.1, HDP 2.2          |
+----------------+-----------------+------------------------------------+
| CDAP 3.2.x     | ``cdap/3.2``    | HDP 2.0, HDP 2.1, HDP 2.2, HDP 2.3 |
+----------------+-----------------+------------------------------------+

**Note:** The CDAP Ambari service has been tested on Ambari Server 2.0 and 2.1, as
supplied from Hortonworks.

.. include:: /../target/_includes/ambari-installation.rst
  :start-after: .. _ambari-install-rpm-using-yum:
  :end-before: .. _ambari-package-installation-title:


Installing the CDAP Service
---------------------------
Now, install the ``cdap-ambari-service`` package from the repo you specified above:

Installing the CDAP Service via YUM
...................................
::

  $ sudo yum install -y cdap-ambari-service
  $ sudo ambari-server restart

Installing the CDAP Service via APT
...................................
::

  $ sudo apt-get install -y cdap-ambari-service
  $ sudo ambari-server restart


CDAP Dependencies
=================
CDAP depends on certain services being present on the cluster. There are **core
dependencies,** which must be running for CDAP system services to operate correctly, and
**optional dependencies,** which may be required for certain functionality or program types.

The host running the CDAP Master service must have the HDFS, YARN, and HBase clients
installed as CDAP uses the command line clients of these for initialization and their
connectivity information for external service dependencies. Also, CDAP currently requires
Internet access on the CDAP service nodes (or until the issues `CDAP-3957
<https://issues.cask.co/browse/CDAP-3957>`__ or `AMBARI-13456
<https://issues.apache.org/jira/browse/AMBARI-13456>`__ are resolved).

Core Dependencies
-----------------
- **HDFS:** The backing file system for distributed storage
- **YARN:** For running system services in containers on cluster NodeManagers
- **HBase:** For system runtime storage and queues
- **MapReduce2:** For batch operations in workflows and data exploration
- **ZooKeeper:** For service discovery and leader election

Optional Dependencies
---------------------
- **Hive:** For data exploration using SQL queries via the CDAP Explore system service
- **Spark:** For running Spark programs within CDAP applications


Installing using the Wizard
===========================

You can now install CDAP using the Ambari Service Wizard.

Start the Ambari Service Wizard
-------------------------------
1. In the Ambari UI (the Ambari Dashboard), start the **Add Service Wizard**.

   .. figure:: ../_images/ambari/ss01-add-service.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Starting the *Add Service* Wizard

 
#. Select CDAP from the list and click *Next*. If there are core dependencies which are not
   currently installed on the cluster, Ambari will prompt you to install them.
 
   .. figure:: ../_images/ambari/ss02-select-cdap.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Selecting *CDAP*
 
Assign CDAP Services to Hosts
-----------------------------

3. Next, assign CDAP services to hosts.

   CDAP consists of 4 daemons:
 
   #. **Master:** Coordinator service which launches CDAP system services into YARN
   #. **Router:** Serves HTTP endpoints for CDAP applications and REST API
   #. **Kafka Server:** For storing CDAP metrics and CDAP system service log data
   #. **UI:** Web interface to CDAP and :ref:`Cask Hydrator <cdap-apps-intro-hydrator>`
      (for CDAP 3.2.x and later installations)
 
   .. figure:: ../_images/ambari/ss03-assign-masters.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Assigning Masters
 
   We recommended you install all CDAP services onto an edge node (or the *NameNode*, for
   smaller clusters) such as in our example above. After assigning the master hosts, click
   *Next*.

#. Select hosts for the CDAP CLI client. This should be installed on every edge node on
   the cluster or, for smaller clusters, on the same node as the CDAP services.

   .. figure:: ../_images/ambari/ss04-choose-clients.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Selecting hosts for *CDAP*
 
#. Click *Next* to customize the CDAP installation.

Customize CDAP
--------------

6. On the **Customize Services** screen, click the *Advanced* tab to bring up the CDAP configuration.
   Under *Advanced cdap-env*, you can configure environment settings such as heap sizes
   and the directories used to store logs and pids for the CDAP services which run on the edge nodes.

   .. figure:: ../_images/ambari/ss05-config-cdap-env.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Customizing Services 1

#. Under *Advanced cdap-site*, you can configure all options for the operation and running
   of CDAP and CDAP applications.

   .. figure:: ../_images/ambari/ss06-config-cdap-site.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Customizing Services 2

#. To use the CDAP Explore service (to use SQL to query CDAP data), you must have Hive
   installed on the cluster, have the Hive client libraries installed on the same host as
   the CDAP services, and have the *Advanced cdap-site* ``explore.enabled`` option set to
   *true* (the default).

   .. figure:: ../_images/ambari/ss07-config-enable-explore.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Enabling *CDAP Explore*

   For a **complete explanation of these options,** refer to the :ref:`CDAP documentation of
   cdap-site.xml <appendix-cdap-site.xml>`. When finished with configuration changes, click
   *Next*.

Deploying and Starting CDAP
===========================

Deploying CDAP
--------------
9. Review the desired service layout and click *Deploy* to begin the actual deployment of CDAP.

   .. figure:: ../_images/ambari/ss08-review-deploy.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Summary of Services

#. Ambari will install CDAP and start the services.

   .. figure:: ../_images/ambari/ss09-install-start-test.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Install, Start, and Test
      
#. After the services are installed and started, you will click *Next* to get to the
   Summary screen.

#. This screen shows a summary of the changes that were made to the cluster. No services
   should need to be restarted following this operation.

   .. figure:: ../_images/ambari/ss10-post-install-summary.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Summary

#. Click *Complete* to complete the CDAP installation.

CDAP Started
------------
14. Now, you should see **CDAP** listed on the main summary screen for your cluster.

   .. figure:: ../_images/ambari/ss11-main-screen.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **Ambari Dashboard:** Selecting *CDAP*

15. Selecting *CDAP* from the left sidebar, or choosing it from the Services drop-down menu, will take
    you to the CDAP service screen.

   .. figure:: ../_images/ambari/ss12-cdap-screen.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **Ambari Dashboard:** *CDAP* Service Screen

Congratulations! CDAP is now running on your cluster, managed by Ambari. You can login to the CDAP UI
at the address of the node running the CDAP-UI service at port 9999.
