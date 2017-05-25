.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

:section-numbering: true

.. |su_hdfs| replace:: su hdfs

.. _admin-installation-ambari:

================================
Installation using Apache Ambari
================================

.. include:: ../_includes/installation/installation-steps-images.txt

.. rubric:: Notes

- `Apache Ambari <https://ambari.apache.org/>`__ can only be used to add CDAP to an **existing**
  Hadoop cluster, one that already has the required services (Hadoop: HDFS, YARN, HBase,
  ZooKeeper, and |---| optionally |---| Hive and Spark) installed.
- Ambari is for setting up HDP (Hortonworks Data Platform) on bare clusters; it can't be
  used for clusters with HDP already installed, where the original installation was
  **not** with Ambari.
- A number of features are currently planned to be added, including:

  - select `CDAP metrics <https://issues.cask.co/browse/CDAP-4108>`__ and
  - a full `smoke test of CDAP functionality <https://issues.cask.co/browse/CDAP-4105>`__ after installation.

- If you are installing CDAP with the intention of using *replication,* see these
  instructions on :ref:`CDAP Replication <installation-replication>` *before* installing or starting CDAP.


Preparing the Cluster
=====================

.. Hadoop Configuration
.. --------------------
.. include:: ../_includes/installation/hadoop-configuration.txt

You can make these changes during the configuration of your cluster `using Ambari
<http://docs.hortonworks.com/HDPDocuments/Ambari-2.4.2.0/bk_ambari-installation/content/customize_services.html>`__.

.. HDFS Permissions
.. ----------------
.. include:: /../target/_includes/ambari-hdfs-permissions.rst


Downloading and Distributing Packages
=====================================

Downloading CDAP Ambari Service
-------------------------------
To install CDAP on a cluster managed by Ambari, we have available packages for
RHEL-compatible and Ubuntu systems, which you can install onto your Ambari management server.
These packages add CDAP to the list of available services which Ambari can install.

.. highlight:: console

To install the ``cdap-ambari-service`` package, first add the appropriate CDAP repository
to your system’s package manager by following the steps below. These steps will install a
Cask repository on your Ambari server.

The **repository version** (shown in the commands below as |literal-cdap-slash-short-version|)
must match the **CDAP series** which you’d like installed on your cluster. To install the
**latest** version of the *CDAP 4.1 series,* you would install the *CDAP 4.1 repository.*

Replace |---| in the commands that follow on this page |---| all references to
|literal-cdap-slash-short-version| with the CDAP Repository corresponding to the version
that you would like to use (such as ``cdap/4.0`` for CDAP 4.0.x):

.. _ambari-compatibility-matrix:

+---------------------------------------------------------+
| Supported Hortonworks Data Platform (HDP) Distributions |
+-------------------------------+-------------------------+
| CDAP Series or Release        | Hadoop Distributions    |
+===============================+=========================+
| CDAP 4.1.1, 4.2.x             | HDP 2.0 through HDP 2.6 |
+-------------------------------+-------------------------+
| CDAP 4.1.0                    | HDP 2.0 through HDP 2.5 |
+-------------------------------+-------------------------+
| CDAP 4.0.x                    | HDP 2.0 through HDP 2.5 |
+-------------------------------+-------------------------+
| CDAP 3.6.x                    | HDP 2.0 through HDP 2.4 |
+-------------------------------+-------------------------+
| CDAP 3.5.x                    | HDP 2.0 through HDP 2.4 |
+-------------------------------+-------------------------+
| CDAP 3.4.x                    | HDP 2.0 through HDP 2.4 |
+-------------------------------+-------------------------+
| CDAP 3.3.x                    | HDP 2.0 through HDP 2.3 |
+-------------------------------+-------------------------+
| CDAP 3.2.x                    | HDP 2.0 through HDP 2.3 |
+-------------------------------+-------------------------+
| CDAP 3.1.x                    | HDP 2.0 through HDP 2.2 |
+-------------------------------+-------------------------+
| CDAP 3.0.x                    | HDP 2.0 and HDP 2.1     |
+-------------------------------+-------------------------+

.. _ambari-compatibility-matrix-end:

**Notes:**

- The CDAP Ambari service has been tested on Ambari Server 2.3 through 2.5, as
  supplied from Hortonworks.
- To install a version lower than the highest current version (such as *CDAP 4.1.0* when
  *4.1.1* is available), you will need to downgrade your repo after installing it.

.. include:: /../target/_includes/ambari-installation.rst
  :start-after: .. _ambari-install-rpm-using-yum:
  :end-before: .. end_install-debian-using-apt


Installing CDAP Ambari Service
------------------------------
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


Installing CDAP Services
========================

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

   CDAP consists of five daemons:

   #. **Master:** Coordinator service which launches CDAP system services into YARN
   #. **Router:** Serves HTTP endpoints for CDAP applications and REST API
   #. **Auth Server:** For managing authentication tokens on CDAP clusters with perimeter security enabled
   #. **Kafka Server:** For transporting CDAP metrics and CDAP system service log data
   #. **UI:** Web interface to CDAP and :ref:`CDAP Studio <cdap-studio>`

   .. figure:: ../_images/ambari/ss03-assign-masters.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image-top-margin

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

6. On the **Customize Services** screen, you can configure both CDAP features and the
   environment settings for CDAP and the CDAP services which run on the edge nodes. At the bottom of
   the *Settings* tab are settings for common CDAP features and Java services.

   .. figure:: ../_images/ambari/ss05-config-cdap.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **Ambari Dashboard:** Customizing Services, CDAP Features and Java Services

#. On the **Customize Services** screen, click the *Advanced* tab to bring up the complete CDAP configuration.
   Under *Advanced cdap-env*, you can configure environment settings such as heap sizes
   and the directories used to store logs and PIDs for the CDAP services which run on the edge nodes.

   .. figure:: ../_images/ambari/ss06-config-cdap-env.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **Ambari Dashboard:** Customizing Services 2

   Under *Advanced cdap-site*, you can configure all options for the operation and running
   of CDAP and CDAP applications.

   **Additional CDAP configuration properties**, not shown in the web interface, can be
   added using Ambari's advanced custom properties at the end of the page. Documentation
   of the available CDAP properties is in the :ref:`appendix-cdap-site.xml`.

   For a **complete explanation of these options,** refer to the :ref:`CDAP documentation
   of cdap-site.xml <appendix-cdap-site.xml>`.

   **Additional environment variables** can be set, as required, using Ambari's
   *Configs > Advanced > Advanced cdap-env*.

   When finished with configuration changes, click *Next*.


Starting CDAP Services
======================

Deploying CDAP
--------------
8. Review the desired service layout and click *Deploy* to begin the actual deployment of CDAP.

   .. figure:: ../_images/ambari/ss07-review-deploy.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **Ambari Dashboard:** Summary of Services

#. Ambari will install CDAP and start the services.

   .. figure:: ../_images/ambari/ss08-install-start-test.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **Ambari Dashboard:** Install, Start, and Test

#. After the services are installed and started, you will click *Next* to get to the
   Summary screen.

#. This screen shows a summary of the changes that were made to the cluster. No services
   should need to be restarted following this operation.

   .. figure:: ../_images/ambari/ss09-post-install-summary.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **Ambari Dashboard:** Summary

#. Click *Complete* to complete the CDAP installation.

CDAP Started
------------
13. You should now see **CDAP** listed on the main summary screen for your cluster.

   .. figure:: ../_images/ambari/ss10-main-screen.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **Ambari Dashboard:** Selecting *CDAP*

.. _ambari-verification:

Verification
============

Service Checks in Apache Ambari
-------------------------------

14. Selecting *CDAP* from the left sidebar, or choosing it from the Services drop-down menu, will take
    you to the CDAP service screen.

   .. figure:: ../_images/ambari/ss11-cdap-screen.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image

      **Ambari Dashboard:** *CDAP* Service Screen

CDAP is now running on your cluster, managed by Ambari. You can login to the CDAP UI at
the address of the node running the CDAP UI service at port 11011. The drop-down *Quick
Links* menu has a menu item directly to the CDAP UI.

.. include:: /_includes/installation/smoke-test-cdap.txt

.. _ambari-installation-advanced-topics:

Advanced Topics
===============

- :ref:`Enabling Security <ambari-configuration-security>`
- :ref:`Enabling Kerberos <ambari-configuration-enabling-kerberos>`
- :ref:`Enabling CDAP High Availability <ambari-configuration-highly-available>`
- :ref:`Enabling Hive Execution Engines <ambari-configuration-enabling-hive-execution-engines>`

.. _ambari-configuration-security:

.. Enabling Perimeter Security
.. ---------------------------
.. include:: /../target/_includes/ambari-configuration.rst
    :start-after: .. _ambari-configuration-eps:

:ref:`CDAP Security <admin-security>` is configured by setting the appropriate
settings under Ambari for your environment.

.. _ambari-configuration-enabling-kerberos:

Enabling Kerberos
-----------------
Kerberos support in CDAP is automatically enabled when enabling Kerberos security on your
cluster via Ambari. Consult the appropriate Ambari documentation for instructions on enabling
Kerberos support for your cluster.

The ``cdap`` user must be able to launch YARN containers, which can be accomplished by
adjusting the YARN ``min.user.id`` (to 500) to include the ``cdap`` user. (As Ambari does
not have a mechanism for setting the YARN ``allowed.system.users`` |---| the preferred
method of enabling the ``cdap`` user as it is more precise and limited |---| the setting
of ``min.user.id`` needs to be used instead.)

A. If you are **adding CDAP** to an existing Kerberos cluster, in order to configure **CDAP for
   Kerberos authentication**:

   #. The ``<cdap-principal>`` is shown in the commands that follow as ``cdap``;
      however, you are free to use a different appropriate name.

      .. highlight:: console

   #. When running on a secure HBase cluster, as the ``hbase`` user, issue the command::

        $ echo "grant 'cdap', 'RWCA'" | hbase shell

   #. In order to configure **CDAP Explore Service for secure Hadoop:**

      .. highlight:: xml

      i. To allow CDAP to act as a Hive client, it must be given ``proxyuser`` permissions and allowed
         from all hosts. For example: set the following properties in the configuration file ``core-site.xml``,
         where ``cdap`` is a system group to which the ``cdap`` user is a member::

           <property>
             <name>hadoop.proxyuser.hive.groups</name>
             <value>cdap,hadoop,hive</value>
           </property>
           <property>
             <name>hadoop.proxyuser.hive.hosts</name>
             <value>*</value>
           </property>

      #. To execute Hive queries on a secure cluster, the cluster must be running the MapReduce ``JobHistoryServer``
         service. Consult your distribution documentation on the proper configuration of this service.

      #. To execute Hive queries on a secure cluster using the CDAP Explore Service, the Hive MetaStore service
         must be configured for Kerberos authentication. Consult your distribution documentation on the proper
         configuration of the Hive MetaStore service.

      With all these properties set, the CDAP Explore Service will run on secure Hadoop clusters.

B. If you are **adding Kerberos** to an existing cluster, in order to configure **CDAP for
   Kerberos authentication**:

   .. highlight:: console

   #. The ``/cdap`` directory needs to be owned by the ``<cdap-principal>``; you can set
      that by running the following command as the ``hdfs`` user (change the ownership in the
      command from ``cdap`` to whatever is the ``<cdap-principal>``)::

        $ su hdfs && hadoop fs -mkdir -p /cdap && hadoop fs -chown cdap /cdap

   #. When converting an existing CDAP cluster to being Kerberos-enabled, you may
      run into YARN usercache directory permission problems. A non-Kerberos cluster with
      default settings will run CDAP containers as the user ``yarn``. A Kerberos cluster will
      run them as the user ``cdap``. When converting, the usercache directory that YARN
      creates will already exist and be owned by a different user. On all datanodes, run this
      command, substituting in the correct value of the YARN parameter ``yarn.nodemanager.local-dirs``::

        $ rm -rf <YARN.NODEMANAGER.LOCAL-DIRS>/usercache/cdap

      (As ``yarn.nodemanager.local-dirs`` can be a comma-separated list of directories, you may
      need to run this command multiple times, once for each entry.)

      If, for example, the setting for ``yarn.nodemanager.local-dirs`` is ``/yarn/nm``, you would use::

        $ rm -rf /yarn/nm/usercache/cdap

      Restart CDAP after removing the usercache(s).


.. _ambari-configuration-highly-available:

Enabling CDAP HA
----------------
In addition to having a :ref:`cluster architecture <admin-manual-install-deployment-architectures-ha>`
that supports HA (high availability), these additional configuration steps need to be followed and completed:

CDAP Components
...............
For each of the CDAP components listed below (Master, Router, Kafka, UI, Authentication Server), these
comments apply:

- Sync the configuration files (such as ``cdap-site.xml`` and ``cdap-security.xml``) on all the nodes.
- While the default *bind.address* settings (``0.0.0.0``, used for ``app.bind.address``,
  ``data.tx.bind.address``, ``router.bind.address``, and so on) can be synced across hosts,
  if you customize them to a particular IP address, they will |---| as a result |---| be
  different on different hosts. This can be controlled by the settings for an individual *Role Instance*.

CDAP Master
...........
The CDAP Master service primarily performs coordination tasks and can be scaled for redundancy. The
instances coordinate amongst themselves, electing one as a leader at all times.

- Using the Ambari UI, add additional hosts for the ``CDAP Master
  Service`` to additional machines.

CDAP Router
...........
The CDAP Router service is a stateless API endpoint for CDAP, and simply routes requests to the
appropriate service. It can be scaled horizontally for performance. A load balancer, if
desired, can be placed in front of the nodes running the service.

- Using the Ambari UI, add additional hosts for the ``CDAP Router Service`` to additional machines.
- Start each ``CDAP Router Service`` role.

CDAP Kafka
..........
- Using the Ambari UI, add additional hosts for the ``CDAP Kafka Service``
  to additional machines.
- Two properties govern the Kafka setting in the cluster:

  - The **list of Kafka seed brokers** is generated automatically, but the
    replication factor (``kafka.default.replication.factor``) is not set
    automatically. Instead, it needs to be set manually.
  - The **replication factor** is used to replicate Kafka messages across
    multiple machines to prevent data loss in the event of a hardware
    failure.

- The recommended setting is to run **at least two** Kafka brokers with a **minimum replication
  factor of two**; set this property to the maximum number of tolerated machine failures
  plus one (assuming you have that number of machines). For example, if you were running
  five Kafka brokers, and would tolerate two of those failing, you would set the
  replication factor to three. The number of Kafka brokers listed should always be equal to
  or greater than the replication factor.

CDAP UI
.......
- Using the Ambari UI, add additional hosts for the ``CDAP UI Service``
  to additional machines.

CDAP Authentication Server
..........................
- Using the Ambari UI, add additional hosts for the ``CDAP Security Auth
  Service`` (the CDAP Authentication Server) to additional machines.
- Note that when an unauthenticated request is made in a secure HA setup, a list of all
  running authentication endpoints will be returned in the body of the request.

.. Enabling Hive Execution Engines
.. -------------------------------
.. _ambari-configuration-enabling-hive-execution-engines:

.. include:: /_includes/installation/hive-execution-engines.txt
