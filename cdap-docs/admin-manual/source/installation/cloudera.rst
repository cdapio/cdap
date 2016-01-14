.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

:section-numbering: true

.. _admin-cloudera:

======================
Cloudera Manager (CDH)
======================

This section describes installing CDAP on Hadoop systems that are `CDH (Cloudera Data Hub)
<http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/
cdh_intro.html>`__ clusters managed with `Cloudera Manager (CM)
<http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/
cloudera-manager.html>`__.

You install CDAP into a CDH cluster by first downloading and installing a 
CSD (`Custom Service Descriptor <http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cm_mc_addon_services.html#concept_qbv_3jk_bn_unique_1>`__) 
for CDAP. Once the CDAP CSD is installed and you have restarted your Cloudera Manager Server, you
will able to use CM to install, start, and manage CDAP on CDH clusters.

These instructions assume that you are familiar with CM and CDH, and already have a
cluster managed by CM with CDH installed and running. The cluster must meet CDAP's
:ref:`hardware, network, and software requirements <admin-manual-system-requirements>`
before you install CDAP.

Follow these steps:

.. figure:: ../_images/steps/cloudera-manager.png
   :height: 80px
   :align: center

Once you have completed the installation and started CDAP services,
you can then :ref:`verify the installation <admin-manual-verification>`.

Preparing Roles and Nodes
=========================

Roles and Dependencies
----------------------
The CDAP CSD (Custom Service Descriptor) consists of four mandatory roles:

#. Master
#. Gateway/Router
#. Kafka-Server
#. UI

A fifth role (Security Auth Service) is optional, plus there is a CDAP Gateway client configuration. 

- As CDAP depends on HDFS, YARN, HBase, ZooKeeper, and (optionally) Hive and Spark, it must be placed
  on a cluster host with full client configurations for these dependent services. 

- CDAP roles must be co-located on a cluster host with at least an HDFS Gateway, a YARN
  Gateway, an HBase Gateway, and |---| optionally |---| Hive or Spark Gateways.
  
- Note that these Gateways are redundant if you are co-locating CDAP on cluster hosts with
  actual services, such as the HDFS Namenode, the YARN resource manager, or the HBase Master.

- All services run as the ``'cdap'`` user installed by the parcel.

Prerequisites
-------------
#. Node.js must be installed on the node(s) where the UI role instance will run. 
   We recommend any version of `Node.js <https://nodejs.org/>`__ |node-js-version|; in
   particular, we recommend |recommended_node_js_version|. You can download an appropriate
   version of Node.js from `nodejs.org <http://nodejs.org/dist/>`__. Detailed
   instructions on installing Node.js :ref:`are available <admin-manual-install-node.js>`.

#. ZooKeeper's ``maxClientCnxns`` must be raised from its default.  We suggest setting it to zero
   (unlimited connections). As each YARN container launched by CDAP makes a connection to ZooKeeper, 
   the number of connections required is a function of usage. You can make this change using Cloudera Manager to
   `modify the ZooKeeper configuration properties <http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cm_mc_mod_configs.html>`__.

#. Ensure YARN is configured properly to run MapReduce programs.  Often, this includes
   ensuring that the HDFS ``/user/yarn`` directory exists with proper permissions::
   
     # su hdfs
     $ hdfs dfs -mkdir -p /user/yarn && hadoop fs -chown yarn /user/yarn && hadoop fs -chgrp yarn /user/yarn

#. Ensure that YARN has sufficient memory capacity by lowering the default minimum container 
   size (controlled by the property ``yarn.scheduler.minimum-allocation-mb``). Lack of
   YARN memory capacity is the leading cause of apparent failures that we see reported.
   We recommend starting with these settings:
   
   - ``yarn.nodemanager.delete.debug-delay-sec``: 43200
   - ``yarn.nodemanager.resource.memory-mb``: Adjust if you need to raise memory per nodemanager
   - ``yarn.nodemanager.resource.cpu-vcores``: Adjust if you need to raise vcores per nodemanager
   - ``yarn.scheduler.minimum-allocation-mb``: 512 mb
   
   You can make these changes `using Cloudera Manager <http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/cm_mc_mod_configs.html>`__.
   You will be prompted to restart the stale services after making changes.
    
#. For Kerberos-enabled Hadoop clusters:

   - The ``'cdap'`` user needs to be granted HBase permissions to create tables.
     In an HBase shell (``$ hbase shell``), enter::
     
      > grant 'cdap', 'RWCA'

   - The ``'cdap'`` user must be able to launch YARN containers, either by adding it to the YARN
     ``allowed.system.users`` or by adjusting the YARN ``min.user.id`` to include the ``cdap`` user.
     (Search for the YARN configuration ``allowed.system.users`` in Cloudera Manager, and then add
     the ``cdap`` user to the whitelist.)


Adding the Parcels
==================

.. _cloudera-installation-download:

Download
--------
Download the CDAP CSD (Custom Service Descriptor) by `downloading the JAR file <http://cask.co/resources/#cdap-integrations>`__.
The source code is available `for review or download <https://github.com/caskdata/cm_csd>`__.

Details on CSDs and Cloudera Manager Extensions are `available online 
<https://github.com/cloudera/cm_ext/wiki>`__.

.. _cloudera-installation-csd:

Install the CSD
---------------
Install the CSD following the instructions at Cloudera's website on `Add-on Services
<http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html>`__, 
using the instructions given for the case of installing software in the form of a parcel.

In this case, you install the CSD first and then install the parcel.

.. _cloudera-installation-download-distribute-parcel:

Download and Distribute
-----------------------
Download and distribute the CDAP-|version| parcel. Complete instructions on parcels are
available at `Cloudera's website
<http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/
cm_ig_parcels.html>`__, but in summary these are the steps:
   
1. Add the repository (installing the CSD adds the corresponding Cask parcel repository for you, but you can 
   `customize the list of repositories 
   <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#cmug_topic_7_11_5_unique_1>`__
   searched by Cloudera Manager if you need to);
#. `Download 
   <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#concept_vwq_421_yk_unique_1__section_cnx_b3y_bm_unique_1>`__
   the parcel to your Cloudera Manager server;
#. `Distribute 
   <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#concept_vwq_421_yk_unique_1__section_sty_b3y_bm_unique_1>`__
   the parcel to all the servers in your cluster; and
#. `Activate 
   <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#concept_vwq_421_yk_unique_1__section_ug1_c3y_bm_unique_1>`__
   the parcel.

If the Cask parcel repository is inaccessible to your cluster, please see :ref:`these
suggestions <faqs-cloudera-direct-parcel-access>`.


Installing CDAP Services
========================

These instructions show how to use the Cloudera Manager Admin Console *Add Service* Wizard
to install and start CDAP. Note that the screens of the wizard will vary depending on
which version of Cloudera Manager and CDAP you are using.

.. _cloudera-add-a-service:

Add A Service
-------------
Start from the Cloudera Manager Admin Console's *Home* page, selecting *Add a Service* from the menu for your cluster:

.. figure:: ../_images/cloudera/cloudera-csd-01.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Cloudera Manager:** Starting the *Add Service* Wizard.

.. _cloudera-add-service-wizard:

Add Service Wizard: Selecting CDAP
----------------------------------

Use the *Add Service* Wizard and select *Cask DAP*.

.. figure:: ../_images/cloudera/cloudera-csd-02.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 1:** Selecting CDAP (Cask DAP) as the service to be added.


Add Service Wizard: Specifying Dependencies
-------------------------------------------

The **Hive dependency** is for the CDAP "Explore" component, which is enabled by default.

.. figure:: ../_images/cloudera/cloudera-csd-03.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 2:** Setting the dependencies (in this case, including Hive).
   

Add Service Wizard: Customize Role Assignments
----------------------------------------------

**Customize Role Assignments:** Ensure CDAP roles are assigned to hosts colocated
with service or gateway roles for HBase, HDFS, YARN, and (optionally) Hive and Spark.

.. figure:: ../_images/cloudera/cloudera-csd-04.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 3:** When customizing Role Assignments, the *CDAP Security
   Auth Service* can be added later.
   
   
Add Service Wizard: Reviewing Configuration
-------------------------------------------

**Kerberos Auth Enabled:** This is needed if running on a secure Hadoop cluster.

**Router Server Port:** This should match the "Router Bind Port"; it’s used by the CDAP UI
to connect to the Router service.

**App Artifact Dir:** This should initially point to the bundled system artifacts included
in the CDAP parcel directory. If you have modified ``${PARCELS_ROOT}``, please update this
setting to match. Users will want to customize this directory to a location outside of the
CDAP Parcel.

.. figure:: ../_images/cloudera/cloudera-csd-06.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Pages 4 & 5:** Reviewing configurations.


**Additional CDAP configuration properties** can be added using the Cloudera Manager's 
*Safety Valve* Advanced Configuration Snippets. Documentation of the available CDAP
properties is in the :ref:`appendix-cdap-site.xml`.

At this point, the CDAP installation is configured and is ready to be installed. Review
your settings before continuing to the next step, which will install and start CDAP.

.. _cloudera-starting-services:

Starting CDAP Services
======================

Add Service Wizard: First Run of Commands
-----------------------------------------
Executing commands to install and automatically start CDAP services.

.. figure:: ../_images/cloudera/cloudera-csd-07.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 6:** Finishing first run of commands to install and start CDAP.
   

Add Service Wizard: Completion Page
-----------------------------------

.. figure:: ../_images/cloudera/cloudera-csd-08.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Add Service Wizard, Page 7:** Congratulations screen, though there is still work to be done.


Verifying CDAP
--------------
After the Cloudera Manager Admin Console's *Add Service* Wizard completes, *Cask DAP* will
show in the list for the cluster where you installed it. You can select it, and go to the
*Cask DAP* page, with *Quick Links* and *Status Summary*. The lights of the *Status
Summary* should all turn green, showing completion of startup. 

The *Quick Links* includes a link to the **CDAP UI**, which by default is running on
port ``9999`` of the host where the UI role instance is running.

.. figure:: ../_images/cloudera/cloudera-csd-09.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Cloudera Manager:** CDAP (Cask DAP) now added to the cluster.
   

.. figure:: ../_images/cloudera/cloudera-csd-10.png
   :figwidth: 100%
   :height: 526px
   :width: 800px
   :align: center
   :class: bordered-image

   **Cloudera Manager:** CDAP completed startup: all lights green!
   
.. _cloudera-cdap-ui:

CDAP UI
-------
The CDAP UI may initially show errors while all of the CDAP YARN containers are
starting up. Allow for up to a few minutes for this. The *Services* link in the CDAP
UI in the upper right will show the status of the CDAP services. 

.. figure:: ../../../admin-manual/source/_images/console/console_01_overview.png
   :figwidth: 100%
   :height: 714px
   :width: 800px
   :align: center
   :class: bordered-image

   **CDAP UI:** Showing started-up with applications deployed.

Further instructions for verifying your installation are contained in :ref:`admin-manual-verification`.

