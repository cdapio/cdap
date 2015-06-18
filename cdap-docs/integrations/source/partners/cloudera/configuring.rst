.. _cloudera-configuring:

======================================================
Configuring and Installing CDAP using Cloudera Manager
======================================================


Overview
=======================================

You can use `Cloudera Manager
<http://www.cloudera.com/content/cloudera/en/products-and-services/cloudera-enterprise/cloudera-manager.html>`__ 
to integrate CDAP into a Hadoop cluster by downloading and installing a CDAP CSD (Custom
Service Descriptor). Once the CSD is installed, you will able to use Cloudera Manager to
install, start and manage CDAP on Hadoop clusters.

These instructions cover the steps to integrate CDAP using Cloudera Manager:

- **Prerequisites:** Preparing your Hadoop cluster for CDAP.
- **Download:** Downloading the CSD file.
- **Install, Setup, and Startup:** Installing the CSD, running the *Add Service* Wizard, and starting CDAP.
- **Verification:** Confirming that CDAP was installed and configured successfully.
- **Troubleshooting:** Particular situations that can occur with Cloudera.


Roles and Dependencies
----------------------
The CDAP CSD consists of four mandatory roles:

- Master
- Gateway/Router
- Kafka-Server
- UI

and an optional role—Security Auth Service—plus a Gateway client configuration. 

CDAP depends on HBase, YARN, HDFS, Zookeeper, and—optionally—Hive. All services run as
the 'cdap' user installed by the parcel.


Prerequisites
=======================================

#. Node.js (from |node-js-version|; we recommend |recommended-node-js-version|) must be installed on the node(s) where the UI
   role instance will run. You can download the appropriate version of Node.js from `nodejs.org
   <http://nodejs.org/dist/>`__.

#. Zookeeper's ``maxClientCnxns`` must be raised from its default.  We suggest setting it to zero
   (unlimited connections). As each YARN container launched by CDAP makes a connection to Zookeeper, 
   the number of connections required is a function of usage.

#. For Kerberos-enabled Hadoop clusters:

   - The 'cdap' user needs to be granted Hbase permissions to create tables.
     In an Hbase shell, enter::
     
      grant 'cdap', 'ACRW'

   - The 'cdap' user must be able to launch YARN containers, either by adding it to the YARN
     ``allowed.system.users`` or by adjusting ``min.user.id``.

#. Ensure YARN is configured properly to run MapReduce programs.  Often, this includes
   ensuring that the HDFS ``/user/yarn`` directory exists with proper permissions.

#. Ensure that YARN has sufficient memory capacity by lowering the default minimum container 
   size. Lack of YARN memory capacity is the leading cause of apparent failures that we
   see reported.

.. _cloudera-configuring-download:

Download
=======================================

Download the CDAP CSD (Custom Service Descriptor): `download JAR file <http://cask.co/resources/#cdap-integrations>`__.

Details on CSDs and Cloudera Manager Extensions are `available online 
<https://github.com/cloudera/cm_ext/wiki>`__.


Install, Setup, and Startup
=======================================

.. rubric:: Install the CSD

#. `Install the CSD <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html>`__.
#. Download and distribute the CDAP-|version| parcel. If the Cask parcel repo is
   inaccessible to your cluster, please see :ref:`these suggestions <cloudera-direct-parcel-access>`.

.. rubric:: Setup using the Cloudera Manager Admin Console *Add Service* Wizard

Run the Cloudera Manager Admin Console *Add Service* Wizard and select *CDAP*.
When completing the Wizard, these notes may help:

   - *Add Service* Wizard, Page 2: **Optional Hive dependency** is for the optional CDAP
     "Explore" component which can be enabled later.
     
   - *Add Service* Wizard, Page 3: CDAP **Security Auth** Service is an optional service
     for CDAP perimeter security; it can be configured and enabled post-wizard.
     
   - *Add Service* Wizard, Page 5: **Kerberos Auth Enabled** is needed if running against a
     secure Hadoop cluster.

   - *Add Service* Wizard, Page 5: **Router Server Port:** This should match the "Router Bind
     Port"; it’s used by the CDAP UI to connect to the Router service.

Complete instructions, step-by-step, for using the Admin Console *Add Service* Wizard to install CDAP
:ref:`are available <step-by-step-cloudera-add-service>`.

.. _cloudera-verification:

Verification
=======================================

After CDAP has started up, the best way to verify the installation is to deploy an application,
ingest some data, run a MapReduce program on it, and then query the results out.  The following
procedure describes how to do this primarily, via the CDAP UI, though this can all be done via
command line.

We provide in our SDK pre-built ``.JAR`` files for convenience.

#. Download and install the latest `CDAP Software Development Kit (SDK)
   <http://cask.co/downloads/#cdap>`__. The version should match the version you have installed.
#. Extract to a folder (``CDAP_HOME``).
#. Open a command prompt and navigate to ``CDAP_HOME/examples``.
#. Each example folder has a ``.jar`` file in its ``target`` directory.
   For verification, we'll use the ``Purchase`` example and its jar, located in 
   ``CDAP_HOME/examples/Purchase-``\ |literal-release|\ ``.jar``. The ``Purchase`` example is documented 
   in the CDAP :ref:`examples manual <examples-purchase>`.

#. Open a web browser to the CDAP UI. It is located on port ``9999`` of the box where
   you installed CDAP.

#. From the CDAP UI Development tab, under "Apps" click "Add App” and navigate to the jar.

#. Once it is deployed, click on it in the list of Applications (*PurchaseHistory*), then click on
   *PurchaseFlow* in the list of Programs to get to the *Flow* detail page, then click the *Start*
   button.  (this will launch additional YARN containers.)

#. Once the Flow is *RUNNING*, inject data by clicking on the *purchaseStream* icon in
   the Flow diagram.  In the dialog that pops up, type ``Tom bought 5 apples for $3`` and click
   *Inject*.  You should see activity in the graphs and the Flowlet counters increment.

#. Run a MapReduce program against this data by navigating back to the *PurchaseHistory* list of 
   programs, select *PurchaseHistoryBuilder*, and click the *Start* button.  This will launch an
   additional container and a MapReduce job in YARN.  After it starts you should see the Map and
   Reduce progress bars complete.  Failures at this stage are often due to YARN MapReduce misconfiguration
   or a lack of YARN capacity.

#. After the MapReduce job is complete, we can startup a query service which will read
   from the processed dataset.  Navigate to Application -> PurchaseHistory ->
   PurchaseHistoryService.  Click the Start button to start the Service.  (This will launch another YARN container)

#. From the *PurchaseHistoryService* page, click *Make Request* for the */history/{customer}* endpoint listed.
   In the dialog that pops up, enter ``Tom`` in the *Path Params* field and click *Make Request*.

#. You should get back a response similar to::

     {"customer":"Tom","purchases":[{"customer":"Tom","product":"apple","quantity":5,"price":3,
      "purchaseTime":1421470224780}]}

#. You have now completed verification of the installation.

Upgrading an Existing Version
=======================================

.. rubric:: Upgrading Patch Release versions

When a new compatible CDAP parcel is released, it will be available via the Parcels page in the Cloudera Manager UI.

#. Stop all Flows, Services, and other Programs in all your applications.

#. Stop CDAP services.

#. Use the Cloudera Manager UI to download, distribute, and activate the parcel on all cluster hosts.

#. Start CDAP services.

.. rubric:: Upgrading Major/Minor Release versions

These steps will upgrade from CDAP 2.8.0 to CDAP 3.0.0. (**Note:** Apps need to be both recompiled and re-deployed.)

#. Stop all Flows, Services, and other Programs in all your applications.

#. Stop CDAP services.

#. Ensure your installed version of the CSD matches the target version of CDAP. For example, CSD version 3.0.* is compatible
   with CDAP version 3.0.*.  Download the latest version of the CSD `here <http://cask.co/resources/#cdap-integrations>`__.

#. Use the Cloudera Manager UI to download, distribute, and activate the parcel on all cluster hosts.

#. Before starting services, run the Upgrade Tool to update any necessary CDAP table definitions.  From the CDAP Service page,
   select "Run CDAP Upgrade Tool" from the Actions menu.

#. Start the CDAP services.  At this point it may be necessary to correct for any changes in the CSD.  For example, if new CDAP services
   were added or removed, you must add or remove role instances as necessary.  When upgrading from 2.8.0 to 3.0.0, the CDAP Web-App role has
   been replaced by the CDAP-UI role:

   - From the CDAP Instances page, select Add Role Instances, and choose a host for the CDAP-UI role.

   - From the CDAP Instances page, check the CDAP-Web-App role, and select Delete from the Actions menu.

#. After CDAP services have started, run the Post-Upgrade tool to perform any necessary upgrade steps against the running services.  From the
   CDAP Service page, select "Run CDAP Post-Upgrade Tasks."

#. You must recompile and then redeploy your applications.

Troubleshooting
=======================================

.. rubric:: Permissions Errors

Some versions of Hive may try to create a temporary staging directory at the table
location when executing queries. If you are seeing permissions errors when running a
query, try setting ``hive.exec.stagingdir`` in your Hive configuration to
``/tmp/hive-staging``. 

This can be done in Cloudera Manager using the *Hive Client
Advanced Configuration Snippet (Safety Valve) for hive-site.xml* configuration field.

.. _cloudera-direct-parcel-access:

.. rubric:: Direct Parcel Access

If you need to download and install the parcels directly (perhaps for a cluster that does
not have direct network access), the parcels are available by their full URLs. As they are
stored in a directory that does not offer browsing, they are listed here:

.. parsed-literal::
  |http:|//repository.cask.co/parcels/cdap/latest/CDAP-|version|-1-el6.parcel
  |http:|//repository.cask.co/parcels/cdap/latest/CDAP-|version|-1-precise.parcel
  |http:|//repository.cask.co/parcels/cdap/latest/CDAP-|version|-1-trusty.parcel
  |http:|//repository.cask.co/parcels/cdap/latest/CDAP-|version|-1-wheezy.parcel
  
If you are hosting your own internal parcel repository, you may also want the
``manifest.json``:

.. parsed-literal::
  |http:|//repository.cask.co/parcels/cdap/latest/manifest.json

The ``manifest.json`` can always be referred to for the list of latest available parcels.

Previously released parcels can also be accessed from their version-specific URLs.  For example:

.. parsed-literal::
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-el6.parcel
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-precise.parcel
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-trusty.parcel
  |http:|//repository.cask.co/parcels/cdap/2.8/CDAP-2.8.0-1-wheezy.parcel
