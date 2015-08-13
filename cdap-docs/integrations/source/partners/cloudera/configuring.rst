.. _cloudera-configuring:

======================================================
Configuring and Installing CDAP using Cloudera Manager
======================================================


Overview
=======================================

.. highlight:: console

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

and an optional role |---| Security Auth Service |---| plus a Gateway client configuration. 

CDAP depends on HBase, YARN, HDFS, Zookeeper, and |---| optionally |---| Hive. It must also be placed on a cluster host with full
client configurations for these dependent services. Therefore, CDAP roles must be colocated on a cluster host with at least
an HDFS Gateway, Yarn Gateway, HBase Gateway, and |---| optionally |---| a Hive Gateway. Note that Gateways are redundant if colocating
CDAP on cluster hosts with actual services, such as the HBase Master, Yarn Resourcemanager, or HDFS Namenode.

All services run as the 'cdap' user installed by the parcel.


Prerequisites
=======================================

#. Node.js (from |node-js-version|; we recommend |recommended-node-js-version|) must be installed on the node(s) where the UI
   role instance will run. You can download the appropriate version of Node.js from `nodejs.org
   <http://nodejs.org/dist/>`__.

#. Zookeeper's ``maxClientCnxns`` must be raised from its default.  We suggest setting it to zero
   (unlimited connections). As each YARN container launched by CDAP makes a connection to Zookeeper, 
   the number of connections required is a function of usage.

#. For Kerberos-enabled Hadoop clusters:

   - The 'cdap' user needs to be granted HBase permissions to create tables.
     In an HBase shell, enter::
     
      > grant 'cdap', 'ACRW'

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

Download the CDAP CSD (Custom Service Descriptor): `download the JAR file <http://cask.co/resources/#cdap-integrations>`__.
The source code is available `here <https://github.com/caskdata/cm_csd>`__.

Details on CSDs and Cloudera Manager Extensions are `available online 
<https://github.com/cloudera/cm_ext/wiki>`__.


Install, Setup, and Startup
=======================================

.. _cloudera-configuring-csd-parcel:

.. rubric:: Install the CSD, Download and Distribute Parcel

#. `Install the CSD <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html>`__.
#. Download and distribute the CDAP-|version| parcel. Complete instructions on parcels are available at
   `Cloudera's website <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html>`__,
   but in summary there are four steps:
   
   1. Add the repository (installing the CSD adds the corresponding CDAP repository for you, but you can 
      `customize the list of repositories <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#cmug_topic_7_11_5_unique_1>`__
      searched by Cloudera Manager if you need to);
   #. `Download <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#concept_vwq_421_yk_unique_1__section_cnx_b3y_bm_unique_1>`__
      the parcel to the Cloudera Manager server;
   #. `Distribute <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#concept_vwq_421_yk_unique_1__section_sty_b3y_bm_unique_1>`__
      the parcel to all the servers in the cluster; and
   #. `Activate <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#concept_vwq_421_yk_unique_1__section_ug1_c3y_bm_unique_1>`__
      the parcel.

   If the Cask parcel repo is inaccessible to your cluster, please see :ref:`these suggestions <cloudera-direct-parcel-access>`.

.. rubric:: Setup using the Cloudera Manager Admin Console *Add Service* Wizard

Run the Cloudera Manager Admin Console *Add Service* Wizard and select *CDAP*.
When completing the Wizard, these notes may help:

   - *Add Service* Wizard, Page 2: **Optional Hive dependency** is for the optional CDAP
     "Explore" component which can be enabled later.
     
   - *Add Service* Wizard, Page 3: **Choosing Role Assignments**. Ensure CDAP roles are assigned to hosts colocated
     with service or gateway roles for HBase, HDFS, Yarn, and optionally Hive.

   - *Add Service* Wizard, Page 3: CDAP **Security Auth** service is an optional service
     for CDAP perimeter security; it can be configured and enabled post-wizard.
     
   - *Add Service* Wizard, Page 5: **Kerberos Auth Enabled** is needed if running against a
     secure Hadoop cluster.

   - *Add Service* Wizard, Page 5: **Router Server Port:** This should match the "Router Bind
     Port"; it’s used by the CDAP UI to connect to the Router service.

   - *Add Service* Wizard, Page 5: **App Template Dir:** This should initially point to the bundled templates included in
     the CDAP parcel directory. If you have modified ``${PARCELS_ROOT}``, please update this setting to match.  Advanced
     users will want to customize this directory to a location outside of the CDAP Parcel.

Complete instructions, step-by-step, for using the Admin Console *Add Service* Wizard to install CDAP
:ref:`are available <step-by-step-cloudera-add-service>`.

.. _cloudera-verification:

.. include:: ../../../../admin-manual/source/installation/installation.rst
   :start-after: .. _install-verification:
   :end-before:  .. _install-upgrade:

Upgrading an Existing Version
=======================================

.. rubric:: Upgrading Patch Release versions

When a new compatible CDAP parcel is released, it will be available via the Parcels page in the Cloudera Manager UI.

#. Stop all flows, services, and other programs in all your applications.

#. Stop CDAP services.

#. Use the Cloudera Manager UI to download, distribute, and activate the parcel on all cluster hosts.

#. Start CDAP services.

.. rubric:: Upgrading Major/Minor Release versions

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

.. rubric:: Upgrading CDAP 3.0 to 3.1 and Upgrading CDH 5.3 to 5.4

**Background:** CDH 5.3 ships with HBase 0.98 while CDH 5.4 ships with HBase 1.0. We support
CDH 5.4 as of CDAP 3.1.0. Upgrading from CDH 5.3 to CDH 5.4 includes an HBase upgrade in
addition to a CDAP upgrade. 

**It is important to perform these steps as described, otherwise the coprocessors may not
get upgraded correctly and HBase regionservers may crash.** In the case where something
goes wrong, see these troubleshooting instructions for :ref:`problems while upgrading CDH
<cloudera-troubleshooting-upgrade-cdh>`.

In the future, we intend to automate all these steps. The issue that tracks that work is
`CDAP-3179 <https://issues.cask.co/browse/CDAP-3179>`__.

**Upgrade Steps**

1. If using Cloudera Manager, :ref:`stop all CDAP application and services
   <install-upgrade>`, as Cloudera Manager will have auto-started CDAP
#. Disable all CDAP tables; from an HBase shell, run this command::

    > disable_all 'cdap.*'
    
#. Upgrade to CDH 5.4
#. :ref:`Stop CDAP application and services <install-upgrade>`, as CDH will have auto-started CDAP
#. Upgrade to CDAP 3.1
#. Run the CDAP Upgrade Tool, as the user that runs CDAP Master (the CDAP user)::

    $ /opt/cdap/master/bin/svc-master run co.cask.cdap.data.tools.UpgradeTool upgrade
    
   If using Cloudera Manager, this can be done by selecting ``Run CDAP Upgrade Tool`` from
   the *CDAP Service Actions* menu
    
#. Check if the coprocessor JARs for these tables have been upgraded to CDH HBase 1.0:

    - ``cdap_system:app.meta``
    - ``cdap_system:datasets.instance``
    - ``cdap_system:datasets.type``
    
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

#. Enable these tables; from an HBase shell, run these commands::
   
    > enable 'cdap_system:app.meta'
    > enable 'cdap_system:datasets.instance'
    > enable 'cdap_system:datasets.type'

#. Run the CDAP Upgrade Tool (again), as the user that runs CDAP Master (the CDAP user)
#. Before starting CDAP, check that all tables have coprocessors upgraded, as described above
#. Enable all CDAP tables; from an HBase shell, run this command::

    > enable_all 'cdap.*'
    
#. Start CDAP

**Note:** Any apps will need to be both recompiled and re-deployed if they use either a
PartitionedFileSet or a TimePartitionedFileSet.


.. rubric:: Upgrading CDAP 2.8 to 3.0

**Note:** Apps need to be both recompiled and re-deployed.

When upgrading from 2.8.0 to 3.0.0, the CDAP Web-App role has been replaced by the CDAP-UI
role.  After starting the 3.0 services for the first time:

   - From the CDAP Instances page, select "Add Role Instances", and choose a host for the CDAP-UI role.

   - From the CDAP Instances page, check the CDAP-Web-App role, and select "Delete" from the Actions menu.


Troubleshooting
=======================================

.. rubric:: Permissions Errors

Some versions of Hive may try to create a temporary staging directory at the table
location when executing queries. If you are seeing permissions errors when running a
query, try setting ``hive.exec.stagingdir`` in your Hive configuration to
``/tmp/hive-staging``. 

This can be done in Cloudera Manager using the *Hive Client
Advanced Configuration Snippet (Safety Valve) for hive-site.xml* configuration field.

.. rubric:: Missing Application Templates

The bundled application templates are included in the CDAP parcel, located in a subdirectory
of Cloudera's ``${PARCELS_ROOT}`` directory, for example::

  /opt/cloudera/parcels/CDAP/master/templates

Ensure that the ``App Template Dir`` configuration option points to this path on disk. Since this
directory can change when CDAP parcels are upgraded, advanced users are encouraged to place
these templates in a static directory outside the parcel root, and configure accordingly.

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
  

.. _cloudera-troubleshooting-upgrade-cdh:

.. rubric:: Problems While Upgrading CDH

If you miss a step in the upgrade process and something goes wrong, it's possible that the
tables will get re-enabled before the coprocessors are upgraded. This could cause the
regionservers to abort and may make it very difficult to get the cluster back to a stable
state where the tables can be disabled again and complete the upgrade process.

.. highlight:: xml

In that case, set this configuration property in ``hbase-site.xml``::

  <property>
    <name>hbase.coprocessor.abortonerror</name>
    <value>false</value>
  </property>

and restart the HBase regionservers. This will allow the regionservers to start up
despite the coprocessor version mismatch. At this point, you should be able to run through
the upgrade steps successfully. 

At the end, remove the entry for ``hbase.coprocessor.abortonerror`` in order to ensure
that data correctness is maintained.
