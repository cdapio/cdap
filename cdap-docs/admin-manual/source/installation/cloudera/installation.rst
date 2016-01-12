.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cloudera-installation:

====================================================
Cloudera Manager: Installing the CDAP Custom Service
====================================================

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
Following the instructions at Cloudera's website, `install the CSD <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html>`__.

.. _cloudera-installation-download-distribute-parcel:

Download and Distribute Parcel
------------------------------
Download and distribute the CDAP-|version| parcel. Complete instructions on parcels are
available at `Cloudera's website
<http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/
cm_ig_parcels.html>`__, but in summary these are the steps:
   
1. Add the repository (installing the CSD adds the corresponding CDAP repository for you, but you can 
   `customize the list of repositories 
   <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#cmug_topic_7_11_5_unique_1>`__
   searched by Cloudera Manager if you need to);
#. `Download 
   <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#concept_vwq_421_yk_unique_1__section_cnx_b3y_bm_unique_1>`__
   the parcel to the Cloudera Manager server;
#. `Distribute 
   <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#concept_vwq_421_yk_unique_1__section_sty_b3y_bm_unique_1>`__
   the parcel to all the servers in the cluster; and
#. `Activate 
   <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_ig_parcels.html#concept_vwq_421_yk_unique_1__section_ug1_c3y_bm_unique_1>`__
   the parcel.

If the Cask parcel repo is inaccessible to your cluster, please see :ref:`these
suggestions <faqs-cloudera-direct-parcel-access>`.

.. _cloudera-installation-setup-startup:

Setup and Startup using the Cloudera Manager
--------------------------------------------
Complete instructions, step-by-step, for using the Admin Console *Add Service* Wizard to install CDAP
:ref:`are available <step-by-step-cloudera-add-service>`.

Run the Cloudera Manager Admin Console *Add Service* Wizard and select *CDAP*.
When completing the Wizard, these notes may help:

- *Add Service* Wizard, Page 2: **Hive dependency** is for the CDAP
  "Explore" component which is enabled by default.
 
- *Add Service* Wizard, Page 3: **Choosing Role Assignments**. Ensure CDAP roles are assigned to hosts colocated
  with service or gateway roles for HBase, HDFS, Yarn, and optionally Hive.

- *Add Service* Wizard, Page 3: CDAP **Security Auth** service is an optional service
  for CDAP perimeter security; it can be configured and enabled post-wizard.
 
- *Add Service* Wizard, Pages 4 & 5: **Kerberos Auth Enabled** is needed if running against a
  secure Hadoop cluster.

- *Add Service* Wizard, Pages 4 & 5: **Router Server Port:** This should match the "Router Bind
  Port"; it’s used by the CDAP UI to connect to the Router service.

- *Add Service* Wizard, Page 4 & 5: **App Artifact Dir:** This should initially point to the
  bundled system artifacts included in the CDAP parcel directory. If you have modified
  ``${PARCELS_ROOT}``, please update this setting to match. Users will want to customize
  this directory to a location outside of the CDAP Parcel.

- **Additional CDAP configuration properties** can be added using the Cloudera Manager's 
  *Safety Valve* Advanced Configuration Snippets. Documentation of the available CDAP
  properties is in the :ref:`appendix-cdap-site.xml`.

Once you have completed the installation and :ref:`started CDAP <step-by-step-cloudera-add-service-startup>`, you can then 
:ref:`verify the installation <cloudera-verification>`.