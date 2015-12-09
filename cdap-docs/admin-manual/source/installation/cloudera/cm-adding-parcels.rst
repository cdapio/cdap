.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cloudera-installation:

================================
Cloudera Manager: Adding Parcels
================================

.. _cloudera-installation-download:

Download
========
Download the CDAP CSD (Custom Service Descriptor) by `downloading the JAR file <http://cask.co/resources/#cdap-integrations>`__.
The source code is available `for review or download <https://github.com/caskdata/cm_csd>`__.

Details on CSDs and Cloudera Manager Extensions are `available online 
<https://github.com/cloudera/cm_ext/wiki>`__.

.. _cloudera-installation-csd:

Install the CSD
===============
Following the instructions at Cloudera's website, `install the CSD <http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cm_mc_addon_services.html>`__.

.. _cloudera-installation-download-distribute-parcel:

Download and Distribute
=======================
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
