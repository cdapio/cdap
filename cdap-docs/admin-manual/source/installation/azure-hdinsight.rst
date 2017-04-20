.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.

.. :section-numbering: true

.. _admin-installation-azure-hdinsight:

=========================================
Installation on Microsoft Azure HDInsight
=========================================

Introduction
============

This section describes installing CDAP on `Microsoft Azure HDInsight
<https://azure.microsoft.com/en-us/services/hdinsight/>`__ using
Microsoft's website to:

- Create an HBase cluster;
- Install CDAP as an application on the cluster; and then
- Verify that CDAP is installed and running by using the CDAP UI.

Information on Microsoft Azure HDInsight is `available online
<https://docs.microsoft.com/en-us/azure/hdinsight/>`__.

CDAP |short-version| is compatible with Microsoft Azure HDInsight.


Creating the Cluster
====================

1. Log onto Microsoft Azure and navigate to *HDInsight clusters* (under *Intelligence + analytics*):

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-1.png
      :figwidth: 100%
      :width: 611px
      :class: bordered-image

      **Microsoft Azure:** Finding HDInsight

#. Create an *HDInsight cluster* by clicking "Create". Enter a *Cluster name* (such as
   *cdap-cluster*). Under *Cluster type*, select "HBase". (To continue, click *Select* to
   close the *Cluster configuration* window):

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-2.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Creating a cluster

#. Enter the required basic settings: *Cluster login password*, *Resource group*, and *Location*:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-3.png
      :figwidth: 100%
      :width: 674px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Configuring basic settings

#. Under *Storage*, set a storage account, either an existing account or enter a unique name for a new account.

#. Click *Next* to review the summary.  Then click *Create* to start cluster initialization:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-4.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Summary and confirming configuration

#. Once the cluster is ready, its icon will show on the portal dashboard:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-5.png
      :figwidth: 100%
      :width: 176px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Cluster is created and running


Install CDAP as an Application
==============================
#. Open the created HBase Cluster. Click the *Applications* button:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-6.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** The cluster dashboard

#. In the open window, click *+ Add* to show the *Available applications*.  Choose the CDAP
   version you'd like to install and accept the legal terms. Once confirmed in the following step,
   installation of CDAP starts:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-7.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Adding CDAP as an application

#. The initial status starts at *Accepted*:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-8.png
      :figwidth: 100%
      :width: 631px
      :class: bordered-image

      **Microsoft Azure HDInsight:** CDAP accepted as an application

#. Once CDAP is installed successfully,  its status shows as *Installed*:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-9.png
      :figwidth: 100%
      :width: 631px
      :class: bordered-image

      **Microsoft Azure HDInsight:** CDAP installed as an application


Verification
============
#. Click the *Portal* button to launch the login window. Login by entering the cluster
   login name (default *admin*) and password which are entered when you created the
   cluster:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-10.png
      :figwidth: 100%
      :width: 415px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Login window

#. A new window will open with the CDAP "Welcome" page:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-11.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** CDAP UI "Welcome" page

#. Navigate to the Administration page from CDAP pulldown menu in the upper-right. (You
   can also directly enter the URL for the page,
   ``https://<cdap-host>:443/cdap/administration``, substituting for ``<cdap-host>`` the host
   name of the CDAP server, as shown above in step 1 of *Verification*):

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-12.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** CDAP Administration page, showing CDAP up and running
