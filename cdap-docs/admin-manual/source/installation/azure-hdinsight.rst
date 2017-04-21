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

- :ref:`Create an HDInsight HBase cluster <admin-installation-azure-hdinsight-creating-cluster>` with CDAP installed as an application
- Install CDAP as an application :ref:`on an existing HDInsight HBase cluster <admin-installation-azure-hdinsight-installing-on-existing>`
- :ref:`Verify that CDAP is installed and running <admin-installation-azure-hdinsight-verification>` by using the CDAP UI

Information on Microsoft Azure HDInsight is `available online
<https://docs.microsoft.com/en-us/azure/hdinsight/>`__.

CDAP |short-version| is compatible with Microsoft Azure HDInsight |hdinsight-versions|.


.. _admin-installation-azure-hdinsight-creating-cluster:

Creating a Cluster with CDAP Installed
======================================

1. Log onto Microsoft Azure and navigate to *HDInsight clusters* (either through the *New*
   menu and under *Intelligence + analytics*, or directly through the *HDInsight clusters*
   icon in the left sidebar):

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-1.png
      :figwidth: 100%
      :width: 611px
      :class: bordered-image

      **Microsoft Azure:** Finding HDInsight

#. Create an *HDInsight cluster* by clicking the "Create" button. *Quick create* will create a
   cluster, but without CDAP installed. We recommend instead using the *Custom (size,
   settings, apps)* tab to access additional options and add installing CDAP as part of
   the cluster creation.

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-2.0.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Creating a cluster using *Quick create*

   Using the *Custom (size, settings, apps)* tab, enter a *Cluster name* (such as
   *cdap-cluster*). Under *Cluster type*, select "HBase". (To continue, click *Select* to
   close the *Cluster configuration* window):

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-2.1.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Creating a cluster using *Custom (size, settings, apps)* settings

#. Enter the required basic settings: *Cluster login password*, *Resource group*, and *Location*:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-3.0.png
      :figwidth: 100%
      :width: 674px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Configuring basic settings

#. Under *Storage*, set a storage account, either an existing account or enter a unique
   name for a new account.

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-3.1.png
      :figwidth: 100%
      :width: 676px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Setting a storage account

#. Under *Applications*, search for *CDAP* and select the version you'd like to install
   and accept the legal terms:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-3.2.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Setting CDAP as an application on the cluster

#. Under *Cluster size*, at a minimum, a CDAP cluster requires four *D3 v2* region nodes. This is
   enough YARN capacity to run only simple examples. For any serious work, you will need a
   cluster with additional nodes (Microsoft recommends additional smaller nodes versus
   fewer larger nodes due to the nature of blob storage):

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-3.3.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Setting cluster size

#. Under *Advanced settings*, there are currently no settings required:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-3.4.png
      :figwidth: 100%
      :width: 710px
      :class: bordered-image

      **Microsoft Azure HDInsight:** *Advanced settings* are currently not required

#. Click *Next* to review the summary. Then click *Create* to start cluster initialization:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-4.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Summary and confirming configuration

#. Once the cluster is ready |---| it may take a few minutes while the YARN containers
   come up |---| its icon will show on the portal dashboard:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-5.0.png
      :figwidth: 100%
      :width: 176px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Cluster is created and running

#. Open the created cluster. Click the *Applications* button to see the installed applications:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-5.1.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** The cluster dashboard, *Applications* button circled

#. CDAP should show as installed successfully, with a *Portal* link to access it:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-5.2.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** Installed applications, showing CDAP and its *Portal* link


.. _admin-installation-azure-hdinsight-installing-on-existing:

Install CDAP as an Application on an Existing Cluster
=====================================================
You can add CDAP to an existing cluster. (If you added CDAP in the previous steps for
creating a cluster, you can jump to :ref:`verification
<admin-installation-azure-hdinsight-verification>`.)

1. Open the created HBase Cluster. Click the *Applications* button:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-6.png
      :figwidth: 100%
      :width: 800px
      :class: bordered-image

      **Microsoft Azure HDInsight:** The cluster dashboard

#. In the open window, click *+ Add* to show the *Available applications*. Choose the CDAP
   version you'd like to install and accept the legal terms. Once confirmed in the
   following step, installation of CDAP starts:

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

#. Once CDAP is installed successfully, its status shows as *Installed*, with a *Portal*
   link to access it:

   .. figure:: ../_images/azure-hdinsight/azure-hdinsight-9.png
      :figwidth: 100%
      :width: 631px
      :class: bordered-image

      **Microsoft Azure HDInsight:** CDAP installed as an application and its *Portal* link


.. _admin-installation-azure-hdinsight-verification:

Verification
============
#. Click the *Portal* link to launch the login window. Login by entering the cluster
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
