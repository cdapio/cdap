.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _ambari-installing-cdap:

===============
Installing CDAP
===============

1. In the Ambari UI (the Ambari Dashboard), start the **Add Service Wizard**.

   .. figure:: ../../_images/ambari/ss01-add-service.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Starting the *Add Service* Wizard

 
#. Select CDAP from the list and click *Next*. If there are core dependencies which are not
   installed on the cluster, Ambari will prompt you to install them.
 
   .. figure:: ../../_images/ambari/ss02-select-cdap.png
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
   - **UI:** web interface to CDAP and :ref:`Cask Hydrator <cdap-apps-intro-hydrator>`
     (for CDAP 3.2.x installations)
 
   .. figure:: ../../_images/ambari/ss03-assign-masters.png
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

   .. figure:: ../../_images/ambari/ss04-choose-clients.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Selecting hosts for *CDAP*
 
#. Click *Next* to continue with customizing CDAP.

#. On the **Customize Services** screen, click *Advanced* to bring up the CDAP configuration.
   Under *Advanced cdap-env*, you can configure heap sizes, and log and pid directories for the
   CDAP services which run on the edge nodes.

   .. figure:: ../../_images/ambari/ss05-config-cdap-env.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Customizing Services 1

#. Under *Advanced cdap-site*, you can configure all options for the operation and running
   of CDAP and CDAP applications.

   .. figure:: ../../_images/ambari/ss06-config-cdap-site.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Customizing Services 2

#. If you wish to use the CDAP Explore service (to use SQL to query CDAP data), you must: have
   Hive installed on the cluster; have the Hive client on the same host as CDAP; and set the
   ``explore.enabled`` option to true.

   .. figure:: ../../_images/ambari/ss07-config-enable-explore.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Enabling *CDAP Explore*

   For a **complete explanation of these options,** refer to the :ref:`CDAP documentation of
   cdap-site.xml <appendix-cdap-site.xml>`. After making any configuration changes, click
   *Next*.

#. Review the desired service layout and click *Deploy* to begin installing CDAP.

   .. figure:: ../../_images/ambari/ss08-review-deploy.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Summary of Services

#. Ambari will install CDAP and start the services.

   .. figure:: ../../_images/ambari/ss09-install-start-test.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Install, Start, and Test
      
#. After the services are installed and started, you will click *Next* to get to the
   Summary screen.

#. This screen shows a summary of the changes that were made to the cluster. No services
   should need to be restarted following this operation.

   .. figure:: ../../_images/ambari/ss10-post-install-summary.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Summary

#. Click *Complete* to complete the CDAP installation.

#. Now, you should see **CDAP** listed on the main summary screen for your cluster.

   .. figure:: ../../_images/ambari/ss11-main-screen.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** Selecting *CDAP*

#. Selecting *CDAP* from the left, or choosing it from the Services drop-down menu, will take
   you to the CDAP service screen.

   .. figure:: ../../_images/ambari/ss12-cdap-screen.png
      :figwidth: 100%
      :width: 800px
      :align: center
      :class: bordered-image
 
      **Ambari Dashboard:** *CDAP* Service Screen
 
Congratulations! CDAP is now running on your cluster, managed by Ambari.