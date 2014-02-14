.. _guide_installation_toplevel:

.. index::
   single: Quick Start Guide
==================
Quick Start Guide
==================

This guide is intended to get you started with Continuuity Loom. In this section, you will learn to provision a cluster
using one of the preset templates.

Installing Loom
===============

Please follow the steps found in the :doc:`Installation Guide </guide/installation/index>`.

Getting Started
===============

Login as an administrator from the Loom UI. The default login credentials are 'admin' as both username and password.
The default home screen shows some basic metrics of clusters currently on the system and the 'Catalog', which is the
list of 'templates' for provisioning clusters. Several default templates should already be available out of the box.

.. figure:: /guide/admin/overview-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center



Configuring a Provider
=========================

To start provisioning machines, you must first set a provider on which the clusters will be created. To do this,
click on the 'Providers' tab on the sidebar to the left. Several defaults should already be available on this
screen, namely OpenStack, Amazon Web Services, Rackspace and Joyent. Choose the provider you want to use for this
tutorial and click on its name to navigate to its edit screen.

Each provider has provider-specific inputs, some of which pertain to information about your account.
These may generally include username and API key, and can be through the provider's system.
If you do not already have an account with the provider, you will need to register an account through their website.

.. figure:: /guide/admin/providers-screenshot-4.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


When you have entered the necessary configurations, click 'Save' to save the settings.

Provisioning your First Cluster
===============================

Click on the 'Clusters' tab on the sidebar to the left. For an administrator, this screen lists all the clusters
that have been created across all users.

.. figure:: /guide/admin/clusters-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

To create a cluster, click on 'Create a cluster' on the top menu bar. This should take you to a page labeled
'Create a cluster'. In the 'Name' field, enter 'loom-quickstart-01' as the name of the cluster. The 'Template' field
specifies which template in the catalog we will use for this cluster.For the purpose of this tutorial, we will
create a distributed Hadoop/HBase cluster with Continuuity Reactor installed. Select 'reactor-distributed'
from the 'Template' drop down box. Enter the number of nodes you want your cluster to have  (for example, 5)
in the field labeled 'Number of machines'.

Display the advanced settings menu by clicking on the small triangle next to the label 'Advanced'. This lists all
the default settings for the template selected. If you configured the provider in the previous section to be anything
other than Rackspace, click on the drop down menu labeled 'Provider' and choose the provider.

.. figure:: /guide/quickstart/quickstart-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

To start provisioning, click on 'Create' at the bottom of the page. You will be brought back to the Clusters home
screen, where you can monitor the progress and status of the cluster you created.