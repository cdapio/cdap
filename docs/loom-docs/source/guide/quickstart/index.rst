.. _guide_installation_toplevel:

.. index::
   single: Quick Start Guide
==================
Quick Start Guide
==================

This guide will help you get started with the Continuuity Loom. In this section, you will learn to provision a cluster
using one of the preset templates.

Installing Loom
===============

Please follow the steps found in the :doc:`Installation Guide </guide/installation/index>`. Once successfully installed,
start all the relevant Loom components: the Loom server, provisioners, and the UI.

Getting Started
===============

Open the Loom UI with a browser at ``http://<loom-host>:8100/`` and login as an administrator. The default administrator
login credentials are 'admin,' for both username and password.

.. figure:: /guide/quickstart/quickstart-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


This will take you to the administrator home screen. The
page, shown below, shows metrics for clusters that are currently running on the system. Note, the 'All Nodes' count metric
indicates all the nodes provisioned since the beginning. (That is, it is a historical cumulative number, including the
deleted nodes.) This page also shows the 'Catalog', which is a list of 'templates'
for provisioning clusters. Several default templates are available out of the box.

.. figure:: /guide/quickstart/quickstart-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Configuring a Provider
=========================

To start provisioning machines, you must first specify an IaaS provider on which the clusters will be created. Click on the 
'Providers' icon on the sidebar to the left. Several defaults should already be available on this
screen, namely OpenStack, Amazon Web Services, Rackspace, and Joyent. Choose the provider you want to use for this
tutorial, then click on its name to navigate to its edit screen.

Each provider has provider-specific inputs, which pertains to information about your provider and user account.
These inputs may include settings such as username and API key, and they can be obtained through the provider's own 
system. If you do not already have an account with the provider, you may register or obtain one on a provider's 
website.

For the purpose of this tutorial, we will use Rackspace as our provider. An API key and username are required for
using Rackspace (for more information on how to obtain your personalized API key, see
`this page <http://www.rackspace.com/knowledge_center/article/rackspace-cloud-essentials-1-generating-your-api-key>`_ ).

.. figure:: /guide/admin/providers-screenshot-4.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


Enter the necessary configurations and click on 'Save' to persist the settings.

Provisioning your First Cluster
===============================

Click on the 'Clusters' icon on the sidebar to the left. For an administrator, this screen lists all the clusters
that have been provisioned across all Loom user accounts.

.. figure:: /guide/quickstart/quickstart-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Click on 'Create a cluster' on the top menu bar to enter the cluster creation page. In the 'Name' field,
enter 'loom-quickstart-01' as the name of the cluster we will create. The 'Template' field
specifies which template in the catalog we will use for this cluster. For this tutorial, we will
create a distributed Hadoop/HBase cluster with Continuuity Reactor installed. 

Select 'reactor-distributed' from the 'Template' drop down box. Enter the number of nodes you want your cluster 
to have (for example, 5) in the field labeled 'Number of machines.'

Display the advanced settings menu by clicking on the small triangle next to the label 'Advanced'. This lists
the default settings for the 'reactor-distributed' template. If you want to chose a provider other than Rackspace
in the previous section, click on the drop down menu labeled 'Provider' to select your choice of the provider.

.. figure:: /guide/quickstart/quickstart-screenshot-5.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

To start provisioning, click on 'Create' at the bottom of the page. This operation will take you back to the Clusters' home
screen, where you can monitor the progress and status of your cluster. Creating a cluster may take several minutes.

.. figure:: /guide/quickstart/quickstart-screenshot-4.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Accessing the Cluster
=====================

Once creation is complete, the cluster is ready for use. For more information, click on the name 'loom-quickstart-01' on the
Clusters' home screen. On this cluster description screen, nodes are grouped together by the set
of services that are available on them. For details of the nodes, click on the white triangles next to each
service set to expand the list. The expanded list shows a list of attributes for each node. You may access these
nodes, by using their corresponding hostnames, IP addresses, usernames, and passwords, through a service such as SSH.

.. figure:: /guide/quickstart/quickstart-screenshot-6.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center
