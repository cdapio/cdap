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

Follow the instructions found in the :doc:`Installation Guide </guide/installation/index>` to install Loom.

Getting Started
===============

Open the Loom UI with a browser at ``http://<loom-host>:8100/`` and login as an administrator. The default administrator login credentials are 'admin' as both
username and password. This will take you to the administrator home screen. This page shows metrics for
clusters that are currently running on the system. This page also shows the 'Catalog', which is a list of 'templates'
for provisioning clusters. Several default templates are available out of the box.

.. figure:: /guide/admin/overview-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center



Configuring a Provider
=========================

To start provisioning machines, you must first specify an IaaS provider on which the clusters will be created. To do so,
click on the 'Providers' tab on the sidebar to the left. Several defaults should already be available on this
screen, namely OpenStack, Amazon Web Services, Rackspace and Joyent. Choose the provider you want to use for this
tutorial, then click on its name to navigate to its edit screen.

Each provider has provider-specific inputs, which pertains to information about your provider and user account on the provider.
These may include settings such as username and API key, and can be obtained through the provider's own system.
If you do not already have an account with the provider, you may need to register one through their website.

For the purpose of this tutorial, we will use Rackspace as our provider. An API key and username is required for
using Rackspace (for more information on how to obtain your personalized API key, see
`this page <http://www.rackspace.com/knowledge_center/article/rackspace-cloud-essentials-1-generating-your-api-key>`_ ).

.. figure:: /guide/admin/providers-screenshot-4.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


When you have entered the necessary configurations, click 'Save' to save the settings.

Provisioning your First Cluster
===============================

Click on the 'Clusters' tab on the sidebar to the left. For an administrator, this screen lists all the clusters
that have been provisioned across all Loom user accounts.

.. figure:: /guide/admin/clusters-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

Click on 'Create a cluster' on the top menu bar to enter the cluster creation page. In the 'Name' field,
enter 'loom-quickstart-01' as the name of the cluster we will create. The 'Template' field
specifies which template in the catalog we will use for this cluster. For the purpose of this tutorial, we will
create a distributed Hadoop/HBase cluster with Continuuity Reactor installed. Select 'reactor-distributed'
from the 'Template' drop down box. Enter the number of nodes you want your cluster to have (for example, 5)
in the field labeled 'Number of machines'.

Display the advanced settings menu by clicking on the small triangle next to the label 'Advanced'. This lists
the default settings for the 'reactor-distributed' template. If you chose a provider other than Rackspace
in the previous section, click on the drop down menu labeled 'Provider' to select your provider.

.. figure:: /guide/quickstart/quickstart-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

To start provisioning, click on 'Create' at the bottom of the page. You will be brought back to the Clusters home
screen, where you can monitor the progress and status of the cluster you created. This process may take several minutes.

Accessing the Cluster
=====================

Once creation is complete, the cluster is ready for use. For more information, click on the name 'loom-quickstart-01' on the
Clusters home screen. On this cluster description screen, nodes are grouped together by the set
of services that are installed on them. For details of the nodes, click on the white triangles next to each
service set to expand the list. The expanded list shows a list of attributes for each node. These nodes can now be
accessed using the corresponding IP addresses, usernames and passwords (through a service such as SSH).

.. figure:: /guide/quickstart/quickstart-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center