.. _guide_user_toplevel:
.. include:: /toplevel-links.rst
.. index::
   single: User Guide
==========
User Guide
==========

This page describes the different interfaces and functions that end users can use to manage their own set of clusters within Loom.
As mentioned earlier in the administration guide, all clusters and nodes within clusters are dictated by templates created by
the administrator, made accessible to individual users (or developers), and displayed in the Catalog. These accessible templates  
are users' blueprint for their individual cluster instantiation.

The User Home Screen
====================
The user home screen shows a list of all clusters provisioned by a user. It displays basic information for each cluster owned 
by the user, such as current clusters, clusters under construction, and deleted clusters. Active and deleted clusters, however, 
are shown separately in this interface. Clicking on each of these items launches a separate screen with individual cluster 
description and detailed cluster information.

.. figure:: user-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


Provisioning a new Cluster
==========================
Users can provision a machine by selecting 'Create a cluster' on the top menu bar. Through this page, a user
can create a cluster with a given name and template setting (as defined by the system administrator), and specify the
number of nodes to allocate to the cluster.

For more information on how administrators can set templates for provisioning a cluster, see the :doc:`Administration
Guide </guide/admin/index>`).

.. figure:: user-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


Advanced Settings
-----------------

The Loom user interface has a number of advanced configuration options.
To access the advanced options, Click on the gray triangle next to the label 'Advanced'. This exposes the options to
explicitly specify the provider and image type to be used for the current cluster. The 'Config' field allows the user
to specify additional custom configurations in a JSON-formatted input (for more information, see
:doc:`Macros </guide/admin/macros>`).

.. figure:: user-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center

The Cluster Description Screen
==============================
A user can view more details of a cluster by clicking on the cluster name on the Home screen, or by selecting
'Clusters' -> <name of the cluster> on the top-left of the screen. The cluster description page provides an up-to-date
status report of a cluster as well as a description of a cluster, including the template used
to create the cluster, the infrastructure provider, and the list of services installed.

.. figure:: user-screenshot-4.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


To see more details of the service sets, click on the white triangle underneath the title. To view the actions of a
particular node, click on the 'Show actions' button.

.. figure:: user-screenshot-5.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


Deleting a Cluster
^^^^^^^^^^^^^^^^^^
The 'Delete' button on the cluster description page deletes the data on the cluster and decommissions the associated
nodes. Clusters that are successfully deleted are removed from the active clusters list on the user's home screen.


