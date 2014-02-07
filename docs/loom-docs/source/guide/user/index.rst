.. _guide_user_toplevel:
.. include:: /toplevel-links.rst

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


The Cluster Description Screen
==============================
A user can view more details of a cluster by clicking on the cluster name on the Home screen, or by selecting
'Clusters' -> <name of the cluster> on the top left of the screen. The cluster description page provides an up-to-date
monitoring of a cluster's status as well as a description of a cluster, including its status, the template used
to create the cluster, the infrastructure provider, and the list of services installed.

.. figure:: user-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


Deleting a Cluster
^^^^^^^^^^^^^^^^^^
The 'Delete' button on the cluster description page deletes the data on the cluster and decommissions the associated
nodes. Clusters that are successfully deleted are removed from the active clusters list on the user's home screen.
