.. _guide_user_toplevel:
.. include:: /toplevel-links.rst

==========
User Guide
==========

This page describes the different interfaces and functions for end users.

.. contents::
        :local:
        :class: faq
        :backlinks: none

The User Home Screen
====================
The user's home screen shows a list of all the clusters provisioned to the user. This screen displays basic information of each cluster owned by the user. The clusters are divided into active clusters and deleted clusters. Clicking on each of these items takes you to their individual cluster description screen, which provides more detailed information on the cluster.

.. figure:: user-screenshot-1.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


Provisioning a new Cluster
==========================

Users can provision a machine by selecting 'Create a cluster' on the top left of the screen. On this screen, the user can create a cluster with a chosen name, a template setting, as defined by the system administrator and specify the number of nodes to allocate to the cluster.

For more information on how administrators can set templates for provisioning a cluster, see the :doc:`Administration Guide </guide/admin/index>`).

.. figure:: user-screenshot-2.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center



The Cluster Description Screen
==============================

The individual description pages provides an up-to-date monitoring of a cluster's status. The page includes a description of a cluster, including its status, the template used to create the cluster, the infrastructure provider and the list of services installed.

.. figure:: user-screenshot-3.png
    :align: center
    :width: 800px
    :alt: alternate text
    :figclass: align-center


Deleting a Cluster
^^^^^^^^^^^^^^^^^^
The 'Delete' button on the cluster description page deletes the data on the cluster and decommissions the associated nodes.