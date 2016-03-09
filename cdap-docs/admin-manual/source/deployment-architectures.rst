.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _admin-manual-install-deployment-architectures:

========================
Deployment Architectures
========================

This section illustrates both a minimal (single host) deployment and a high availability
(multi-host) deployment that is highly scalable.

Our **recommended installation** is to use a multi-host deployment of two boxes for the
CDAP components; the :ref:`hardware requirements <admin-manual-hardware-requirements>` are
relatively modest, as most of the work is done by the Hadoop cluster. These two boxes
provide high availability; at any one time, one of them is the leader providing services
while the other is a follower providing failover support.

CDAP Minimal Deployment
=======================

**Note:** Minimal deployment runs all services on a single host.

.. image:: _images/cdap-minimal-deployment-v2.png
   :width: 8in
   :align: center

.. _admin-manual-install-deployment-architectures-ha:

CDAP High Availability and Highly Scalable Deployment
=====================================================

**Note:** Each component in CDAP is horizontally scalable. The number of nodes for each
component can be changed based on your particular requirements.

.. image:: _images/cdap-ha-hs-deployment-v2.png
   :width: 8in
   :align: center
