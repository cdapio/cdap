:orphan:
.. index::
   single: Overview
.. _index_toplevel:

========
Overview
========
Loom is a cluster provisioning system designed from the ground up to fully facilitate cluster lifecycle management
in both public and private clouds. It streamlines the process of provisioning clusters of any kind, right from an end user's workstation.
Administrators and end users can quickly instantiate live clusters using cluster templates, which can be created easily.

Loom exposes two primary user interfaces: Loom Admin and Loom User. Loom Admin makes it easy for administrators to create and maintain complex cluster templates across multiple IaaS clouds, while Loom User makes it easy for any end user to select and build complex clusters using these templates. This empowers the end users eliminating the need for filing tickets or the pain of configuring complicated clusters. In essence, Loom has been designed to offer a comprehensive self-service provisioning system.

.. figure:: /_images/loom-diagram.png
    :align: center
    :alt: What is Loom?
    :figclass: align-center

IT administrative challenges are more around managing clusters of machines or clusters of clusters and less around indivdual machines. Within Loom, a cluster is the basic indivisible element and supports the following operations: create, delete, amend, update, and monitor. These complex operations are masked by simple REST services that allow easy integrations with existing IT operational systems. Loom is built with DevOps and IT tools in mind - it is highly flexible (through lifecycle hooks)
and easily pluggable (with chef, puppet and other automation tools).

.. _loom-in-production:
Loom in Production
==================
Currently, Loom is running in production at Continuuity to provision Reactor Sandboxes on Continuuity's private cloud. It is also used internally as an IT tool by Continuuity developers to test new and incremental features on an ongoing basis. Within the Continuuity build system, Loom is used via REST APIs to provision multi-node Reactor clusters and perform functional testing.

.. _history-of-loom:
History of Loom
===============
At Continuuity, we built Loom to fill an ongoing need - build and deploy clusters quickly for developers. Since Reactor, our flagship product, utilizes several technologies within the Hadoop ecosystem, it was a constant battle to build, use and tear down clusters regularly on a variety of IaaS providers. Loom solved this problem with the rapid provisioning of our clusters meeting the needs of our business, our developers, and our customers. Loom's streamlined cluster management was a key investment for Continuuity.

