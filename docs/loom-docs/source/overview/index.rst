:orphan:
.. index::
   single: Overview
.. _index_toplevel:

========
Overview
========
Loom is a cluster provisioning system designed from the ground up to fully facilitate cluster lifecycle management
in both the public and private clouds. It streamlines the process of provisioning clusters of any kind, right from a developer's workstation.
Through cluster templates, which can be created easily, administrators and users can quickly instantiate live clusters.

Loom exposes two primary user interfaces: Loom Admin and Loom User. Loom Admin makes it easy for administrators to create and maintain complex cluster templates across multiple IaaS clouds, while Loom User makes it easy for any user to select and build complex clusters using these templates. Effectively, this empowers the users, eliminating the need for filing tickets or fighting to configure complicated clusters. In essence, Loom has been designed to offer a comprehensive self-service provisioning system.

.. figure:: /_images/loom-diagram.png
    :align: center
    :alt: alternate text
    :figclass: align-center

Current and future challenges will focus more on managing clusters of machines or clusters of clusters and less around indivdual machines. Within Loom, a cluster is the basic indivisible element that supports create, delete, amend, update, and monitor operations. These complex operations are masked by simple REST services that allow easy integrations with existing IT operations' systems. Loom is built with DevOps and IT tools in mind: it's highly flexible (through lifecycle hooks)
and easily pluggable (with chef, puppet and other automation tools).

.. _loom-in-production:
Loom in Production
==================
Currently, Loom is running in production to provision Reactor Sandboxes on the Continuuity Cloud and internally as an IT tool by Continuuity developers to test new and incremental features on an ongoing basis. Within the Continuuity build system, Loom is used via REST APIs to provision multi-node Reactor clusters to perform functional testing.

.. _history-of-loom:
History of Loom
===============
At Continuuity, we began work on Loom to simply scratch our own itch. That is, we wanted to build and deploy clusters quickly for developers. Reactor, our flagship product, utilizes several technologies within the Hadoop ecosystem, which meant it was a constant battle to build, use, and tear down clusters reguarly on a variety of IaaS providers. This rapid provision of clusters met the needs of our business, our developers, and our customers. Such a streamlined cluster management was a key investment for Continuuity.

