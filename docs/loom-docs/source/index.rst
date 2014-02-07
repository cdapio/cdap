:orphan:
.. _index_toplevel:
.. include:: toplevel-links.rst

This documentation will provide information about Loom and its functionality for administrators in IT operations and users who use Loom to provision clusters based on templates created by administrators. This document also covers operational management of Loom. The document presumes you are familiar with Hadoop technologies, specifically Zookeeper. Further it will presume knowledge in REST & Ruby.

.. _overview:
Overview
========
Loom is a cluster provisioning system designed from the ground up to fully facilitate cluster lifecycle management
in both public and private clouds. Loom streamlines the process of provisioning clusters of any kind right from a developer workstation. The amount of work for system administrators and developers within an IT organization is reduced substantially through cluster templates, which can be instantiated as live clusters quickly and easily.

Loom exposes two primary user interfaces: Loom Admin and Loom User. Loom Admin makes it easy for administrators to create and maintain complex cluster templates across multiple IaaS clouds. Loom User makes it eays for any user to select and build complex clusters using these templates on the fly. No more filing tickets or fighting to configure complicated clusters–Loom has been designed to offer completely self-service provisioning.

.. figure:: /_images/loom-diagram.png
    :width: 489px
    :align: center
    :height: 286px
    :alt: alternate text
    :figclass: align-center

Current and future challenges will be more focused around not manging individual machines, but around managing clusters of machines or clusters of clusters. Within Loom, a cluster is the basic indivisible element that supports create, delete, amend, update and monitor operations. The complexity of these operations is masked by simple REST services that allow easy integrations with existing IT operations systems. Loom is built with DevOps and IT tools in mind: it's highly flexible (through lifecycle hooks) and pluggable (integrates with chef, puppet and other automation tools).

.. _loom-in-production:
Loom in Production
==================
Loom is currently running in production to provision Reactor Sandboxes on the Continuuity Cloud and also as an internal IT tool used by Continuuity developers for testing new and incremental features on an ongoing basis. Within the Continuuity build system, Loom is used via REST APIs to provision multi-node Reactor clusters to perform functional testing.

.. _history-of-loom:
History of Loom
===============
At Continuuity, we began work on Loom to simply scratch our own itch. Reactor, our flagship product, utilizes several technologies within the Hadoop ecosystem, which meant it was a constant battle to build, use, and tear down clusters on a variety of IaaS providers on an ongoing basis. The needs of our business, developers, and customers meant that streamlining cluster management would be a key investment for us as a company.