.. _guide_admin_toplevel:
.. include:: /toplevel-links.rst

=======================
Administration Overview
=======================

.. include:: /guide/admin/admin-links.rst

Overview
========

This page describes the concepts used in Loom administration, and the different tools that Loom provides for
administrators to configure and manage their system.

Concepts
========

Loom works through the use of provision **Templates**. These templates dictate the configurations for the types of
clusters which users can spin up. An administrator can specify any number of such templates to put in the **Catalog**
for their users.

Several elements of cluster configuration are supported and customizable in Loom. These settings are:

* **Providers** - the actual server infrastructure providers to materialize a cluster on.
* **Hardware types** - The different configurations of hardware available on the providers cluster network.
* **Image types** - The basic disk image installed on the nodes of a cluster.
* **Services** - The software services that are available on a cluster.

Templates are defined through specifying combinations and constraints of these services. Using templates, Loom provides
server administrators the ability to create flexible options for machine provisioning.


Administration Tools
====================
Several tools are provided to simplify and provide flexibility for administrators. Apart from the
:doc:`Admin User Interface </guide/admin/ui>`, Loom provides :doc:`plugin tools </guide/admin/plugins>` to create
custom plugins for allocating machines on your providers, or to custom implement your services, and
:doc:`server configuration macros </guide/admin/macros>` to configure the server.

A user interface is provided. Macros, plugins. Additionally ReST API. Please refer to the following pages for more details:
 * :doc:`Administration Interface </guide/admin/ui>`
 * :doc:`Provisioner Plugins </guide/admin/plugins>`
 * :doc:`Macro and Server Configuration </guide/admin/macros>`
