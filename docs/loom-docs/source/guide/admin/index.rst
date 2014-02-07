.. _guide_admin_toplevel:
.. include:: /toplevel-links.rst

==============================
Administration Guide: Overview
==============================

.. include:: /guide/admin/admin-links.rst

Overview
========

This guide describes the different interfaces and functions of the administrator UI. Each screen in the administration
interface provides ways to create and edit settings for cluster provisioning.

Concepts
========

Loom works through the use of provision **Templates**. These templates dictate the configurations for the types of
clusters which users can spin up. An administrator can specify any number of such templates to put in the **Catalog**
for their users.

Several elements of cluster configuration are supported and customizable in Loom. These settings are:

**Providers** - the actual server infrastructure providers to materialize a cluster on.

**Hardware types** - The different configurations of hardware available on the providers cluster network.

**Image types** - The basic disk image installed on the nodes of a cluster.

**Services** - The software services that are available on a cluster.

Templates are defined through specifying combinations and constraints of these services. Using templates, Loom provides
server administrators the ability to create flexible options for machine provisioning.


Administration Tools
====================
A user interface is provided. Macros, plugins. Additionally ReST API.