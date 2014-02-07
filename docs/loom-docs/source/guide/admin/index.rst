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

Loom works through the use of provision **Templates**, which dictate the configuration of the
clusters which users can spin up. An administrator can specify any number of such templates to put into
their **Catalog** for users.

Several aspects of cluster configuration are supported and customizable in Loom. These aspects are:

* **Providers** - the infrastructure providers from which clusters are materialized.

* **Hardware types** - The different configurations of hardware available on the providers cluster network.

* **Image types** - The basic disk image installed on the nodes of a cluster.

* **Services** - The software services that are available on a cluster.

Templates are defined through specifying combinations of and constraints to these services. Using templates, Loom provides
server administrators the ability to create flexible customizations for cluster provisioning.

.. _provision-templates2:
Administration Tools
====================
Several tools are included to simplify the process for administrators and provide flexibility. Apart from the
:doc:`Admin UI </guide/admin/ui>`, Loom provides :doc:`plugin tools </guide/admin/plugins>` to create
custom plugins for allocating machines on your providers, or to custom implement your services, setting files to
:doc:`configure your server </guide/admin/server-config>` and `macros to refer to individual nodes on a cluster </guide/admin/macros>`.
:doc:`Web services </rest/index>` are also provided through a ReST API to interact directly with Loom.

Please refer to the following pages for more details:

        * :doc:`Administration Interface </guide/admin/ui>`

        * :doc:`Provisioner Plugins </guide/admin/plugins>`

        * :doc:`Server Configuration </guide/admin/server-config>`

        * :doc:`Macros </guide/admin/macros>`

        * :doc:`REST Web Service Interface </rest/index>`

