.. _guide_admin_toplevel:

.. index::
   single: Administration Overview

========
Overview
========

.. include:: /guide/admin/admin-links.rst

In this page, we explore various concepts behind Loom administration and explain various tools used in Loom for
administrators to configure and manage their clusters. At the core of a Loom cluster is the notion of a **Template**, which 
is the blueprint or the primordial stuff of what Loom clusters are comprised of—it is the essence or the DNA of how different 
parts and components come to materialize in a cluster.

Concepts
========

As mentioned above, Loom works through the use of **Templates**, which dictate the configuration of the
clusters that users can spin up. An administrator can specify any number of such templates to put into
their **Catalog** for users. 

Several concepts central to cluster configuration are supported and customizable in Loom. These aspects are:

* **Providers** - These are infrastructure providers (such as Amazon or OpenStack) from which clusters are materialized.

* **Hardware types** - These are different configurations of hardware available or supported on the providers' cluster network.

* **Image types** - These are the basic disk images installed on the nodes of a cluster.

* **Services** - These are comprised of bundled software services that are available on a cluster.

Templates are defined through specifying combinations of and constraints to these services. Using templates, Loom provides
server administrators the ability to create flexible customizations for cluster provisioning. But before they can specify or 
select types of **Templates**, an administrator must create these **Templates**. Administrators can do so in 
two ways: 1) with the :doc:`Admin UI </guide/admin/ui>` and 2) through the :doc:`Web Services REST Cluster API</rest/templates>`. 

Because the notion of **Templates** is central to the Loom cluster creation, please read the :doc:`Web Services REST Cluster 
API</rest/templates>` or :doc:`Admin UI </guide/admin/ui>` carefully to design compatible templates with all the constraints 
and combinations in mind. 


.. _provision-templates2:
Tools
=====
Loom includes several tools for administrators to simplify their administrative tasks. Apart from the
:doc:`Admin UI </guide/admin/ui>`, Loom provides several additional :doc:`metrics and monitoring </guide/admin/monitoring>` tools.
Loom allows administrators to :doc:`configure their servers </guide/admin/server-config>`
and  :doc:`write custom plugins </guide/admin/plugins>` for allocating machines on your providers or to implement custom services.
For those administrators who are more command line driven, or wish to write quick administrative scripts,
they can employ the :doc:`Web services API </rest/index>`, and use :doc:`macros</guide/admin/macros>` to refer to nodes on a cluster.

Please refer to the following pages for more details:

        * :doc:`Administration User Interface </guide/admin/ui>`

        * :doc:`Server Configuration </guide/admin/server-config>`

        * :doc:`Monitoring and Metrics </guide/admin/monitoring>`

        * :doc:`Macros </guide/admin/macros>`

        * :doc:`Provisioner Plugins </guide/admin/plugins>`

        * :doc:`REST Web Service Interface </rest/index>`

