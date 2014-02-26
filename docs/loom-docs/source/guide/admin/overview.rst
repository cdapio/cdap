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
parts and components come together to materialize into a cluster.

Concepts
========

As mentioned above, Loom works through the use of **Templates**, which dictate the configuration of the
clusters that users can spin up. An administrator can specify any number of such templates to put into
their **Catalog** for users. 

Several concepts central to cluster configuration are definable in Loom. These aspects are:

* **Providers** - Infrastructure providers (such as Amazon or OpenStack) that supply machines.

* **Hardware types** - Type of hardware (such as small, medium, or large) that can be used for the nodes of a cluster. 

* **Image types** - Basic disk images installed on the nodes of a cluster.

* **Services** - Bundled software services that can be placed on a cluster.

* **Cluster Template** - Blueprint describing show hardware, images, and services should be laid out to form a cluster.

Templates are defined by specifying hardware types, image types, and services that can be used in a cluster, as well
as a set of constraints that describes how those hardware types, image types, and services should be laid out in a cluster.
Template creation can be done in 
two ways: 1) :doc:`Admin UI </guide/admin/ui>` and 2) :doc:`Web Services REST Cluster API</rest/templates>`. 

Because the notion of **Templates** is central to the Loom cluster creation, please read the :doc:`Web Services REST Cluster 
API</rest/templates>` or :doc:`Admin UI </guide/admin/ui>` carefully to design templates that meet your needs. 


.. _provision-templates2:
Tools
=====
Loom includes several tools for administrators to simplify their administrative tasks. Apart from the
:doc:`Admin UI </guide/admin/ui>`, Loom provides several additional :doc:`metrics and monitoring </guide/admin/monitoring>` tools.
Loom allows administrators to :doc:`configure their servers </guide/admin/server-config>`
and  :doc:`write custom plugins </guide/admin/plugins>` for allocating machines with your providers or to implement custom services.
Administrators who are more command line driven, or who wish to write quick administrative scripts,
can employ the :doc:`Web services API </rest/index>`.

Please refer to the following pages for more details:

        * :doc:`Administration User Interface </guide/admin/ui>`

        * :doc:`Server Configuration </guide/admin/server-config>`

        * :doc:`Monitoring and Metrics </guide/admin/monitoring>`

        * :doc:`Macros </guide/admin/macros>`

        * :doc:`Provisioner Plugins </guide/admin/plugins>`

        * :doc:`REST Web Service Interface </rest/index>`

