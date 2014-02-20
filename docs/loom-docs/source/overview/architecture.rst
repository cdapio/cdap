.. _overview_architecture:
.. index::
   single: Architecture
============
Architecture
============

.. _architecture:
.. figure:: /_images/Loom-Architecture.png
    :width: 700px
    :align: center
    :alt: Loom Architecture
    :figclass: align-center

Loom Server
===========
Loom Server is responsible for storing and managing metadata around providers, services, and cluster templates. It materializes
a cluster template into an execution plan by solving the complex placement problem involving complex constraints. Furthermore, 
the server exposes and manages all the web services supported by Loom.

Loom Provisioner
================
Upon materializing the execution plan, the Loom Server creates tasks that are executed as a DAG plan. These tasks are then
queued for Provisioners to execute. After completing a task, the provisioners report the tasks's status back to the Loom Server.

Provisioners support a pluggable architecture for integrating different infrastructure providers (e.g. OpenStack, Rackspace, Amazon Web Services, Google App Engine, and Joyent) 
and automators (e.g. Chef, Puppet, Shell scripts). Provisioners are not directly installed on the target host, but rather use SSHD to interact with the remote host, making the 
model simple and secure. With multiple provisioners working concurrently, this layer of provisioners renders scalability to support execution of thousands of concurrent tasks.

Loom Web UI
===========
Loom Web UI exposes two major facets: an Admin view and a User view. The Admin view allows system administrators or server administrators to configure
providers, disk images, machine hardware types, and software services. The UI also supports constructing cluster templates that
can be accessed and employed by users. Basically, the cluster templates are blueprints that the administrators wish to expose
to their users to enable them to instantiate clusters with the template's settings.

From a user UI perspective, users are able to list all the cluster templates available to them and, from there, create
instances of those clusters. The user can also retrieve details of their own clusters, (including the cluster's metadata)
and make status changes to those clusters.
