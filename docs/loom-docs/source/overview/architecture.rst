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
a cluster template into an execution plan by solving the service placement problem using complex constraints. Additionally, 
the server exposes and manages all the web services supported by Loom.

Loom Provisioner
================
Once the execution plan is materialized, the Loom Server creates tasks that are executed as a DAG plan. These tasks are then
queued for Provisioners to execute. After completing a task, the provisioners report the tasks' status back to the Loom Server.

Provisioners support a pluggable architecture for integrating different infrastructure providers (e.g. OpenStack, Rackspace, Amazon Web Services, Google App Engine, and Joyent) 
and automators (e.g. Chef, Puppet, Shell scripts). Provisioners are not directly installed on the target host, but rather use SSHD to interact with the remote host, making Loom's architecture simple and secure. Since multiple provisioners can work concurrently, this layer of provisioners support execution of thousands of concurrent tasks to render scalability.

Loom Web UI
===========
Loom Web UI exposes two major functionalities: an Admin view and a User view. The Admin view allows system administrators or server administrators to configure
providers, disk images, machine hardware types, and software services. The UI also supports the construction of cluster templates that
can be accessed and executed by users. Cluster templates are blueprints that administrators expose
to their users that enable their users to instantiate clusters.

Using the user UI, end users are able to see all the cluster templates available to them and use them to create
instances of clusters. The end user can retrieve details of their own clusters, (including the cluster's metadata)
and execute the following operations to their clusters: create, delete, amend, update, and monitor.
