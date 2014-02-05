.. _overview_architecture:
.. include:: /toplevel-links.rst

========
Architecture
========

.. _architecture:
.. include:: overview-links.rst

.. figure:: Loom-Architecture.png
    :width: 700px
    :align: center
    :alt: Loom Architecture
    :figclass: align-center

**Loom Server**
^^^^^^^^^^^^^^^
Loom Server is responsible for storing and managing metadata around providers, services & cluster templates. It's 
also responsible for materializing a cluster template into a execution plan by solving the complex placement problem
involving complex constraints. Server also exposes and manages all the web services supported by Loom. 

**Loom Provisioner**
^^^^^^^^^^^^^^^
Loom Server upon materializing the execution plan creates tasks to be executed as a plan DAG. These tasks are then
put on a queue to be picked up by the provisioners to complete the tasks. The provisioners then report back the status 
of the tasks back to the Loom Server. Provisioners support a pluggable architecture for integrating different providers
(e.g. openstack, rackspace, joyent, aws, google engine) and automators (e.g. Chef, Puppet, shell script ...). Provisioners
are not installed on the target host, but they use SSHD to interact with the remote host - making the model simple to 
use. Also, this layer of provisioners provides a way to scale the system to support 1000s of concurrent tasks being executed.

**Loom Web UI**
^^^^^^^^^^^
Loom Web UI exposes two major facets namely Admin view and User view. Admin view allows the sysadmins or adminsistrators to configure
providers, manage images, machine types, services. It also supports building cluster templates that can then be made available to users
for materializing them. The cluster templates are blue prints of cluster the sysadmin or administrator want to expose to their users.
From user perspective, a user is able to list all the cluster templates available to him and then can create instances of those clusters
and get details of the clusters being created and also a lot of meta data of cluster are made available to user.
