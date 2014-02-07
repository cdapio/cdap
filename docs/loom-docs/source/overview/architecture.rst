.. _overview_architecture:

.. include:: ../toplevel-links.rst
========
Architecture
========

.. _architecture:
.. figure:: /_images/Loom-Architecture.png
    :width: 700px
    :align: center
    :alt: Loom Architecture
    :figclass: align-center

**Loom Server**
^^^^^^^^^^^^^^^
Loom Server is responsible for storing and managing metadata around providers, services & cluster templates. It is
also responsible for materializing a cluster template into a execution plan by solving the complex placement problem
involving complex constraints. Furthermore, Loom Server exposes and manages all the web services supported by Loom.

**Loom Provisioner**
^^^^^^^^^^^^^^^^^^^^
Upon materializing the execution plan, Loom Server, creates tasks to be executed as a plan DAG. These tasks are then
placed in a queue to be picked up by Provisioners. After completing a tasks, the provisioners report the tasks's status
back to the Loom Server.

Provisioners support a pluggable architecture for integrating different infrastructure providers
(e.g. OpenStack, Rackspace, Amazon Web Services, Google App Engine and Joyent) and automators (e.g. Chef, Puppet and Shell script).
Provisioners are not directly installed on the target host, but rather use SSHD to interact with the remote host, making the model simple to
use. Also, this layer of provisioners provides a way to scale the system to support thousands of concurrent tasks being executed.

**Loom Web UI**
^^^^^^^^^^^^^^^
Loom Web UI exposes two major facets: an Admin view and a User view. Admin view allows system administrators and server administrators to configure
providers, disk images, machine hardware types and software services. The UI also supports building cluster templates that
can then be made available to users. The cluster templates are blueprints  the administrators wish to expose
to their users to enable them to materialize clusters with the template's settings.

From a user UI perspective, users are able to list all the cluster templates available to them and, from there, create
instances of those clusters. The user can also retrieve details of their own clusters, (including the cluster's metadata)
and make status changes to those clusters.
