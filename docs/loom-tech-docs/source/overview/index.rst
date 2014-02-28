:orphan:
.. index::
   single: Overview
.. _index_toplevel:

============
Architecture
============

.. _architecture:

Overview
========
Before moving on, it is necessary to define some terminology that will be used throughout the remainder of the document. 

A node is a machine, real or virtual, that consists of a service or collection of services running on hardware, where
a service is some piece of software.  

A cluster is a collection of nodes.  Typically, nodes in a cluster can communicate
with one another to provide some functionality that each individual node is unable to fully provide.  An example of a cluster
is a collection of nodes that work together to provide a distributed file system like Hadoop HDFS.  

A cluster management operation is an action that is performed on a cluster, affecting some or all of the nodes in the cluster.
Some examples are creating, deleting, shrinking, expanding, upgrading, rollback, configuring, starting, and stopping a cluster. 
Cluster level operations typically involve many node level tasks. For example, configuring a cluster usually requires configuring 
services on each node in the cluster, and may also involve stop and starting those services in a particular order.

A task is an action that is performed on a node.  Some examples are creation and deletion of the node, and the installation,
initialization, configuration, start, stop, or removal of a service on the node.  

Loom is a system that allows users to manage clusters as single entities; users no longer have to know about nodes. 
Loom consists of two major pieces: the server and the provisioners.  The server is responsible for determining what needs to be 
done for different cluster management operations.  It breaks down cluster level operations into node level tasks, coordinating 
which tasks should be performed at what time.  It also stores the state of all provisioned clusters as well as a history of all
operations performed.  The server does not perform any of the tasks, but places tasks onto a queue for the provisioners to 
execute.  Provisioners are responsible for actually executing the tasks on the desired node, reporting back to the server 
success or failure of its given task.  An architectural overview is shown in the figure below. 

.. figure:: /_images/Loom-Architecture.png
    :align: center
    :alt: Loom Architecture
    :figclass: align-center

We now give an overview of each component before going into their details.

Server
======
The Loom server performs several separate, but related roles.  The first role is to interact with administrators to define providers,
hardware, images, services, and templates that can be used for cluster management purposes. These definitions are persistently 
stored, as illustrated in the figure by the Admin APIs box and Store. The second role is to create plans to perform cluster management
operations. Users make cluster management operation requests through the User APIs, which are then sent to the Solver and Planner to 
transform the cluster level operation into a plan of node level tasks.
The final role is to interact with provisioners to coordinate the execution of those node level tasks.  In the figure, this role 
is performed through the Provisioner APIs box, Task Queue, and Janitor. 

Provisioner
===========
Provisioners are responsible for taking tasks from the server, executing the task, and reporting back to the server whether or not the 
task was successfully performed. Provisioners perform tasks by using the provider or automator plugin specified in the task.  Provider plugins
are used to allocate, delete, and manage machines using different infrastructure providers such as OpenStack, Rackspace, Amazon Web Services, 
Google App Engine, and Joyent. Automator plugins are responsible for implementing the various services defined on a cluster.  For example, a 
Chef automator plugin could be used to invoke Chef recipes that install, configure, initialize, start or stop your application.  Various plugins may be 
implemented to support desired technologies, such as a Puppet plugin, or even shell commands.  
Provisioners are not directly installed on the target host, but rather use SSHD to interact with the remote host, making Loom's architecture 
simple and secure. Since multiple provisioners can work concurrently, this layer of provisioners support execution of thousands of concurrent
tasks.
