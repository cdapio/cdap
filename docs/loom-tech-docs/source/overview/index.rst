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
Before moving on, it is necessary to define some terminology that will be used throughout the remainder of this document. 

A node is a machine, real or virtual, that consists of a service or collection of services running on hardware, where
a service is some piece of software.  

A cluster is a collection of nodes.  Typically, nodes in a cluster can communicate
with one another to provide some functionality that each individual node is unable to fully provide.  An example of a cluster
is a collection of nodes that work together to provide a distributed file system like Hadoop HDFS.  

A cluster management operation is an action that is performed on a cluster, affecting some or all of the nodes in the cluster.
Some examples are creating, deleting, shrinking, expanding, upgrading, rollback, configuring, starting, and stopping a cluster or 
its services. Cluster level operations typically involve many node level tasks. For example, configuring a cluster usually requires 
configuring services on each node in the cluster, and may also involve stopping and starting those services in a particular order.
Cluster management operations usually must obey some constraints as well. For example, when expanding a cluster, we usually do not
want to make any changes to existing nodes, and usually want the new nodes to mirror the types of nodes already existing in the cluster.

A task is an action that is performed on a node.  Some examples are creation and deletion of the node, and the installation,
initialization, configuration, start, stop, or removal of a service on the node.  

Loom is a system that allows users to manage clusters as single entities; users no longer have to know about nodes. 
Loom consists of two major pieces: the server and the provisioners.  The server is responsible for determining what needs to be 
done for different cluster management operations. It first takes the cluster management operation and cluster, solving the problem
of performing the operation while obeying all constraints that apply to the cluster. The result is that cluster level operations 
are broken down into node level tasks that are coordinated and performed in a way that all constraints are satisfied. 
It also stores the state of all provisioned clusters as well as a history of all
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
stored, as illustrated in the figure by the Admin APIs box and Store. There is also a permissions manager that hooks into existing user
management systems like LDAP. The permissions manager ensures that users can only perform actions they are authorized to perform. For example,
an admin from group A can only read and write entities belonging to group A and not entities belonging to group B.
The second role of the server is to create plans to perform cluster management operations. In short, the role is to transform cluster 
level operations into a plan of node level tasks, all of which can be wrapped in a transaction. 
Users make cluster management operation requests through the User APIs, which are then sent to an orchestrator that determines which
operations require constraints to be solved, and thus sent to the Solver, and which operations can be directly sent to the Planner.
The transaction manager ensures that operations are atomic, performing rollbacks if needed. It also records all actions performed for auditing purposes.
The final role is to interact with provisioners to coordinate the execution of those node level tasks.  In the figure, this role 
is performed through the Provisioner APIs box, Task Queue, and Janitor. The Planner will divide the cluster operation into a series of stages, 
with each stage containing one or more node level tasks that can be executed in parallel. It places all tasks in a stage onto the Task Queue. 
Provisioners periodically poll the Server, asking for tasks to execute. If the Task Queue has tasks, the Server will hand the task to the 
Provisioner and wait for it to report back with a success or failure. Based on the success or failure, the Planner can decide to wait for more 
tasks to finish, move on to the next stage, or initiate rollback plans in case the cluster operation cannot be successfully completed. The 
Janitor will periodically time out tasks if they have been taken from the queue, but the Server has not heard back about the task for more 
than a configurable threshold of time. The Planner works with the Transaction Manager throughout the process to provide a complete audit log 
of all tasks performed, and to store cluster state for rollback purposes.

Provisioner
===========
Provisioners are lightweight and stateless, responsible for taking node level tasks from the server, executing the task,
and reporting back to the server whether or not the task was successfully performed. Provisioners perform tasks by using the correct 
provisioner plugin, depending on the task. There are two types of provisioner plugins: provider plugins and automator plugins. Provider plugins
are used to allocate, delete, and manage machines using different infrastructure providers such as OpenStack, Rackspace, Amazon Web Services, 
Google App Engine, and Joyent. Automator plugins are responsible for implementing the various services defined on a cluster.  For example, a 
Chef automator plugin could be used to invoke Chef recipes that install, configure, initialize, start or stop your application.  Various plugins may be 
implemented to support desired technologies, such as a Puppet plugin, or even shell commands.  
Provisioners are not directly installed on the target host, but rather use SSHD to interact with the remote host, making Loom's architecture 
simple and secure. Since multiple provisioners can work concurrently, this layer of provisioners support execution of thousands of concurrent
tasks. Provisioners can also be managed by the Server to automatically scale according to workload.
The Server manages tasks on a queue and tracks queue length. If the queue length is consistently high, it can be an indication that there are 
not enough Provisioners to handle the workload. As another example, if the task queue is constantly growing in size over a long period of time, 
it is a strong indication that there are not enough Provisioners to handle a normal workload. Based on the size of the queue and some other 
possible metrics such as average task completion time and rate of growth for the queue, the Server can estimate how many more Provisioners are 
needed to handle the workload and spin them up directly, or notify an administrator with the suggested number of Provisioners to add. Similarly, 
if the task queue is constantly empty, it may be an indication that there are more Provisioners running than required to handle the normal workload. 
Based on metrics like the average time a task stays in the queue before being taken by a Provisioner, the Server can estimate how many Provisioners 
are actually required and either shut down some running Provisioners or notify an administrator with the suggested number of Provisioners to shut down.
