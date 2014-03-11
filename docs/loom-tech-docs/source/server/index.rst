:orphan:
.. index::
   single: Loom Server
.. _index_toplevel:

===========
Loom Server
===========

The Loom server performs several separate, but related roles.  The first role is to interact with administrators to define providers,
hardware, images, services, and templates that can be used for cluster management purposes. These definitions are persistently
stored, as illustrated in the figure by the Admin APIs box and Store. The second role is to create plans to perform cluster management
operations. Users make cluster management operation requests through the User APIs, which are then sent to the Solver and Planner to
transform the cluster level operation into a plan of node level tasks.
The final role is to interact with provisioners to coordinate the execution of those node level tasks.  In the figure, this role
is performed through the Provisioner APIs box, Task Queue, and Janitor.


Admin Defined Entities 
======================
The administrator is responsible for defining Providers, Hardware Types, Image Types, Services and Cluster Templates.  These entities
are required to perform cluster management operations.

Providers
^^^^^^^^^
Loom can create and delete machines, real or virtual, through different providers. A provider can be an instance of an Infrastructure
as a Service provider, such as Openstack, Rackspace, Joyent, or Amazon Web Services. It can also be a custom provider created by the 
user that manages bare metal, or really any system that can provide a machine to run services on. The Loom server is not concerned with
how to create and delete machines. An administrator creates a provider by specifying a unique name, a description, and a list of key-value
pairs that the provisioner plugin needs. The key-value pairs may vary for different provisioner plugins. For example, one key-value pair 
may specify the url to use to communicate with the provider. Another key-value pair may specify the user id to use with the provider, 
and another may specify the password to use.

Hardware Type
^^^^^^^^^^^^^
A hardware type is an entity that unifies hardware from different providers into a single entity that can be referenced elsewhere.  
Administrators specify a unique name, a description, and a mapping of provider name to provider specific key-value pairs.  For example,
an administrator can define a "small" hardware type.  In it, the admin maps the "amazon" provider to have a key-value pair with "flavor" 
as a key and "m1.small" as a value.  The "amazon" provider understands what a flavor is, and understands what the "m1.small" flavor refers
to. Similarly, the admin maps the "rackspace" provider to have a key-value pair with "flavor" as key and "2" as value.  The "rackspace" 
provider understands what a flavor is and what the "2" flavor refers to.  Now, elsewhere in Loom, the administrator can simply refer to 
the "small" hardware type instead of referring to different flavor identities that depend on what provider is in use.  

Image Type
^^^^^^^^^^
An image type is an entity that it unifies images from different providers into a single entity that can be referenced elsewhere.
Administrators specify a unique name, a description, and a mapping of provider name to provider specific key-value pairs.  For example,
an administrator can create a "centos6" image type that maps
the "amazon" provider to some identifier for an image that uses centos6 as its operating system, and that maps the "rackspace" provider
to some other identifier for an image that uses centos6 as its operating system.  Now, elsewhere in Loom, the administrator can simply 
refer to the "centos6" image type instead of referring to different image identities that depend on what provider is in use.

Services
^^^^^^^^
A service is a piece of software that can run on a node.  An administrator defines a service by giving it a unique name, a description,
a set of other services it depends on, and a set of provisioner actions that can be run with the service.  Provisioner actions contain
an action that can be performed on a service on a node, such as install, remove, initialize, configure, start, and stop.  It also contains
a script and any optional data that the automator plugin will use to actually perform the task. 

Cluster Templates
^^^^^^^^^^^^^^^^^
A template is a blueprint that describes how a cluster should be laid out by specifying how different services, image types, and hardware
types can be placed on nodes in a cluster.  Templates contain enough information that a user only needs to specify a template and a 
number of machines to create a cluster.  Each cluster template has a unique name, a short description, and a section devoted to 
compatibilities, defaults, and constraints. 

Template Compatibilities
^^^^^^^^^^^^^^^^^^^^^^^^
A cluster template defines 3 things in its compatibility section. The first is a set of services that are compatible with the template. 
This means that when a user goes to create a cluster with this template, the user is allowed to specify any service from this set as 
services to place on the cluster. Loom will not automatically pull in service dependencies, so the full set of compatible services must be defined.
Next, a set of compatible hardware types is defined. This means only hardware types in the compatible set can be used to create a cluster. 
Similarly, the compatible image types are defined, where only image types in the compatible set can be used to create a cluster.

Template Defaults
^^^^^^^^^^^^^^^^^
The defaults section describes what will be used to create a cluster if the user does not specifically specify anything beyond the 
number of machines to use in a cluster. Everything in this section can be overwritten by the user, though it is likely only advanced 
users will want to do so. Templates must contain a set of default services, a default provider, and a default config. Optionally, a 
hardware type to use across the entire cluster, and an image type to use across the entire cluster, may be specified. The default services 
must be a subset of the services defined in the compatibility section. Similarly, if a hardwaretype or imagetype is specified, it must be 
one of the types given in the compatibility section. Lastly, the config is passed straight through to provisioners, usually describing 
different configuration settings for the services that will be placed on the cluster.

Template Constraints
^^^^^^^^^^^^^^^^^^^^
Currently, Loom supports two classes of constraints – layout and service.  However, the general idea of a constraint based template is
not restricted to just these types of constraints. Many additional types of constraints can be thought of and potentially added.

Layout constraints define which services must coexist with other service on the same node and which service can’t coexist on the same node. 
For example, in a hadoop cluster, you generally want 
datanodes, regionservers, and nodemanagers to all be placed together. To achieve this, an administrator would put all 3 services 
in the same “must coexist” constraint. Must coexist constraints are not transitive. If there is one constraint saying serviceA must coexist 
with serviceB, and another constraint saying serviceB must coexist with serviceC, this does not mean that serviceA must coexist with serviceC. 
This is to prevent unintended links between services, especially as the number of must coexist constraints increase. If a must coexist rule 
contains a service that is not on the cluster, it is shrunk to ignore the service that is not on the cluster. For example, a template may be 
compatible with datanodes, nodemanagers, and regionservers. However, by default, you only put datanodes and nodemanagers on the cluster. 
A constraint stating that datanodes, nodemanagers, and regionservers must coexist on the same node will get transformed into a constraint 
that just says datanodes and nodemanagers must coexist on the same node.

The other type of layout constraint are can’t coexist constraints. For example, in a hadoop cluster, you generally do not want your namenode 
to be on the same node as a datanode. Specifying more than 2 services in a can’t coexist rule means the entire set cannot exist on the same 
node. For example, if there is a constraint that serviceA, serviceB, and serviceC can’t coexist, serviceA and serviceB can still coexist on 
the same node. Though supported, this can be confusing, so the best practice is to keep the can’t coexist constraints binary. 
Anything not mentioned in the must or can’t coexist constraints are allowed.

Service constraints define hardware types, image types, and quantities for a specific service that can be placed on the cluster. 
A service constraint can contain a set of hardware types that it must be placed with. Any node with that service must use one of 
the hardware types in the set. If empty, the service can go on a node with any type of hardware. Similarly, a service constraint 
can be a set of image types that a service must be placed with. Any node with that service must use one of the image types in the array. If
empty, the service can go on a node with any type of image. A service constraint can also limit the quantities of that service across 
the entire cluster. It can specify a minimum and maximum number of nodes that must contain the service across the entire cluster.  A ratio
can also be specified, stating that a service must be placed on at least x percent of nodes across the entire cluster, or at most x percent
of nodes across the entire cluster. Other types of constraints are possible. For example, a constraint could be added stating that there must 
always be an odd number of nodes with the specified service, or the service is only allowed if there are at least y nodes that have another
service.

Cluster templates differentiate Loom from other systems. Templates make it so that administrators dont have to specify every single detail
for every cluster. Normally, an administrator find out that a certain type of cluster needs to be created. The admin gets some hardware,
installs some operating system on it, then installs the necessary software on each node. The admin then goes and configures the services on 
each node, then starts and initializes the services in the correct order, depending on which services depend on which others. Everything is 
a manual process, and small tweaks to the cluster require manual changes and specialized knowledge. For example, creating a cluster with 5 
nodes may require a different layout than a cluster with 50 nodes. The administrator must then be involved in creating the 5 node cluster and 
then 50 node cluster. With templates, small tweaks are automatically taken care of, and manual steps are removed. 

Templates also give administrators power and flexibility.  An administrator can
make a template completely rigid, where every service, hardware, image, and configuration setting is specified and unchangeable by end users.
An administrator can also make a flexible template that allows end users to specify properties they are interested in, such as which 
services should be placed on the cluster and what hardware and image to use.   

Permissions Manager
===================
The Permissions Manager is in charge of deciding which users have permission to perform which action on which entity. An 
entity here is any of the previously mentioned admin defined entities, as well as actual cluster instances. The Permissions
Manager has a pluggable interface for integration with existing user management systems such as LDAP. From existing systems,
it can get a list of users as well as the groups the users belong to. For each user and entity pair, the Permissions Manager
determines what actions the user can perform on the entity. Some examples of actions are reading, writing, executing, and
granting. Read permission means the user is able to read the entity. Write permission means the user is able to change
the entity. Execute permission applies to certain types of entities. For example, execute permission on a cluster template
means the user is able to create a cluster with the template. Grant permission means the user is able to grant other users
permission to perform certain actions on the entity. Actions are not limited to those listed above and can include many more.
The actual implementation that takes a user and entity as input and outputs the permissible actions is left as a pluggable
interface so different Loom setups can have different policies. By default, Loom supports a super admin that has all 
permissions on all entities. The super admin is able to create admins that have permission to write admin entities and 
perform cluster operations, as well as grant permissions to other users in their group. 

However, the default permissions policy is not the only possible policy. For example, it is possible to create a policy where
the superadmin only has permission to create regular admins and grant them permission, but no permission to write any 
admin entities or perform any cluster operation. This may be desired if the role of the super admin is simply to delegate
responsibility to admins for each user group. Another policy may limit all admin permissions to be just that of writing
admin entities, but leave cluster operations completely for users. Admins therefore would not be able to read cluster 
information or perform cluster operations. A setup like this may be desired if privacy is important, and each user needs
to make sure only they are able to access their cluster and the data on their cluster. The type of permissions policy 
implemented by the Permissions Manager should not be limited to the scope mentioned in the previous examples. What is 
important is that the Permissions Manager is able to determine what a user is able to do with or to any given entity, and 
with or to other permissions regarding other users.

Solver
======
Users can make requests to perform different cluster management operations, such as creating, deleting, shrinking, expanding, configuring,
starting, stopping, and upgrading clusters.  Some of these operations change a cluster layout while others are performed on an existing 
cluster without any layout change.  A cluster layout defines the exact set of nodes for a cluster, where each node definition contains which hardware 
and image types to use, as well as the set of services that should be placed on the node.  Operations that can change a cluster layout are
first sent to the Solver, which will find a valid cluster layout and then send the layout and operation on to the Planner. Operations that
will not change a cluster layout are sent directly to the Planner. 

Overview
^^^^^^^^
The solver is responsible for taking an existing cluster layout, the template associated with the cluster, user specified properties, and
finding a valid cluster layout that satisfies all inputs. There are 3 stages involved in solving a cluster layout. The first is finding
valid service sets. The second is finding valid node layouts. The third is finding a valid cluster layout. It should be noted that what 
is described is just one way to find a cluster layout. There are many ways this constraint satisfaction problem could be solved. 

Finding Service Sets
^^^^^^^^^^^^^^^^^^^^
A service set is a set of services that can be placed on a node. The set of valid service sets will depend on the services
that should be placed on the cluster, as well as the constraints defined in the template. 
We define N as the number of services that must be placed on the cluster, and n as the number of services in a particular service set.  
For each n from 1 to N, we go through every possible service combination and check if the service combination is valid, given the constraints
defined in the template. If the service set is valid, it is added to the list of valid service sets. An example with 3 services is shown 
in the figure below.

.. figure:: /_images/service_sets.png
    :align: center
    :alt: Service Sets
    :figclass: align-center

We start with n=3, which has only one combination.  This service set is invalid because s1 cannot coexist with s2, so it is not added to the 
valid service sets.  Next we move on to n=2, which has 3 combinations.  Of these, {s1, s2} is invalid because s1 cannot coexist with s2.  
{s1, s3} is valid because it satisfies all the constraints and is added to the valid service sets.  {s2, s3} is invalid because s2 cannot coexist
with s3.  Finally, we move on to n=1, which has 3 combinations.  {s1} is invalid because s1 must coexist with s3.  {s2} is valid because it 
satisfies all the constraints and is added to the valid service sets.  {s3} is invalid because s1 must coexist with s3.  Thus, we end up with
2 valid service sets in this scenario. If there are no valid service sets, there is no solution and the cluster operation fails.

Finding Node Layouts
^^^^^^^^^^^^^^^^^^^^
A node layout describes a node and consists of a service set, hardware type, and image type. The goal in this stage is to take the valid 
service sets from the previous stage and find all valid node layouts that can be used in the cluster. A similar approach is taken to first 
find all valid node layouts. For each valid service set, each combination of service set, hardware type, and image type is examined. If the
node layout satisfies all constraints, it is added to the set of valid node layouts. If not it is discarded. 
After that, if there are multiple valid node layouts for a service set, one is chosen and the others are discarded. Which node layout is 
chosen is deterministically chosen by a comparator that compares node layouts. The comparator is pluggable and can be used that the same 
image is chosen across the entire cluster when possible. Or it could be used to prefer cheaper hardware when possible. Different users can
define their own to match their needs. An example of this process is shown in the figure below.

.. figure:: /_images/node_layouts.png 
    :align: center
    :alt: Node Layouts
    :figclass: align-center

In this example, there are two hardware types that can be used: hw1 and hw2. Also, there are two image types that can be used: img1 and img2.
The starting valid service sets are taken from the previous example.  Every possible node layout is examined.  Since there are 2 hardware 
types and 2 image types, this means there are 4 possible node layouts for each service set. Each one is checked against the constraints.
In this example, s1 must be placed on a node with hw1, and s2 must be placed on a node with img1. After each possible node layout is examined,
we end up with 4 valid node layouts.  However, there are 2 valid node layouts for each service set, which lets us narrow down the final set
until we end up with 2 final node layouts.  Which layout is chosen is deterministically chosen by a pluggable comparator. 

Finding Cluster Layout
^^^^^^^^^^^^^^^^^^^^^^
After the final set of node layouts is determined, the solver finds how many of each node layout there should be based on the number of nodes
in the cluster. It does this by first ordering the node layouts by preference, then searching through every possible cluster layout until it
finds a cluster layout that satisfies all constraints. The search is done in a deterministic fashion by trying to use as many of the more 
preferred node layouts as possible. Again the preference order is determined using a pluggable comparator. An example is illustrated in the 
figure below.

.. figure:: /_images/cluster_layout.png 
    :align: center
    :alt: Cluster Layout
    :figclass: align-center

In this example, the cluster must have 5 nodes, and there is a constraint that s1 must only be placed on one node, and there must be at least
one node with s2. The comparator decides that the node layout with s1 and s3 is preferred over the node layout with just s2. The search then
begins with as many of the first node as possible. At each step, if the current cluster layout is invalid, a single node is taken away from 
the most preferred node and given to the next most preferred node. The search continues in this way until a valid cluster layout is found,
or until the search space is completely exhausted. In reality, there are some search optimizations that occur that are not illustrated in the
figure. For example, there can only be at most 1 node of the first node layout since there can only be one node with s1. We can therefore skip
ahead to a cluster layout with only 1 of the first node layout and continue searching from there. 

It should be noted that the above examples only illustrate a small number of constraints, whereas many more constraints are possible. 
In fact, when shrinking and expanding a cluster, or when removing or adding services from an existing cluster, the current cluster itself 
is used as a constraint. That is, the hardware and image types on existing nodes cannot change and are enforced as constraints. 
Similarly, services uninvolved in the cluster operation are not allowed to move to a different node. 

Once a valid cluster layout has been found, it is sent to the Planner to determine what tasks need to happen to execute the cluster operation.
If no layout is found, the operation fails.

Planner
=======
The planner takes a cluster, its layout and a cluster management operation, and creates an execution plan of node level tasks that must be
performed in order to perform the cluster operation.  It coordinates which tasks must occur before other tasks, and which tasks can be 
run in parallel. Ordering of tasks is based on action dependencies that are inherent to the type of cluster operation being performed, and
also based on the service dependencies defined by the administrator. For example, when creating a cluster, creation of nodes must always 
happen before installing services on those nodes. That is an example of a dependency that is inherent to the cluster create operation.
An example of a dependency derived from services is if service A depends on service B, then starting service A must happen after service B was started.
The planner works by examining the cluster layout and action dependencies, creating a direct acyclic graphed (DAG) based on the cluster action
and cluster layout, grouping tasks that can be run in parallel into stages, and placing tasks that can currently be run onto a queue for 
consumption by the Provisioners. 

Creating the DAG
^^^^^^^^^^^^^^^^

Below is an example DAG created from a cluster create operation with the cluster layout shown in the examples above.

.. figure:: /_images/planner_dag.png 
    :align: center
    :alt: Planner Dag
    :figclass: align-center

For a cluster create operation, each node must be created, then each service on it must be installed, then configured,
then initialized, then started. In this example, service s3 depends on both s1 and s2. Neither s1 nor s2 depend on any
other service. Since s3 depends on both s1 and s2, the initialize s3 task cannot be performed until all services s1
and s2 on all other nodes in the cluster have been started. There is, however, no dependencies required for installation
and configuration of services.  

Grouping into Stages
^^^^^^^^^^^^^^^^^^^^
In the above example, many of the tasks can be performed in parallel, while some tasks can only be performed
after others have completed. For example, all of the create node tasks can be done in parallel, but the install
s2 task on node 2 can only be done after the create node 2 task has completed successfully. The Planner takes
the DAG and divides it into stages based on what can be done in parallel. An example is shown in the figure below. 

.. figure:: /_images/planner_dag_stages.png 
    :align: center
    :alt: Planner Dag Stages
    :figclass: align-center

The basic algorithm is to identify "sources" in the dag, group all sources into a stage, remove all sources and their edges,
and continue the loop until all tasks are gone from the dag. A "source" is a task that depends on no other task in the DAG.
For example, in the first iteration, all the create node tasks are sources and are therefore grouped into the same stage. Once
the create node tasks and their edges are removed from the DAG, the next iteration begins. All the install tasks are identified
as sources and grouped together into the second stage. This continues until we end up with the stages shown in the figure.  
Finally, the Planner also ensures that there is only one task for a given node in a stage. In the above example, stage 2 has
the install s1 task and install s3 task that both need to be performed on node 1. They are therefore split into separate stages
as shown in the final plan shown below.

.. figure:: /_images/planner_dag_stages2.png 
    :align: center
    :alt: Planner Dag Stages 2
    :figclass: align-center


Task Coordination
^^^^^^^^^^^^^^^^^
Each task in a stage can be performed concurrently, and all tasks in a stage must be completed before moving on to the next stage. 
That is, tasks in stage i+1 are not performed until all tasks in stage i have completed successfully.
Note that this staged approach is not the only way to coordinate execution of the tasks. For example, from the original DAG,
there is nothing wrong with performing the install s2 task on node 2 once the create node 2 task has completed, but the staged approach
will wait until all other create node tasks have completed before perform the install s2 task. Execution order and parallization can
be done in many ways; this is just one simple way to do it.

After the stages have been determined, the Planner will place all tasks in a stage onto a queue for consumption by the Provisioners.
In case a task fails, it is retried a configurable amount of times. Almost all tasks are idempotent with the exception of the create task.
If a create fails, it is possible that the actual machine was provisioned, but there was an issue with the machine. In this case,
the machine is deleted before another is created to prevent resource leaks. In case a Provisioner fails to reply back with a task failure
or success after some configurable timeout, the Planner will assume a failure and retry the task up to the configurable retry limit. 
There is a Janitor that runs in the background to perform the timeout.
Once all tasks in a stage are complete, the Planner places all tasks in the next stage onto the queue. 

Transaction Manager
^^^^^^^^^^^^^^^^^^^
Cluster operations in Loom are failably transactional, meaning Loom will try to ensure that either the entire operation
completes successfully or it does not complete, at all.
What this means is that there must be a way to roll back changes if an operation is unsuccessful. This is 
accomplished by the Planner working in conjunction with a Transaction Manager. The Transaction Manager is responsible for
maintaining and managing cluster state. It stores a snapshot of the cluster after each successful cluster operation. The 
snapshot contains every detail about a cluster. It contains the full cluster layout, every single configuration setting,
hostnames, ip addresses, ssh keys, every little detail. In this way, Loom is able to rollback to any previous cluster state
in case of an operation failure. Rolling back to a previous state is a functional rollback where the cluster layout is 
exactly the same, but where some node information may change, depending on the operation. For example, when shrinking
a cluster from 10 nodes to 5, it is possible 4 nodes are deleted but some issue happens on the fifth node and the operation
must be rolled back. In that case the Transaction Manager is able to tell the Planner that it needs to recreate 4 nodes with
what hardware and which image and with which services on each node and which configuration settings. But it cannot guarantee 
that the same ip addresses are used and the same hostnames are used. So funtionally the cluster is the same and the layout
is the same, but some details may change. After a rollback, the state is saved again as a separate entry to preserve the 
full cluster history. 

A cluster operation can fail if a given task is retried past the max retries configured for Loom. If this happens, the 
Planner will notify the Transaction Manager that the cluster operation has failed, and the Transaction Manager will send
the full state of the cluster prior to the start of the transaction. The Planner is then able to create another task plan
to rollback the state of the cluster. It does this by creating a new DAG based on the current failed DAG. It starts in the
current failed stage, and for each successfully completed task, it creates corresponding rollback tasks. It then works 
backwards in the original DAG, adding rollback tasks for each successfully completed task. For example, for a configure 
service A task on node X, the rollback task would be a configure service A task on node X, but with the previous 
configuration settings as given by the Transaction Manager. Similarly, for a install service B task on node Y, the rollback
task would be to remove service B on node Y. As another example, a create node Z task has a rollback task of delete node Z.
The Planner is thus able to work backwards to create a rollback task plan from the original failed plan. Before actually 
starting the rollback, the Transaction Manager stores a snapshot of the cluster for historical purposes. The planner then 
proceeds to coordinate the rollback tasks as before, by dividing the DAG into stages and placing the tasks onto the queue
for consumption by provisioners. If the rollback task plan also fails, the Transaction Manager stores a snapshot of the
state of the cluster at that moment, and marks the cluster for examination by an administrator. Cluster operations can
be tried again in the future once the errors have been investigated. This is what is meant by failably transactional. A
rollback can fail in which case the transaction has failed.



Audit Log
^^^^^^^^^
It is important that Loom provides an audit log so that every single operation is logged. Each step of the way, the Planner
will write to the log. It writes to the log when a task is placed onto the queue for consumption by a provisioner, again when
the task is taken from the queue, and once more when a provisioner comes back with the task status. Retries are logged in the
same way as separate tasks so a full and complete history is kept. The planner also works with the Transaction Manager to 
ensure that each task is tied to the correct cluster transaction so that audits can be performed for periods of time or by
cluster action.
