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
pairs that the provisioner plugin needs. The key-value pairs may vary for different provisioner plugins. 

Hardware Type
^^^^^^^^^^^^^
A hardware type is an entity that unifies hardware from different providers into a single entity that can be referenced elsewhere.  
Administrators specify a unique name, a description, and a mapping of provider name to provider specific key-value pairs.  For example,
an administrator can define a "small" hardware type.  In it, the admin maps the "amazon" provider to have a key-value pair with "flavor" 
as a key and "m1.small" as a value.  The "amazon" provider understands what a flavor is, and understands what the "m1.small" flavor refers
to. Similarly, the admin maps the "rackspace" provider to have a key-value pair with "flavor" as key and "2" as value.  The "rackspace" 
provider understands what a flavor is and what the "2" flavor refers to.  Now, elsewhere in Loom, the administrator can simply refer to 
the "small" hardware type instead of referring to different flavors identifies that depend on what provider is in use.  

Image Type
^^^^^^^^^^
An image type is an entity that it unifies images from different providers into a single entity that can be referenced elsewhere.
Administrators specify a unique name, a description, and a mapping of provider name to provider specific key-value pairs.  For example,
an administrator can create a "centos6" image type that maps
the "amazon" provider to some identifier for an image that uses centos6 as its operating system, and that maps the "rackspace" provider
to some other identifier for an image that uses centos6 as its operating system.  Now, elsewhere in Loom, the administrator can simply 
refer to the "centos6" image type instead of referring to different image identifies that depend on what provider is in use.

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

Layout constraints define which services must and can’t coexist on the same node. For example, in a hadoop cluster, you generally want 
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
can a set of image types that it must be placed with. Any node with that service must use one of the image types in the array. If
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

Solver
======
Users can make requests to perform different cluster management operations, such as creating, deleting, shrinking, expanding, configuring,
starting, stopping, and upgrading clusters.  Some of these operations change a cluster layout while others are performed on an existing 
cluster without any layout change.  A cluster layout defines the exact set of nodes for a cluster, where each node contains which hardware 
and image types to use, as well as the set of services that should be placed on the node.  Operations that can change a cluster layout are
first sent to the Solver, which will find a valid cluster layout and then send the layout and operation on to the Planner. Operations that
will not change a cluster layout are sent directly to the Planner. 

Overview
^^^^^^^^
The solver is responsible for taking an existing cluster layout, the template associated with the cluster, user specified properties, and
finding a valid cluster layout that satisfies all input. There are 3 stages involved in solving a cluster layout. The first is finding
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
node layout satisfies all constraints, it is added a valid node layouts. If not it is discarded. 
After that, if there are multiple valid node layouts for a service set, one is chosen and the others are discarded. Which node layout is 
chosen is deterministically chosen by a comparator that compares node layouts. An example of this process is shown in the figure below.

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

.. figure:: /_images/planner_dag.png 
    :align: center
    :alt: Planner Dag
    :figclass: align-center

Grouping into Stages
^^^^^^^^^^^^^^^^^^^^

.. figure:: /_images/planner_dag_stages.png 
    :align: center
    :alt: Planner Dag Stages
    :figclass: align-center

Task Coordination
^^^^^^^^^^^^^^^^^


