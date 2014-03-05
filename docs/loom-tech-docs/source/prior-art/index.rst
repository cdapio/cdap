:orphan:
.. index::
   single: Prior Art
.. _index_toplevel:

=========
Prior Art
=========
Other cluster management systems exist.  For example, 
Amazon EMR provides a subset of Hadoop services (such as Hive, Pig, HBase, and MapReduce) and manages jobs and workflows on top
of those services. Loom, on the other hand, is a software agnostic, generic system for managing clusters of any layout and of 
any type. As a software agnostic system, Loom has no direct support for managing jobs on clusters, as its focus 
is on cluster creation and management, not jobs and workflow management.

The Apache Ambari project is aimed at making Hadoop management simpler 
by developing software for provisioning, managing, and monitoring Apache Hadoop clusters. Ambari aims to provide an easy-to-use Hadoop 
management web UI backed by its RESTful APIs. Ambari, however, does not support actually creating machines.  A list of machines must
be given to it. It is also directly tied to managing Hadoop clusters, whereas Loom is software agnostic and can be used to manage
any type of cluster, not just Hadoop. 

The Savanna project aims to provide users with simple means to provision a Hadoop 
cluster at OpenStack by specifying several parameters like Hadoop version, cluster topology, nodes hardware details, and others.
Again, Savanna is focused on managing Hadoop clusters, whereas Loom is software agnostic and can be used to manage any type of cluster.
Savanna has a concept of a template, but requires node groups to be explicitly defined for placing service on each node group.
There is no dynamic layout solver that takes the template and translates it into a placement of services on different nodes in the cluster.
Rather, the user must explicitly define which services should be placed on which type of node. Loom offers built-in logic for solving 
the service placement problem through the layout Solver and Planner. This capability differentiates Loom from any other current system.
