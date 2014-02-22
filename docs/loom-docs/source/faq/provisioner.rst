:orphan:

.. _faq_toplevel:

.. index::
   single: FAQ: Loom Provisioner
============================
Loom Provisioner
============================

When something goes wrong, how can I look at the logs?
------------------------------------------------------

When a user provisions a cluster, the logs for the nodes that fail are reported at node level, making 
it easier to investigate the node level errors.

How many provisioners should I run?
-----------------------------------
A good rule of thumb is:

C * N * n / S 

Where:

C = Number of concurrent cluster creations you need to support 

N = Average number of nodes per cluster

n = Average number of services per cluster node

S = Average total number of services in a cluster 

Since only one operation can be running on a node at any given time, you will never need more provisioners
than the number of concurrent node creations you need to support. However, because work on a cluster is broken up into stages, and because 
not all cluster nodes will be busy in each stage, it is usually fine to have less than the total number of created nodes.
The formula above tries to capture the average number of tasks per cluster creation stage times the number of clusters being created at any given time.

For example, if you need to support 10 concurrent cluster creations, on average each node across the clusters
contains 4 services, on average each cluster contains 8 services, and each cluster is on average 10 nodes 
in size, so a good starting point is 10 * 10 * 4 / 8 = 50 provisioners.  Ultimately, if your provisioners are always busy, you probably want to add more.  
If they are mostly idle, you probably want to decrease number. With a lot of provisioners, you will want to edit the number of worker threads in the loom server accordingly.

Memory limits should also be considered when deciding on the number of provisioners to run. On average, each provisioner
requires roughly 200MB of memory, hence, you may want to consider your system specifications
when allocating the number of provisioners.

Can I increase the number of provisioners on the fly?
-----------------------------------------------------
No, you can't in this release. We intend to support it in a future release. 

How many resources does each provisioner need?
----------------------------------------------
Provisioners are very light-weight daemons. They are state-less and require less
amount of memory. Nor are they CPU bound. For most of the time, they are idle, waiting for operations to 
finish on a remote host. Currently, each Provisioner can handle one task at a time. In future releases, 
the Provisioner will support performing multiple tasks currently.

Is it possible for multiple provisioners to perform operations on the same node at the same time?
-------------------------------------------------------------------------------------------------
During normal operations, there is only one Provisioner performing operation on the machine. In case 
of failure, the previous operation would be timed out, and a new operation would be started.

Can I run different types of provisioners at the same time?
-----------------------------------------------------------
Currently, the system doesn't support registration of multiple types of provisoners. All Provisioners are currently 
expected to be of the same type.

Can I customize provisioners?
-----------------------------
Yes, you can. Please look for more information :doc:`here</guide/admin/plugins>`

What happens when I stop a provisioner while it is performing a task?
---------------------------------------------------------------------
The provisioner will attempt to finish any task it is performing before stopping.  This means it may take minutes
before a provisioner shuts itself down after receiving a kill signal.

Can the chef-automator use a Chef server ?
------------------------------------------
Currently, it does not. The current version uses only chef-solo, however, future version of Loom will support both chef-solo and
chef-client. 
