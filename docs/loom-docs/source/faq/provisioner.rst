:orphan:

.. _faq_toplevel:
.. include:: /toplevel-links.rst

============================
Loom Provisioner
============================

When something goes wrong, how can I look at the logs?
------------------------------------------------------

When a user provisions a cluster, the logs for the nodes that fail are reported at node level making 
it easier to investigate the node level errors.

How many provisioners should I run?
-----------------------------------
This will depend on the templates and services you setup in Loom.  Since only one operation can be running
on a node at any given time, you will never need more provisioners than the number of concurrent node creations
you need to support.  However, because work on a cluster is broken up into stages, and because not all cluster
nodes will be busy in each stage, a decent rule of thumb is to take the average number of concurrent node 
creations you need to support and multiply it by the average dominant node share across your clusters.  
By dominant node share, we mean the percentage of a cluster taken up by 
the most common type of node in the cluster.  For example, in a hadoop cluster, most of the cluster consists 
of slaves (datanodes, nodemanagers, etc).  If your slaves take up 80% of your clusters, your dominant node
share is 0.80.  So if you are normally creating 100 nodes at any given point in time, and you only 
have hadoop templates, you can start off with 80 provisioners.    
Ultimately, if your provisioners are always busy, you probably want to add more.  If they are mostly
idle, you probably want to decrease number.  With a lot of provisioners, you will want to edit the number 
of worker threads in the loom server accordingly.  

How many resources does each provisioner need?
----------------------------------------------
Provisioners are very light-weight daemons. Provisioners are also state-less and require very less
amount of memory. They are not CPU bound too. Most of the time, they are waiting for operations to 
be completed on a remote host. Currently, each Provisioner can handle one task at a time. In future releases, 
the Provisioner will support performing multiple tasks currently.

Is it possible for multiple provisioners to perform operations on the same node at the same time?
-------------------------------------------------------------------------------------------------
During normal operations there is only one Provisioner performing operation on the machine. In case 
of failure, the previous operation would be timed out and new operation would be started.

Can I run different types of provisioners at the same time?
-----------------------------------------------------------
Currently, the system doesn't support registration because of which we cannot have different types of provisoners. 
All Provisioners are currently expected to be of the same type.

Can I customize provisioners?
-----------------------------
Yes, you can. Please look for more information :doc:`here</guide/admin/plugins>`
