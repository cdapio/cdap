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
All Provisioners are currently expected to be all of same types.

Can I customize provisioners?
-----------------------------
Yes, you can. Please look for more information :doc:`here</guide/admin/plugins>`
