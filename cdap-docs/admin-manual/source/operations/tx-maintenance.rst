.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

.. _tx-maintenance:

===============================
Transaction Service Maintenance
===============================

.. highlight:: console

.. _tx-maintenance-pruning-invalid-transactions:

Pruning Invalid Transactions
============================
The Transaction Service keeps track of all invalid transactions so as to exclude their
writes from all future reads. Over time, this *invalid* list can grow and lead to
performance degradation. To avoid that, you can prune the *invalid* list after the data of
invalid transactions has been removed |---| the removal of which happens during major
HBase compactions of the transactional tables.

To prune the invalid list manually, follow these steps:

1. Find the minimum transaction state cache reload time across all HBase region servers,
   by finding the last occurrence of the following line in the HBase region server logs::

     [<instance.name>] Transaction state reloaded with snapshot

   ``<instance.name>`` is the unique identifier for the CDAP instance being pruned as defined in
   :ref:`cdap-site.xml <appendix-cdap-site.xml>`. The default value for it is ``cdap``.

   Run the following command on each region server to get the transaction cache state reload time
   after replacing the ``<instance.name>`` with the right value::
 
     grep -F "[<instance.name>] Transaction state reloaded with snapshot" <region-server-log-file> | tail -1
   
   This should give lines as shown below. Each line below represents one line from each
   region server::
 
     15/08/22 00:22:34 INFO coprocessor.TransactionStateCache: [cdap] Transaction state reloaded with snapshot from 1440202895873
     15/08/22 00:22:42 INFO coprocessor.TransactionStateCache: [cdap] Transaction state reloaded with snapshot from 1440202956306
     15/08/22 00:22:44 INFO coprocessor.TransactionStateCache: [cdap] Transaction state reloaded with snapshot from 1440202956306
     15/08/22 00:22:47 INFO coprocessor.TransactionStateCache: [cdap] Transaction state reloaded with snapshot from 1440202956306
     15/08/22 00:23:34 INFO coprocessor.TransactionStateCache: [cdap] Transaction state reloaded with snapshot from 1440202956306
 
   Pick the minimum time across all region servers. In this case, ``1440202895873``.
 
#. Run a flush and a major compaction on all CDAP transactional tables.
 
#. Wait for the major compaction to complete.
 
#. Get the minimum time to obtain the ``pruneTime``. In this case, ``1440202895873``.
 
#. If the CDAP tables are replicated to other clusters, see the section below
   (:ref:`tx-maintenance-pruning-replicated`) to obtain the ``pruneTime`` for the slave
   clusters.
 
#. The final ``pruneTime`` is the minimum ``pruneTime`` across all replicated clusters (if
   any); let this be time ``t``.

Now, the invalid transaction list can be safely pruned until ``(t - 1 day)`` using a call
to :ref:`truncate invalid transactions before a specific time
<http-restful-api-transactions-truncate>`.

The current length of the invalid transaction list can be obtained using a call to
:ref:`retrieve the number of invalid transactions <http-restful-api-transactions-number>`.

.. _tx-maintenance-pruning-replicated:

Pruning Invalid Transactions in a Replicated Cluster
----------------------------------------------------
1. Copy over the latest transaction snapshots from the master cluster to the slave cluster.

#. Wait for three to four minutes for the latest transaction state to be reloaded from the
   snapshot.

#. Run steps 1 to 5 from the above section (:ref:`tx-maintenance-pruning-invalid-transactions`)
   on the slave cluster to find the ``pruneTime`` for that slave cluster.

Automated Pruning of Invalid Transactions
-----------------------------------------
From CDAP 4.1 onwards, CDAP supports automated pruning of the invalid transactions list.
It is turned off by default. Please contact support@cask.co if you are
interested in turning it on.

For automated pruning to work in a secure Hadoop cluster with authorization enabled,
CDAP needs to be able to list all CDAP tables and the table descriptors of all CDAP tables in HBase.
If CDAP is not able to list the table descriptors of all CDAP tables, running automated pruning
can lead to data inconsistency.

Note that the automated pruning in 4.1 works only on a *non-replicated* cluster.
