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

1. Find the minimum transaction state cache reload time across all region servers,
   by finding the last occurrence of ``Transaction state reloaded with snapshot`` in the
   HBase region server logs. This can be done by running the following command on each
   region server::
 
     grep "Transaction state reloaded with snapshot" <region-server-log-file> | tail -1
   
   This should give lines as shown below. Each line below represents one line from each
   region server::
 
     15/08/22 00:22:34 INFO coprocessor.TransactionStateCache: Transaction state reloaded with snapshot from 1440202895873
     15/08/22 00:22:42 INFO coprocessor.TransactionStateCache: Transaction state reloaded with snapshot from 1440202956306
     15/08/22 00:22:44 INFO coprocessor.TransactionStateCache: Transaction state reloaded with snapshot from 1440202956306
     15/08/22 00:22:47 INFO coprocessor.TransactionStateCache: Transaction state reloaded with snapshot from 1440202956306
     15/08/22 00:23:34 INFO coprocessor.TransactionStateCache: Transaction state reloaded with snapshot from 1440202956306
 
   Pick the minimum time across all region servers. In this case, ``1440202895873``.
 
#. Run a flush and a major compaction on all CDAP transactional tables.
 
#. Wait for the major compaction to complete.
 
#. Find the minimum prune time across all queues by running the tool
   ``SimpleHBaseQueueDebugger`` (note that authorization is disabled when running the tool
   so that CDAP can read all users' queue tables)::
 
 
     $ /opt/cdap/master/bin/cdap run co.cask.cdap.data.tools.SimpleHBaseQueueDebugger
 
     Results for queue queue:///ns1/WordCount/WordCounter/counter/queue: min tx timestamp: 1440198510309
     Results for queue queue:///ns1/WordCount/WordCounter/splitter/wordArrayOut: min tx timestamp: 1440198510280
     Results for queue queue:///ns2/WordCount/WordCounter/counter/queue: min tx timestamp: n/a
     Results for queue queue:///ns2/WordCount/WordCounter/splitter/wordArrayOut: min tx timestamp: n/a
     Results for queue queue:///default/WordCount/WordCounter/counter/queue: min tx timestamp: 1440194184568
     Results for queue queue:///default/WordCount/WordCounter/splitter/wordArrayOut: min tx timestamp: 1440194184476
     Results for queue queue:///default/WordCount/WordCounter/splitter/wordOut: min tx timestamp: 1440194184476
     Total results for all queues: min tx timestamp: 1440194184476
 
   Pick the timestamp from the line beginning ``Total results for all queues``. In this case, ``1440194184476``.
 
#. Get the minimum time from the above two steps, to obtain the ``pruneTime``. In this case, ``1440194184476``.
 
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


Using the Queue Debugger Tool
=============================
The Queue Debugger Tool allows you to calculate queue statistics, and can be useful in
solving problems with queues. This is a debug tool for a queue, returning information such
as how many entries are in a queue, how many have been processed, and how many are not yet
processed. 

Background
----------
Each flow has a queue table. Within each queue table are multiple queues. There is one
queue for each connection between flowlets. As each flowlet may have multiple consuming
flowlets, this results in there being multiple queues.

The name of a queue is determined by the producer flowlet. The consumer flowlet (required
for the command line parameters of the tool) is referred to as the consumer. 

Running the Tool
----------------
It's important that the tool be run with the same classpath as CDAP Master to avoid
problems with the ordering of classes and to ensure that the CDAP classes appear before
the HBase classpath. This is to avoid the invocation of any older versions of the ASM
library that are present in the HBase classpath.

The easiest way to start the tool with the same classpath as CDAP Master is to use::

  $ /etc/init.d/cdap-master run co.cask.cdap.data.tools.HBaseQueueDebugger
  
or::

  $ /opt/cdap/master/bin/cdap run co.cask.cdap.data.tools.HBaseQueueDebugger
  
Running the ``help`` option will give a summary of commands and required parameters.

The tool ``SimpleHBaseQueueDebugger`` is a wrapper of the tool that that uses a set of
defaults useful for displaying the minimum transaction time for all events in all queues.
