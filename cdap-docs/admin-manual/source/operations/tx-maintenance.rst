.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. _tx-maintenance:

===============================
Transaction Service Maintenance
===============================

.. highlight:: console

Pruning Invalid Transactions
============================
The Transaction Service keeps track of all invalid transactions so as to exclude their writes from all future reads. 
Over time, this *invalid* list can grow and lead to performance degradation. To avoid that, you can prune the *invalid*
list after the data of invalid transactions has been removed |---| the removal of which happens during major HBase 
compactions of the transactional tables.

To prune the invalid list manually, follow these steps:

- Run a major compaction on all CDAP transactional tables.

- Find the minimum transaction state cache reload time across all region servers before the major compaction started.
  This can be done by grepping for ``Transaction state reloaded with snapshot`` in the HBase region server logs.
  
  This should give lines such as::

    15/08/22 00:22:34 INFO coprocessor.TransactionStateCache: Transaction state reloaded with snapshot from 1440202895873
    15/08/22 00:22:42 INFO coprocessor.TransactionStateCache: Transaction state reloaded with snapshot from 1440202956306
    15/08/22 00:22:44 INFO coprocessor.TransactionStateCache: Transaction state reloaded with snapshot from 1440202956306
    15/08/22 00:22:47 INFO coprocessor.TransactionStateCache: Transaction state reloaded with snapshot from 1440202956306
    15/08/22 00:23:34 INFO coprocessor.TransactionStateCache: Transaction state reloaded with snapshot from 1440202956306

  Pick the minimum time across all region servers. In this case, ``1440202895873``.

- Find the minimum prune time across all queues by running the tool ``SimpleHBaseQueueDebugger``
  (note that authorization is disabled when running the tool so that CDAP can read all users' tables)::


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

- Get the minimum time from the above two steps, let this be time ``t``. In this case, ``1440194184476``.

Now, the invalid transaction list can be safely pruned
until ``(t - 1 day)`` using :ref:`a call <http-restful-api-transactions-truncate>`
with the :ref:`Transaction Service HTTP RESTful API <http-restful-api-transactions-truncate>`.

The current length of the invalid transaction list can be obtained using 
:ref:`a call <http-restful-api-transactions-number>` 
with the :ref:`Transaction Service HTTP RESTful API <http-restful-api-transactions-number>`.


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
