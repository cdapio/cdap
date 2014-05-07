============================================
placeholder
============================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. Slide Presentation HTML Generation
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/

.. include:: ../_slide-fragments/continuuity_logo.rst

.. |br_m00| raw:: html

   <br />

----

Module Objectives
=================

In this module, you will look at:

- The need for transactions
- OCC: Optimistic Concurrency Control
- Rules for transactions
- Disabling transactions: when and where

---

Transaction System
==================
.fx: center_title_slide

----

The Need for Transactions
=========================

A Flowlet processes the data objects received on its inputs one at a time

While processing a single input object, all operations, including the removal of the data
from the input, and emission of data to the outputs, are executed in a **transaction**
 
This provides us with **ACID:** atomicity, consistency, isolation, and durability properties:

- The process method runs under read isolation to ensure that it does not see dirty writes
  (uncommitted writes from concurrent processing) in any of its reads; it does see, 
  however, its own writes

- A failed attempt to process an input object leaves the data in a consistent state;
  it does not leave partial writes behind

- All writes and emission of data are committed atomically; either all of them or none of
  them are persisted

- After processing completes successfully, all its writes are persisted in a durable way

In case of failure, the state of the data is unchanged and processing of the input
object can be reattempted

This ensures "exactly-once" processing of each object

----

OCC
===
.fx: center_title_slide

----

OCC: Optimistic Concurrency Control
===================================

The Continuuity Reactor uses *Optimistic Concurrency Control* (OCC) to implement transactions

- Most relational databases use locks to prevent conflicting operations between transactions

- Under OCC we allow conflicting writes to happen

- When the transaction is committed, we can detect whether it has any conflicts: namely,
if during the lifetime of the transaction, another transaction committed a write for one
of the same keys that the transaction has written. In that case, the transaction is
aborted and all of its writes are rolled back.

In other words:

- If two overlapping transactions modify the same row, then the transaction that commits
first will succeed, but the transaction that commits last is rolled back due to a write conflict.

----

OCC: Optimistic Concurrency Control
===================================

Optimistic Concurrency Control is lockless and therefore avoids problems such as idle
processes waiting for locks, or even worse, deadlocks

However, it comes at the cost of rollback in case of write conflicts

We can only achieve high throughput with OCC if the number of conflicts is small

It is therefore a good practice to reduce the probability of conflicts wherever possible

----

Rules for Flows, Flowlets and Procedures
==================================================

Keep transactions short

- The Continuuity Reactor attempts to delay the beginning of each transaction as long as
  possible
- If your Flowlet only performs write operations, but no read operations, then all writes
  are deferred until the process method returns
- They are then performed and transacted, together with the removal of the processed
  object from the input, in a single batch execution
- This minimizes the duration of the transaction

- However, if your Flowlet performs a read, then the transaction must begin at the time of
  the read
- If your Flowlet performs long-running computations after that read, then the transaction
  runs longer, too, and the risk of conflicts increases
- It is therefore a good practice to perform reads as late in the process method as possible

There are two ways to perform an increment

- As a write operation that returns nothing, or
- As a read-write operation that returns the incremented value
- If you perform the read-write operation, then that forces the transaction to begin, and
  the chance of conflict increases
- Unless you depend on that return value, always perform an increment only as a write operation

Use hash-based partitioning

- For the inputs of highly concurrent Flowlets that perform writes
- This helps reduce concurrent writes to the same key from different instances of the Flowlet

Keeping these guidelines in mind will help you write more efficient and faster-performing code

----

The Need for Disabling Transactions (1 of 2)
============================================

Transactions providing **ACID** (atomicity, consistency, isolation, and durability) 
guarantees are useful in applications where data accuracy is critical:

- Billing applications
- Computing click-through rates

However, some applications—such as trending—might not need it

Applications that do not strictly require accuracy can trade off accuracy against
increased throughput by taking advantage of not having to write/read all the data
in a transaction

----

The Need for Disabling Transactions (2 of 2)
============================================

Disabling Transactions
----------------------

Transaction can be disabled for a Flow by annotating the Flow class with the 
``@DisableTransaction`` annotation:

.. sourcecode:: java

	@DisableTransaction
	class MyExampleFlow implements Flow {
	  ...
	}

While this may speed up performance, if—for example—a Flowlet fails, the system would not
be able to roll back to its previous state

You will need to judge whether the increase in performance offsets the increased risk of
inaccurate data

----

Transactions in MapReduce
===================================

For a MapReduce job that interacts with DataSets, the system creates a long-running transaction

Similar to the transaction of a Flowlet or a Procedure, here are rules to follow:

- Reads can only see the writes of other transactions that were committed
  at the time the long-running transaction was started

- All writes of the long-running transaction are committed atomically,
  and only become visible to others after they are committed

- The long-running transaction can read its own writes

However, there is a key difference: long-running transactions do not participate in
conflict detection

- If another transaction overlaps with the long-running transaction and writes to the same
  row, it will not cause a conflict but simply overwrite it

- It is not efficient to fail the long-running job based on a single conflict

- Because of this, it is not recommended to write to the same DataSet from both real-time 
  and MapReduce programs

- It is better to use different DataSets, or at least ensure that the real-time processing 
  writes to a disjoint set of columns

**Important:** MapReduce framework will reattempt a task (Mapper or Reducer) if it fails

If the task is writing to a DataSet, the reattempt of the task will most likely 
repeat the writes that were already performed in the failed attempt

Highly advisable that all writes performed by MapReduce programs be idempotent

----

Module Summary
==============

You should be able to explain:

- The need for transactions
- OCC: Optimistic Concurrency Control
- Rules for transactions
- Disabling transactions: when and where

----

Module Completed
================

`Chapter Index <return.html#m20>`__