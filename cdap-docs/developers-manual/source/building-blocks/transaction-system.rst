.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _transaction-system:

============================================
Transaction System
============================================

The Need for Transactions
-------------------------

A Flowlet processes the data objects received on its inputs one at a time. While processing
a single input object, all operations, including the removal of the data from the input,
and emission of data to the outputs, are executed in a **transaction**. This provides us
with ACID—atomicity, consistency, isolation, and durability properties:

- The process method runs under read isolation to ensure that it does not see dirty writes
  (uncommitted writes from concurrent processing) in any of its reads.
  It does see, however, its own writes.

- A failed attempt to process an input object leaves the data in a consistent state;
  it does not leave partial writes behind.

- All writes and emission of data are committed atomically;
  either all of them or none of them are persisted.

- After processing completes successfully, all its writes are persisted in a durable way.

In case of failure, the state of the data is unchanged and processing of the input
object can be reattempted. This ensures "exactly-once" processing of each object.

OCC: Optimistic Concurrency Control
-----------------------------------

The Cask Data Application Platform uses `Cask's Tephra, <http://tephra.io>`__ which uses
*Optimistic Concurrency Control* (OCC) to implement transactions. Unlike most relational
databases that use locks to prevent conflicting operations between transactions, under OCC
we allow these conflicting writes to happen. When the transaction is committed, we can
detect whether it has any conflicts: namely, if during the lifetime of the transaction,
another transaction committed a write for one of the same keys that the transaction has
written. In that case, the transaction is aborted and all of its writes are rolled back.

In other words: If two overlapping transactions modify the same row, then the transaction
that commits first will succeed, but the transaction that commits last is rolled back due
to a write conflict.

Optimistic Concurrency Control is lockless and therefore avoids problems such as idle
processes waiting for locks, or even worse, deadlocks. However, it comes at the cost of
rollback in case of write conflicts. We can only achieve high throughput with OCC if the
number of conflicts is small. It is therefore good practice to reduce the probability of
conflicts wherever possible.

Here are some rules to follow for Flows, Flowlets, Services, and Procedures:

- Keep transactions short. The Cask Data Application Platform attempts to delay the beginning of each
  transaction as long as possible. For instance, if your Flowlet only performs write
  operations, but no read operations, then all writes are deferred until the process
  method returns. They are then performed and transacted, together with the
  removal of the processed object from the input, in a single batch execution.
  This minimizes the duration of the transaction.

- However, if your Flowlet performs a read, then the transaction must
  begin at the time of the read. If your Flowlet performs long-running
  computations after that read, then the transaction runs longer, too,
  and the risk of conflicts increases. It is therefore good practice
  to perform reads as late in the process method as possible.

- There are two ways to perform an increment: As a write operation that
  returns nothing, or as a read-write operation that returns the incremented
  value. If you perform the read-write operation, then that forces the
  transaction to begin, and the chance of conflict increases. Unless you
  depend on that return value, you should always perform an increment
  only as a write operation.

- Use hash-based partitioning for the inputs of highly concurrent Flowlets
  that perform writes. This helps reduce concurrent writes to the same
  key from different instances of the Flowlet.

Keeping these guidelines in mind will help you write more efficient and faster-performing
code.

.. _transaction-system-conflict-detection:

Levels of Conflict Detection
----------------------------

Transactions providing ACID (atomicity, consistency, isolation, and durability) guarantees
are useful in several applications where data accuracy is critical—examples include billing
applications and computing click-through rates.

However, transaction are not for free: the transaction system must track all the writes
made by all transactions, and it must check transactions for conflicts before committing them.
If conflicts are frequent, they will impact performance because the failed transactions
have to be rolled back and reattempted.

In some scenarios, you may want to fine-tune the manner in which a dataset participates in
transactions:

- Some applications—such as trending—might not need transactions for all writes, because
  small inaccuracies have little effect on trends with great momentum. Applications that
  do not strictly require accuracy can trade it for increased throughput by disabling
  transactions for some datasets.
- Some applications perform concurrent updates to the same row of a table, but typically
  those updates do not strictly conflict with each other because they are on different
  columns of the row. In this case it can make sense to increase the precision of conflict
  detection by tracking changes at the column level instead of the row level.

Both of these can be achieved by specifying a conflict detection level when the table is
created. For example, in your application's ``configure()`` method::

    Tables.createTable(getConfigurer(), "myTable", ConflictDetection.COLUMN);

You have these options:

- ``ConflictDetection.NONE`` to disable transactions for the table. None of the writes
  performed on this table will participate in conflict detection. However, all writes
  will still be rolled back in case of a transaction failure (the transaction may fail
  for other reasons than a conflict on this table).
- ``ConflictDetection.ROW`` to track writes at the row level. This means that two
  concurrent transactions will cause a conflict if they write to the same row of the table,
  even if the writes are for different columns. This is the default.
- ``ConflictDetection.COLUMN`` to increase the precision of the conflict detection to
  the column level: Two concurrent transactions can write to the same row without conflict,
  as long as they write to disjoint sets of columns. This will increase the overhead for
  each transaction, because the transaction system must track writes with greater detail.
  But it can also greatly reduce the number of transaction conflicts, leading to improved
  overall application throughput.

  See the :ref:`UserProfile example <examples-user-profiles>`
  for a sample use case.

Transactions in MapReduce
-------------------------
When you run a MapReduce that interacts with Datasets, the system creates a
long-running transaction. Similar to the transaction of a Flowlet or a Procedure, here are
some rules to follow:

- Reads can only see the writes of other transactions that were committed
  at the time the long-running transaction was started.

- All writes of the long-running transaction are committed atomically,
  and only become visible to others after they are committed.

- The long-running transaction can read its own writes.

However, there is a key difference: long-running transactions do not participate in
conflict detection. If another transaction overlaps with the long-running transaction and
writes to the same row, it will not cause a conflict but simply overwrite it.

It is not efficient to fail the long-running job based on a single conflict. Because of
this, it is not recommended to write to the same Dataset from both real-time and MapReduce
programs. It is better to use different Datasets, or at least ensure that the real-time
processing writes to a disjoint set of columns.

It's important to note that the MapReduce framework will reattempt a task (Mapper or
Reducer) if it fails. If the task is writing to a Dataset, the reattempt of the task will
most likely repeat the writes that were already performed in the failed attempt. Therefore
it is highly advisable that all writes performed by MapReduce programs be idempotent.
