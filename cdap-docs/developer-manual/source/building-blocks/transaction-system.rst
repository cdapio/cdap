.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _transaction-system:

==================
Transaction System
==================

The Need for Transactions
=========================
A flowlet processes the data objects received on its inputs one at a time. While processing
a single input object, all operations, including the removal of the data from the input,
and emission of data to the outputs, are executed in a *transaction*. This provides us
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
===================================
The Cask Data Application Platform uses `Apache Tephra™ <http://tephra.incubator.apache.org>`__, 
which uses *Optimistic Concurrency Control* (OCC) to implement transactions. Unlike most relational
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

Here are some rules to follow for flows, flowlets, and services:

- Keep transactions short. The Cask Data Application Platform attempts to delay the beginning of each
  transaction as long as possible. For instance, if your flowlet only performs write
  operations, but no read operations, then all writes are deferred until the process
  method returns. They are then performed and transacted, together with the
  removal of the processed object from the input, in a single batch execution.
  This minimizes the duration of the transaction.

- However, if your flowlet performs a read, then the transaction must
  begin at the time of the read. If your flowlet performs long-running
  computations after that read, then the transaction runs longer, too,
  and the risk of conflicts increases. It is therefore good practice
  to perform reads as late in the process method as possible.

- There are two ways to perform an increment: As a write operation that
  returns nothing, or as a read-write operation that returns the incremented
  value. If you perform the read-write operation, then that forces the
  transaction to begin, and the chance of conflict increases. Unless you
  depend on that return value, you should always perform an increment
  only as a write operation.

- Use hash-based partitioning for the inputs of highly concurrent flowlets
  that perform writes. This helps reduce concurrent writes to the same
  key from different instances of the flowlet.

Keeping these guidelines in mind will help you write more efficient and faster-performing
code.


.. _transaction-system-using-in-programs:

Using Transactions in Programs
==============================
CDAP provides transactional capabilities to help ensure consistency of data under highly
concurrent workloads. To make transactions easy to use, CDAP will often implicitly execute
application code inside a transaction |---| and retry the execution if the transaction fails
due to write conflicts. 

For example, to guarantee exactly-once processing semantics for flows, the process method
of a flowlet is always run inside a transaction. This transaction encapsulates the removal
of data from an input queue, all data operations performed in the course of processing
this data, and the emitting of data to its output queues for downstream flowlets. All of
these must be together in the same transaction and committed atomically: otherwise,
exactly-once processing cannot be ensured.

For other types of programs, transactions can also be useful. For example, the handler
methods of services are executed transactionally to make sure they operate on consistent
data. The lifecycle methods (``initialize()`` and ``destroy()``) of all programs are also executed
within an implicit transaction.

However, there are use cases where that transaction is not desired:

- The default transaction timeout (as :ref:`configured by
  <appendix-cdap-default-datasets>` ``data.tx.timeout`` in ``cdap-site.xml``) may be too
  short for the operations performed by a method. For example, the ``destroy()`` method of a
  MapReduce program may have to clean up temporary data, or make a web service call to
  notify some other party of the job completion.

- A method does not perform any transactional operations. For example, FileSet datasets do
  not require a transaction |---| a method using only FileSets therefore does not require an 
  implicit transaction.

- A method performs many operations and wishes to execute them in several short
  transactions rather than a single long transaction. A good example of such a method is the
  ``run()`` method of a worker, which runs perpetually and cannot be executed inside a single
  transaction. Instead, it needs to start an explicit transaction whenever it performs
  operations on transactional datasets. 

To facilitate these use cases, CDAP offers programs control over the execution of
transactions:

- Annotate a method with an ``@TransactionPolicy`` to turn off the implicit transaction
  started by CDAP.

- Use the program context’s ``execute()`` method to run a block of code inside an explicit
  transaction.

- Control the timeout of transactions by setting a system-wide configuration
  (``data.tx.timeout``); by setting a preference for an individual namespace, application, or
  program; or by passing a timeout for the transaction to the ``execute()`` method.

Implicit versus Explicit Transactions
-------------------------------------
By default, CDAP will start an implicit transaction for these methods:

- All flowlet process methods
- All service handler methods
- The ``ProgramLifecycle`` methods (``initialize()`` and ``destroy()``) for all types of
  programs and sub-programs (flowlets, service handlers, and workflow actions), with the
  exception of worker programs.

For example, as shown in the :ref:`HelloWorld example <examples-hello-world>`, the
``GreetingHandler`` uses the *whom* ``KeyValueTable``. CDAP implicitly starts a
transaction for this handler method, and the handler can rely on the transactional
consistency of the data it reads from the dataset:

.. literalinclude:: /../../../cdap-examples/HelloWorld/src/main/java/co/cask/cdap/examples/helloworld/HelloWorld.java
   :language: java
   :lines: 118-135
   :dedent: 2

For flowlet process methods, this starting of implicit transactions cannot be disabled,
because that would impact the semantics of flow execution.

For MapReduce programs, the lifecycle methods of MapReduce tasks (mappers and reducers)
and MapReduce helpers (such as partitioners and comparators) are always run inside a
transaction: the long-running transaction that encapsulates an entire MapReduce job (see
:ref:`mapreduce-transactions`).

For Spark programs, see :ref:`Transactions and Spark <spark-transactions>` for using
transactions in Spark programs.

For all other lifecycle methods and for service handlers, the implicit transaction can be
turned off by annotating the method with ``@TransactionPolicy(TransactionControl.EXPLICIT)``.

For example, in the ``FileSetService`` of the :ref:`FileSetExample <examples-fileset>`:

.. literalinclude:: /../../../cdap-examples/FileSetExample/src/main/java/co/cask/cdap/examples/fileset/FileSetService.java
   :language: java
   :lines: 79-83
   :dedent: 4
   :append: . . .

This service handler method only accesses FileSets, which do not require transactions.
Therefore, we can safely turn off the implicit transaction for this method. 

Note that you can access any dataset through the program context’s ``getDataset()`` method.
However, if you attempt to perform an operation on a transactional dataset (such as a
Table) without a transaction, that operation will fail with an exception.

For the lifecycle methods of a worker, CDAP does not (by default) start an implicit
transaction. In a similar fashion as above, that can be changed by annotating the
lifecycle method ``initialize()``::

  @Override
  @TransactionPolicy(TransactionControl.IMPLICIT)
  public void initialize(WorkerContext context) throws Exception {
    ...

This method will now run inside an implicit transaction. 

Note that you cannot annotate the ``run()`` method of a worker of a custom workflow action
with implicit transaction control; they are always executed without an implicit
transaction and must start transactions explicitly when needed. This is described in the
next section.

Explicit Transactions 
----------------------
Every program context (except for the ``FlowletContext`` and the ``MapReduceTaskContext``)
allows the executing of a block of code in an explicit transaction. 

For example, this service handler method (from the ``UploadService`` of the
:ref:`SportResultsExample <examples-sport-results>`) uses an explicit transaction to
access the partition metadata, whereas the streaming of the file contents to the client is
performed outside the transaction:

.. literalinclude:: /../../../cdap-examples/SportResults/src/main/java/co/cask/cdap/examples/sportresults/UploadService.java
   :language: java
   :lines: 74-103
   :dedent: 4

Be aware that you cannot nest transactions. For example, either:

- calling ``execute()`` from a method that already runs inside an implicit transaction; or
- calling ``execute()`` from the ``run()`` method of a ``TxRunnable``

would fail with an exception. 

Controlling the Transaction Timeout
-----------------------------------
By default, all transactions are executed with the same transaction timeout. This timeout
is :ref:`configured site-wide <appendix-cdap-default-datasets>` as ``data.tx.timeout``
(default value 30 seconds) in ``cdap-site.xml``. You can change it to a higher number of
seconds if your transactions typically require a longer timeout. 

To control the transaction timeout for individual namespaces, applications, or programs,
you can :ref:`set a preference <preferences>` for the namespace, application, or program.
The name of the preference is ``system.data.tx.timeout``. 

To configure the timeout for a sub-program (a flowlet or a custom workflow action), prefix
the preference name with ``flowlet.<name>`` or ``action.<name>``. For example, setting
``flowlet.aggregator.system.data.tx.timeout`` to 60 seconds will only affect the flowlet
named *aggregator* but not the other flowlets of the flow. 

To control the transaction timeout for an individual run of a program, you can also
provide this setting as a runtime argument when starting the program. Note that this will
:ref:`prevail over a preference <preferences-order-of>` configured for the namespace, 
application, or program.

Finally, for explicit transactions, you can control the transaction timeout by passing in
the timeout in seconds to the ``execute()`` method::

  getContext().execute(90, new TxRunnable() {
    @Override
    public void run(DatasetContext context) throws Exception {
      ...
    }
  });

This will execute the ``TxRunnable`` in a transaction with a timeout of 90 seconds.


.. _transaction-system-conflict-detection:

Levels of Conflict Detection
============================
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


Transaction Examples
====================

- For an example of using **implicit transactions,** see the :ref:`HelloWorld example
  <examples-hello-world>` and its ``Greeting`` service and ``GreetingHandler``, which uses
  implicit transactions.

- For an example of using **explicit transactions,** see the :ref:`FileSet example
  <examples-fileset>` and its ``FileSetService``, whose ``FileSetHandler`` uses explicit
  transactions for all of its operations.

- For another example of using **explicit transactions,** see the 
  :ref:`Sport Results example <examples-sport-results>`, where the service
  ``UploadService`` has an ``UploadHandler`` that reads and writes using explicit
  transactions.
