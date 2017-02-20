.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.

.. _program-retry-policies:

.. highlight:: java

======================
Program Retry Policies
======================

Like many distributed systems, CDAP needs to deal with node failures across the cluster.
If any of the CDAP system services that are running in your cluster are unavailable for any
reason, CDAP programs can be configured to retry API calls that rely on these services.
For example, if the leader transaction service that was running on a node has suddendly died,
programs can retry their transactional calls until the follower transaction service comes online.

Retry policies discussed here pertain to retrying individual API calls.
If a program run has failed, it will not be retried. 


.. _program-retry-policies-operations:

Retryable Operations
====================
In general, developers can assume that CDAP will attempt to retry every operation that 
can be safely retried. As a rule of thumb, any operation that is idempotent can be retried.
Some non-idempotent operations can also be retried, depending on the type of failure.
For example, developers can use the ``Admin`` interface to create a new dataset.
This is not an idempotent operation, as attempting to create a dataset that already
exists will result in a failure. However, the call to create a dataset will be retried
by CDAP if it failed because the program could not discover or connect to the CDAP
dataset service that actually creates the dataset.

Methods in the ``Admin``, ``Transactional``, ``StreamWriter``, and ``DatasetContext`` interface
will be retried by CDAP if they fail due to unavailability of a required CDAP system service. 
For example, consider a ``Worker`` that uses ``Transactional`` to write to a ``Table``:: 

  @Override
  public void run() {
    ...
    getContext().execute(new TxRunnable() {
      @Override
      public void run(DatasetContext datasetContext) throws Exception {
        Table table = datasetContext.getDataset("mytable");
        Put put = new Put(row);
        put.add(col, val);
        table.write(row, put);
      }
    });
  }

The developer does not need to worry about what will happen when this code is run and the CDAP
transaction service is unavailable. The program will retry the relevant transaction calls until
the configured timeout is reached or the transaction service recovers.

.. _program-retry-policies-config:

Configuring Default Retry Policy 
================================
Each program type is configured with its own default retry policy. For example, a MapReduce program has
a different default retry policy than a Service. The default retry policy for each program type
can be set by specifying these properties in the ``cdap-site.xml`` file: 

  - ``<program-type>.retry.policy.type``
  - ``<program-type>.retry.policy.max.time.secs``
  - ``<program-type>.retry.policy.max.retries``
  - ``<program-type>.retry.policy.base.delay.ms``
  - ``<program-type>.retry.policy.max.delay.ms``

where ``<program-type>`` is one of ``custom.action``, ``flow``, ``mapreduce``,
``service``, ``spark``, ``worker``, or ``workflow``.

The **retry.policy.type** property must be set to one of ``none``, ``fixed.delay``, or ``exponential.backoff``:

- The ``none`` type means there will be no retry attempt.
- The ``fixed.delay`` type will retry the call at a fixed interval.
- The ``exponential.backoff`` type will retry at exponentially greater intervals after each failure.

The **retry.policy.max.time.secs** property specifies a limit to the total amount of time CDAP will 
wait before aborting the operation. For example, when set to 60, CDAP will abort the operation
if more than 60 seconds have passed since the original call was made.

The **retry.policy.max.retries** property specifies a limit to the number of retries CDAP will
attempt before aborting the operation. The original call does not count as a retry attempt.
For example, when set to five, CDAP will abort the operation after the sixth call has failed.

The **retry.policy.base.delay.ms** property applies to the ``fixed.delay`` and ``exponential.backoff``
retry policy types. When using ``fixed.delay``, it is the amount of time in milliseconds between
retry attempts. For example, when set to 500, CDAP will retry the operation 500 milliseconds
after the previous attempt failed. When using ``exponential.backoff``, it is the amount of time
in milliseconds between the original failure and the first retry attempt. 

The **retry.policy.max.delay.ms** property only applies to the ``exponential.backoff`` retry policy type.
It is the maximum amount of time in milliseconds between retry attempts. For example, when
``retry.policy.base.delay.ms`` is 1000 and ``retry.policy.max.delay.ms`` is 4000, CDAP will wait for
one second after the original failure to attempt the first retry. If that fails, CDAP will wait
for two seconds before attempting the second retry. If that fails, CDAP will wait for four seconds
before attempting the third retry. For every failure after that, CDAP will wait for four seconds, as
``retry.policy.max.delay.ms`` has been reached.
Once an attempt succeeds, the time between retries will be reset to the base delay.

.. _program-retry-policies-override:

Overriding Retry Policy
=======================
Retry policies can be overridden using preferences and runtime arguments. Each of the five
properties can be overriden by setting that preference or runtime argument prefixed with ``system``:

  - ``system.retry.policy.type``
  - ``system.retry.policy.max.time.secs``
  - ``system.retry.policy.max.retries``
  - ``system.retry.policy.delay.base.ms``
  - ``system.retry.policy.delay.max.ms``

In this way, you can set different retry policies for different programs. For example, if one MapReduce
program normally takes days to finish, you may want to use a longer retry policy for it than for a
MapReduce that normally takes ten minutes to finish.
