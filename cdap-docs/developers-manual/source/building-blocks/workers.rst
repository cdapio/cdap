.. meta::
:author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _workers:

============================================
Workers
============================================

Workers can be used to run background processes. Workers provide an ability to write data processing
logic that doesn't fit well in Flows or MapReduce etc.

You can add workers to your application by calling the ``addWorker`` method in the Application's
``configure`` method::

  public class AnalyticsApp extends AbstractApplication {
    @Override
    public void configure() {
      setName("AnalyticsApp");
      ...
      addWorker(new ProcessWorker());
      ...
    }
  }

Workers have semantics similar to a Java thread and are run as one when CDAP is run in In-Memory
or Standalone Mode. In distributed mode, each instance of worker run in its own YARN container.
Their instances may be updated via the CDAP Console or the REST APIs.

  public class ProcessWorker extends AbstractWorker {

    @Override
    public void initialize(WorkerContext context) {
      ...
    }

    @Override
    public void run() {
      ...
    }

    @Override
    public void stop() {
      ...
    }

    @Override
    public void destroy() {
      ...
    }
  }

Workers can access and use ``Dataset``\s via a ``DatasetContext`` inside their ``run`` method.

  @Override
  public void run() {
    getContext().execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) {
        Dataset dataset = context.getDataset("tableName");
        ...
      }
    });
  }

Operations executed on ``Dataset``\s within a ``run`` are committed as part of a single transaction.
The transaction is started before ``run`` is invoked and is committed upon successful execution. Exceptions
thrown while committing the transaction or thrown by user-code result in a rollback of the transaction.
It is recommended that ``TransactionConflictException`` be caught and handled appropriately, for example
you can retry the ``Dataset`` operation.
