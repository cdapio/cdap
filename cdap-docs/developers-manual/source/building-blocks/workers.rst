.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _workers:

============================================
Workers
============================================

Workers can be used to run background processes. Workers provide the ability to write data processing logic
that doesn't fit into the other paradigms such as Flows and MapReduce or realtime and batch.

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

Workers have semantics similar to a Java thread and are run in a thread when CDAP is run in either In-Memory
or Standalone Modes. In distributed mode, each instance of a worker runs in its own YARN container.
Their instances may be updated via the :ref:`Command Line Interface <cli-available-commands>` or a :ref:`RESTful API <http-restful-api-lifecycle>`::

  public class ProcessWorker extends AbstractWorker {

    @Override
    public void initialize(WorkerContext context) {
      super.initialize(context);
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

Workers can access and use ``Dataset``\s via a ``DatasetContext`` inside their ``run`` method::

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
It is recommended that ``TransactionConflictException`` be caught and handled appropriately; for example
you can retry a ``Dataset`` operation.

Services can be discovered inside Workers. Service can either belong to the same application or another
application in the same namespace. WorkerContext can be used to discover the URL of the Service of interest::

  @Override
  public void run() {
    URL url = getContext().getServiceURL("myService");
    //To discover Service in another application: getContext().getServiceURL("anotherAppName", "serviceId");
  }

