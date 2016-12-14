.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _program_lifecycle:

=================
Program Lifecycle
=================

Every program in CDAP goes through a *program lifecycle*, with a specific set of methods
called in succession at different points of the lifecycle.

Though there are slight differences between program types, in general all programs follow
the same lifecycle. The exceptions are flows and services, as they have sub-programs
(flowlets and service handlers) that handle part of the lifecycle.

In order to be a CDAP program, a program must implement the ``ProgramLifecycle``
interface. It requires the implementation of these methods:

- At deployment time:

  - ``configure()``
  
- At runtime:

  - ``initialize()``
  - ``getConfig()``
  - ``getContext()``
  - ``destroy()``

The ``initialize()`` method is called once, at the start of the program. The
``getConfig()`` and ``getContext()`` methods can be used to retrieve the information about
the application configuration and the context the program is running in, whenever required.
The ``destroy()`` method is called once at the end of program.

Flows and services do not have ``initialize()`` because they have a sub-program (flowlets
for flows, service handlers for services) which has an ``initialize()`` method instead.

Note also that the instance of the object called at deployment **is not the same**
instance of the object called at runtime. Because the result of the deployment stage is an
immutable program specification, any local member variables set during deployment will not
be available during runtime. This behavior can cause unexpected null-pointer exceptions.
The solution is instead to set these as properties in a configuration object, which is
available at runtime, as :ref:`shown below <program_lifecycle_specification_properties>`.

Program Types
=============
This table summarizes, for each program type, the methods available and their signatures:

.. list-table::
   :widths: 20 20 30 30
   :header-rows: 1

   * - Program Type
     - Sub-program Type
     - Extends
     - Runtime Methods
   * - Flow
     -
     - ``AbstractFlow``
     - | ``configure()``
       | ``destroy()``
       |
       | Note: no ``initialize()`` method
   * - 
     - Flowlet
     - ``AbstractFlowlet``
     - | ``initialize()``
       |
       | If annotated or ``process()``:
       | ``@ProcessInput``
       | ``process()``
       |
       | If annotated or ``process()``:
       | ``@Tick``
       | ``generate()``
       |
       | ``destroy()``
  
   * - MapReduce
     - 
     - ``AbstractMapReduce``
     - | ``configure()``
       | ``initialize()``
       | ``destroy()``
   * - 
     - Mappper
     - ``Mapper``
     - | ``initialize()``
       | ``map()``
       | ``destroy()``
   * - 
     - Reducer
     - ``Reducer``
     - | ``initialize()``
       | ``reduce()``
       | ``destroy()``
   * - Service
     -
     - ``AbstractService``
     - | ``configure()``
       | ``destroy()``
       |
       | Note: no ``initialize()`` method
   * - 
     - ServiceHandler
     - ``AbstractHttpServiceHandler``
     - | ``configure()``
       | ``initialize()``
       | ``destroy()``
       |  
       | ``@GET`` or
       | ``@PUT`` or
       | ``@POST`` or
       | ``@DELETE``
       | ``@Path{"handlerPath"}``
       | ``handlerMethod()``
       |  
       | Note: classes that extend ``AbstractHttpServiceHandler`` are only required to implement ``configure()``
   * - Spark
     - 
     - ``AbstractSpark``
     - | ``configure()``
       | ``initialize()``
       | ``destroy()``
   * - 
     - SparkMain
     - ``SparkMain``
     - | ``run()``
   * - 
     - JavaSparkMain
     - ``JavaSparkMain``
     - | ``run()``
   * - Worker
     - 
     - ``AbstractWorker``
     - | ``configure()``
       | ``initialize()``
       | ``destroy()``
   * - Workflow
     - 
     - ``AbstractWorkflow``
     - | ``configure()``
       | ``initialize()``
       | ``destroy()``
   * - 
     - Custom Action
     - ``AbstractCustomAction``
     - | ``run()``


Deployment
==========
At time of deployment, a program's ``configure()`` method is called. 
It is called once, and produces an immutable program specification::

  public class PurchaseHistoryBuilder extends AbstractMapReduce {
    @Override
    public void configure() {
      setName(...);
      setDescription(...);
      ...
    }


Runtime
=======
At runtime, these methods are called:

- ``initialize()`` Initializes a program. This method will be called only once for each
  ProgramLifecycle instance.

- ``destroy()`` Destroy is the last method called before the program is shutdown. If
  there is any cleanup required, it can be specified in this method.

**Note:** In the case of a flow or service, only the configure() methods are called.
There are no initialize() or destroy() methods. Only flowlets or service handlers have those.

Transactions
============
The relationship between transactions and lifecycle depends on which method is involved:

- ``configure()`` No transactions

- ``initialize()`` Inside a transaction

- ``destroy()`` Inside a transaction

The exception to this are :ref:`Workers <workers-datasets>`, which are run inside their own transaction.
See :ref:`Workers and Datasets <workers-datasets>` for details.

To create a new transaction, use code following this pattern::

  getContext().execute(timeout, new TxRunnable() {
        void run(DataSetContext){
          . . .
        }
      }
    );


Service Handlers
----------------
For :ref:`service handlers <user-service-handlers>` and transactions, there are two
options available depending on your needs. The idea is to make it as simple as possible
for the common cases but retain flexibility for the edge cases.

If you use an implicit transaction policy, such as::

  @TransactionControl(TransactionPolicy IMPLICIT)
  public void initialize() {
    . . .
  }

a default transaction will be created for you. (This is required for Workers that need to
perform any transaction operations.)

In the case of certain datasets, such as :ref:`Filesets <datasets-fileset>`, the datasets
are not transactional, and a service handler would not need a transaction. In that case,
you should use a ``TransactionControl`` of ``Explicit``::

  @TransactionControl(TransactionPolicy EXPLICIT)


Lifecycle Tips
==============

.. _program_lifecycle_specification_properties:

Adding Properties
-----------------
A good practice is to add properties to the specification that you want to access or
preserve for runtime, by using a custom ``Config`` class::

  public class MyApp extends AbstractApplication<MyApp.MyConfig> {

    @Override
    public void configure() {
      MyConfig config = getConfig();
      . . .
      addFlow(new MyFlow(config)):
      addService(new MyService(config));
    }

    public static class MyConfig extends Config {
      . . .
    }
  }

  public class MySpark extends AbstractSpark {

    public MySpark(MyApp.MyConfig appConfig) {
      this.config = appConfig;
    }

    @Override
    protected void configure() {
      // Use config here to configure the Spark program at runtime with
      // application properties
      . . .
    }
  }

Dataset Access in MapReduce
---------------------------
Due to a limitation in the CDAP MapReduce implementation (see :cask-issue:`CDAP-6099`),
writing to a dataset does not work in a MapReduce Mapper's ``destroy()`` method.

