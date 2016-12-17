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
(flowlets and service handlers).

**When an application is deployed,** the application is configured, followed by the
configuring of all programs and sub-programs of the application, recursively. The
configuration happens by calling the ``configure()`` method of each entity, which creates
a specification for that entity. These specifications are bundled together to create the
application.

This results in application, program, and sub-program specifications. In a specification,
you can set the name, description, resources, programs and sub-programs used, plugins used
and required. (An example of the latter is a requirement for a JDBC driver.)

Any member variables created in the ``configure()`` methods are available only at
deployment. No dataset operations are possible, as there is no access to datasets or
transactions.

**When an application is run,** each program of an application (and any sub-programs) is
executed by calling first ``initialize()`` and (eventually) ``destroy()`` for each program
and sub-program. In these methods, dataset operations and transactions are allowed.

The following of the program lifecycle is a combination of CDAP's conventions and the
implementation of specific interfaces. They can be summarized as:

- At deployment time:

  - ``configure()`` By convention, all CDAP applications and programs have this method.
    It produces an immutable program specification.
    
- At runtime:

  - ``initialize()`` A requirement of the ``ProgramLifecycle`` interface.
  - ``destroy()`` A requirement of the ``ProgramLifecycle`` interface.

The ``initialize()`` method is called once, at the start of the program. The ``destroy()``
method is called once, at the end of program before it is shutdown. If there is any
cleanup required, it can be implemented in this method.

Flows and services do not have an ``initialize()`` because they have a sub-program (flowlets
for a flow, service handlers for a service) which has an ``initialize()`` method instead.

Note that the instance of the object called at deployment **is not the same** instance of
the object called at runtime. Because the result of the deployment stage is an immutable
program specification, any local member variables set during deployment will not be
available during runtime. This behavior can cause unexpected null-pointer exceptions. The
solution is instead to set these as properties in the specification, which is
available at runtime, as in the examples on :ref:`initializing instance fields
<best-practices-initializing>`. For example, in a flowlet::

  getContext().getSpecification().getProperties()


Transactions
============
The relationship between transactions and lifecycle depends on the method involved:

- ``configure()`` No transactions

- ``initialize()`` Inside a transaction

- ``destroy()`` Inside a transaction

The exception to this are :ref:`Workers <workers-datasets>`, which have to execute their
own, explicit transactions. See :ref:`workers and datasets <workers-datasets>` for details.

Details on transactions in these methods are covered in the section on :ref:`using the
transaction system in programs <transaction-system-using-in-programs>`.

Program Types
=============
For convenience, most program types have a corresponding abstract program class. It is
recommended to always extend the abstract class instead of implementing the program interface.
The abstract classes provide:

- proxy methods for the program configurer's methods;
- an ``initialize()`` method that stores the program context in a class member and makes
  it available via ``getContext()``; and
- a ``destroy()`` method that does nothing.

This table summarizes, for each program or sub-program type, the methods available,
abstract class, and their signatures:

.. list-table::
   :widths: 20 20 30 30
   :header-rows: 1

   * - Program Type
     - Sub-program Type
     - Abstract Class
     - Runtime Methods
   * - Flow
     -
     - ``AbstractFlow``
     - | ``configure()``
       | ``destroy()``
       |
       | *Note: no* ``initialize()``
   * - 
     - Flowlet
     - ``AbstractFlowlet``
     - | ``configure()``
       | ``initialize()``
       |
       | *If annotated or* ``process()``:
       | ``@ProcessInput``
       | ``process()``
       |
       | *If annotated or* ``generate()``:
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
       | *Note: no* ``initialize()``
   * - 
     - ServiceHandler
     - ``AbstractHttpServiceHandler``
     - | ``configure()``
       | ``initialize()``
       | ``destroy()``
       |  
       | ``@GET`` *or*
       | ``@PUT`` *or*
       | ``@POST`` *or*
       | ``@DELETE``
       | ``@Path{"handlerPath"}``
       | ``handlerMethod()``
       |  
       | *Note: classes extending* ``AbstractHttpServiceHandler`` *are only required to implement* ``configure()``
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
     - | ``configure()``
       | ``initialize()``
       | ``run()``
       | ``destroy()``
