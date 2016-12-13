.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.

=================
Program Lifecycle
=================

.. rubric:: Data Abstractions

Every program in CDAP goes through a *program lifecycle*, with a specific set of methods called in succession 
at different points of the program lifecycle.

Though there are slight differences between some program types, in general all programs follow these steps.

In order to be a CDAP program, a program must implement the ``ProgramLifecycle<T extends RuntimeContext>`` interface.
It requires the implementation of these methods:

< insert methods? >

These program types:

< insert program types? >


.. rubric:: Deployment

At time of deployment, a program's ``configure()`` method is called. 
It is called once, and produces a program specification that immutable::

  public class PurchaseHistoryBuilder extends AbstractMapReduce {
    @Override
    public void configure() {
      setName(...);
      setDescription(...);
      ...
    }


A good practice is to add properties to the specification that you want to access or preserve
for runtime::

  public class PurchaseHistoryBuilder extends AbstractMapReduce {
    @Override
    public void configure() {
      setName(...);
      setDescription(...);
      ...
    }

.. rubric:: Runtime

At runtime, these methods are called:

- ``initialize()``
- ``run()``
- ``destroy()``

When the program lifecycle instance is initialized, the method ``initialize()`` is called once.






.. rubric:: Data and Applications Combined
