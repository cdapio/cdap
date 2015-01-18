.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _workflows:

============================================
Workflows
============================================

**Workflows** are used to execute a series of :ref:`MapReduce <mapreduce>` or :ref:`Spark <spark>`.

A Workflow is given a sequence of jobs that follow each other, with an optional schedule
to run the Workflow periodically. Upon successful execution of a job, the control is
transferred to the next job in sequence until the last job in the sequence is executed. Upon
failure, the execution is stopped at the failed job and no subsequent jobs in the sequence
are executed.

To process MapReduce or Spark programs in sequence, specify
``addWorkflow()`` in your application::

  public void configure() {
    ...
    addWorkflow(new PurchaseHistoryWorkflow());

You'll then implement the ``Workflow`` interface, which requires the
``configure()`` method. From within ``configure``, call the
``addSchedule()`` method to run a WorkFlow job periodically::

  public static class PurchaseHistoryWorkflow implements Workflow {

    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with()
        .setName("PurchaseHistoryWorkflow")
        .setDescription("PurchaseHistoryWorkflow description")
        .startWith(new PurchaseHistoryBuilder())
        .last(new PurchaseTrendBuilder())
        .addSchedule(new DefaultSchedule("FiveMinuteSchedule", "Run every 5 minutes",
                     "0/5 * * * *", Schedule.Action.START))
        .build();
    }
  }

If there is only one MapReduce or Spark program to be run as a part of a WorkFlow,
use the ``onlyWith()`` method after ``setDescription()`` when building
the Workflow::

  public static class PurchaseHistoryWorkflow implements Workflow {

    @Override
    public WorkflowSpecification configure() {
      return WorkflowSpecification.Builder.with() .setName("PurchaseHistoryWorkflow")
        .setDescription("PurchaseHistoryWorkflow description")
        .onlyWith(new PurchaseHistoryBuilder())
        .addSchedule(new DefaultSchedule("FiveMinuteSchedule", "Run every 5 minutes",
                     "0/5 * * * *", Schedule.Action.START))
        .build();
    }
  }

.. rubric::  Example of Using a Workflow

- For an example of use of **a Workflow,** see the :ref:`Purchase
  <examples-purchase>` example.
