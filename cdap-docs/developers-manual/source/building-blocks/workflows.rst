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

To execute MapReduce or Spark programs in a Workflow, you will need to add them in your
application along with the Workflow. You can optionally add a schedule (such as a
`crontab schedule <../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#scheduleWorkflow(java.lang.String,%20java.lang.String,%20java.lang.String)>`__)
to the Workflow::

  public void configure() {
    ...
    addMapReduce(new MyMapReduce());
    addMapReduce(new AnotherMapReduce());
    addSpark(new MySpark());
    addWorkflow(new MyWorkflow());
    scheduleWorkflow("FiveHourSchedule", "0 */5 * * *", "MyWorkflow");
    ...
  }

You'll then extend the ``AbstractWorkflow`` class and implement the
``configure()`` method. Inside ``configure``, you can add multiple MapReduce, Spark, or Custom Action
programs to the Workflow. The programs will be executed in the order they are specified in the ``configure``
method::

  public static class MyWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
        setName("MyWorkflow");
        setDescription("MyWorkflow description");
        addMapReduce("MyMapReduce");
        addSpark("MySpark");
        addMapReduce("AnotherMapReduce");
        addAction(new MyAction());
    }
  }

In this example, the ``MyWorkflow`` will be executed every 5 hours. During each execution of the Workflow,
the ``MyMapReduce``, ``MySpark``, ``AnotherMapReduce``, and ``MyAction`` programs will be executed in order.

.. rubric::  Workflow Custom Action

In addition to MapReduce and Spark programs, Workflow can also execute custom actions. Custom actions are
implemented in Java and can perform tasks such as make a REST call to an external system. To define
custom action, you will need to extend the ``AbstractWorkflowAction`` and implement the ``run()`` method::

  public static class MyAction extends AbstractWorkflowAction {

    @Override
    public void run() {
      // your code goes here
    }
  }

The custom action then can be added to the Workflow using the ``addAction()`` method as shown above.

.. rubric::  Example of Using a Workflow

- For an example of use of **a Workflow,** see the :ref:`Purchase
  <examples-purchase>` example.
