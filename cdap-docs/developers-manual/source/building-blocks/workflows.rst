.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _workflows:

=========
Workflows
=========

*Workflows* are used to automate the execution of a series of :ref:`MapReduce
<mapreduce>`, :ref:`Spark <spark>` or :ref:`custom actions <workflow-custom-actions>`. It
allows for both sequential and :ref:`parallel execution <workflow_parallel>` of programs.

The workflow system allows specifying, executing, scheduling, and monitoring complex
series of jobs and tasks in CDAP. The system can manage thousands of workflows and
maintain millions of historic workflow logs. 

Overview
========

A *Workflow* is given a sequence of programs that follow each other, with an optional
schedule to run the workflow periodically. Upon successful execution of a program, the
control is transferred to the next program in the sequence until the last program in the
sequence is executed. Upon failure, the execution is stopped at the failed program and no
subsequent programs in the sequence are executed.

To execute MapReduce or Spark programs in a workflow, you will need to add them in your
application along with the workflow. You can optionally add a :ref:`schedule <schedules>` 
(such as a `crontab schedule 
<../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#scheduleWorkflow(java.lang.String,%20java.lang.String,%20java.lang.String)>`__)
to the workflow::

  public void configure() {
    ...
    addMapReduce(new MyMapReduce());
    addMapReduce(new AnotherMapReduce());
    addSpark(new MySpark());
    addWorkflow(new MyWorkflow());
    scheduleWorkflow(Schedules.createTimeSchedule("FiveHourSchedule", 
                                                  "Schedule running every 5 hours", 
                                                  "0 */5 * * *"),
                     "MyWorkflow");
    ...
  }

You'll then extend the ``AbstractWorkflow`` class and implement the ``configure()``
method. Inside ``configure``, you can add multiple MapReduce, Spark programs or custom
actions to the workflow. The programs will be executed in the order they are specified in
the ``configure`` method::

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

In this example, the ``MyWorkflow`` will be executed every 5 hours. During each execution
of the workflow, the ``MyMapReduce``, ``MySpark``, and ``AnotherMapReduce`` programs and
the ``MyAction`` custom action will be executed in order.

.. _workflow-custom-actions:

Workflow Custom Action
----------------------
In addition to MapReduce and Spark programs, workflow can also execute custom actions.
Custom actions are implemented in Java and can perform tasks such as sending an email. To
define a custom action, you will need to extend the ``AbstractWorkflowAction`` and
implement the ``run()`` method::

  public static class MyAction extends AbstractWorkflowAction {

    @Override
    public void run() {
      // your code goes here
    }
  }

The custom action then can be added to the workflow using the ``addAction()`` method as
shown above.

Concurrent Workflows
--------------------
By default, a workflow runs sequentially. Multiple instances of a workflow can be run
concurrently. To enable concurrent runs for a workflow, set its runtime argument
``concurrent.runs.enabled`` to ``true``.


.. _workflow_token:

Workflow Tokens
===============

In addition to passing the control flow from one node to the next, a **workflow token** is
passed, available to each of the programs in the workflow. This allows programs to:

- pass custom data (such as metrics, a status, or an error code) from one program in the 
  workflow to the next subsequent programs; 
- query and set the data in the token;
- fetch the data from the token which was set by a specific node; and
- determine the name of the node which most recently set the token value for a specific key;
  e.g., the node who last set the ERROR flag in the token.
  
The last example is intended to allow appropriate action to be taken in response, such as
logging, modifying the conditional execution, or terminating the execution of workflow.

Execution in the workflow can be made conditional, based on the information contained in
the token. Execution can be terminated if a node in the workflow produces unexpected
results.

Once a run is completed, you can query the tokens from past workflow runs for analyses that
determine which node was executed more frequently and when. You can retrieve the token values
that were added by a specific node in the workflow to debug the flow of execution.

Scope

Two scopes |---| *System* and *User* |---| are provided for workflow keys. CDAP adds keys
(such as MapReduce counters) under the *System* scope. User programs add their keys under
the *User* scope.


Putting and Getting Token Values

When a value is put into a token, it is stored under a specific key. Both keys and their
corresponding values must be non-null. The token stores additional information about the 
context in which the key is being set, such as the unique name of the workflow node. 





.. _workflow_parallel:

Parallelizing Workflow Execution
================================

The control flow of a workflow can be described as a directed, acyclic graph (DAG) of actions.
To be more precise, we require that it be a series-parallel graph. This is a graph with a
single start node and a single finish node. In between, execution can fork into concurrent
branches, but the graph may not have cycles. Every action can be a batch job or a custom
action (implemented in Java; for example, making a RESTful call to an external system).

For example, a simple control flow could be computing user and product profiles from
purchase events. After the start, a batch job could start that joins the events with the
product catalog. After that, execution could continue with a fork, and with two batch jobs
running in parallel: one computing product profiles; while the other computes user
profiles. When they are both done, execution is joined and continues with a custom action
to upload the computed profiles to a serving system, after which the control flow
terminates:

.. image:: /_images/parallelized-workflow.png
   :width: 8in
   :align: center

Forks and Joins
---------------

To create such a workflow, you provide a series of *forks* and *joins* in your workflow
specification, following these rules:

- Where your control flow initially splits, you place a ``fork`` method. 
- Every time your control flow splits, you add additional ``fork`` methods. 
- Every point where you have either a program or an action, you add a ``addMapReduce``,
  ``addSpark``, or ``addAction`` method. 
- To show each fork, use a ``also`` method to separate the different branches of the
  control flow. 
- Where your control flow reconnects, you add a ``join`` method to indicate. 
- The control flow always concludes with a ``join`` method.

The application shown above could be coded (assuming the other classes referred to exist) as::

  public class ParallelizedWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("ParallelizedWorkflow");
      setDescription("Demonstration of parallelizing execution of a workflow");
      
      addMapReduce("JoinWithCatalogMR");
    
      fork()
        .addMapReduce("BuildProductProfileMR")
      .also()
        .addMapReduce("BuildUserProfileMR")
      .join();
      
      addAction(new UploadProfilesCA());
    }
  }

Provided that the control flow does not have cycles or the joining of any branches that do
not originate from the same fork, flows of different complexity can be created using these
rules and methods.

More complicated structures can be created using ``fork``. To add another MapReduce
that runs in parallel to the entire process described above, you could use code such as::

  public class ComplexParallelizedWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("ComplexParallelizedWorkflow");
      setDescription("Demonstration of parallelized execution using a complex fork in a workflow");

      fork()
        .addMapReduce("JoinWithCatalogMR")
        .fork()
          .addMapReduce("BuildProductProfileMR")
        .also()
          .addMapReduce("BuildUserProfileMR")
        .join()
          .addAction(new UploadProfilesCA())
      .also()
        .addMapReduce("LogMonitoringMR")
      .join();
    }
  }

The diagram for this code would be:

.. image:: /_images/complex-parallelized-workflow.png
   :width: 8in
   :align: center

Conditional Node
----------------

You can provide a *conditional* node in your structure that allows for branching based on 
a boolean predicate.

Taking our first example and modifying it, you could use code such as::

  public class ConditionalWorkflow extends AbstractWorkflow {

    @Override
    public void configure() {
      setName("ConditionalWorkflow");
      setDescription("Demonstration of conditional execution of a workflow");
      
      addMapReduce("JoinWithCatalogMR");
      
      condition(new MyPredicate())
        .addMapReduce("BuildProductProfileMR")
      .otherwise()
        .addMapReduce("BuildUserProfileMR")
      .end();
      
      addAction(new UploadProfilesCA());
    }
  }

where ``MyPredicate`` is a public class which implements the ``Predicate`` interface as::

  public static class MyPredicate implements Predicate<WorkflowContext> {

     @Override
      public boolean apply(@Nullable WorkflowContext input) {
         if (input == null) {
            return false;
         }
         Value tokenValue = input.getToken().get("BuildProductProfile");
         if (tokenValue == null) {
            return false;
         }
         if (tokenValue.getAsInt() > 0) {
           return true;
         }
         return false;
     }
  }

In the ``JoinWithCatalogMR`` MapReduce, it could have in its Mapper class code that 
governs which condition to follow::

  public static final class JoinWithCatalogMR extends AbstractMapReduce {

    @Override
    public void configure() {
      setName("JoinWithCatalogMR");
      setDescription("MapReduce program to demonstrate a Conditional workflow");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(MyVerifier.class);
      String inputPath = context.getRuntimeArguments().get("inputPath");
      String outputPath = context.getRuntimeArguments().get("outputPath");
      FileInputFormat.addInputPath(job, new Path(inputPath));
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }
  }

  public static class MyVerifier extends Mapper<LongWritable, Text, Text, NullWritable> {
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      if (value != null and value.toString().equals("BuildProductProfile")) {
        context.getCounter("MyCustomCounters", "BuildProductProfile").setValue(1L);
      } else {
        context.getCounter("MyCustomCounters", "BuildProductProfile").setValue(0);
      }
    }
  }

In this case, if the predicate finds that the a ``MapReduceCounter`` *BuildProductProfile*
is greater than zero, the logic will follow the path of *BuildProductProfileMR*;
otherwise, the other path will be taken. The diagram for this code would be:

.. image:: /_images/conditional-workflow.png
   :width: 8in
   :align: center

In addition to using counters of MapReduce programs, you can use :ref:`workflow tokens
<workflow_token>` and base the logic on data stored in a token.


Example of Using a Workflow
===========================

- For an example of the use of **a workflow,** see the :ref:`Purchase
  <examples-purchase>` example.
