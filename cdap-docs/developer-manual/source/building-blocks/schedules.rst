.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

.. _schedules:

=========
Schedules
=========

*Schedules* are used to automate the execution of :ref:`workflows <workflows>` on a
recurring time basis, on data availability, on certain statuses reached by another program,
or a logical combination of the conditions mentioned previously. The schedules of an
application can be added as part of application configuration; they can also be managed
through RESTful endpoints that allow listing, modifying, deleting and creating schedules,
as well as disabling and enabling them.

A schedule must have a unique name within its application (the same name can
be used in different applications), and additionally consists of:

- the workflow to be executed, along with properties that translate into runtime arguments
  for the workflow run;
- a *Trigger*, which initiates the execution of the program by creating a Job for the
  workflow;
- a set of *Run Constraints*, which can delay or prevent the execution of the workflow;
- a timeout for the run constraints to be fulfilled; when this timeout is exceeded,
  the workflow will not execute.

To add a schedule to an application extended from ``AbstractApplication``, use the method
`schedule
<../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#schedule(co.cask.cdap.internal.schedule.ScheduleCreationSpec)>`__
. For example, the Purchase example creates a schedule for the PurchaseHistoryWorkflow
as follows::

  schedule(
    buildSchedule("DailySchedule", ProgramType.WORKFLOW, "PurchaseHistoryWorkflow")
      .withConcurrency(1).abortIfNotMet()
      .triggerByTime("0 4 * * *"));

This schedule uses a time trigger that fires every day at 4:00 am, and it specifies a
concurrency run constraint that will prevent the workflow execution if another run of
the same workflow is executing at that time. Note that:

- The buildSchedule method returns a builder to create schedules with various kinds of
  triggers and run constraints.
- Currently, the only program type that can be scheduled is a workflow.
- Replacing ``abortIfNotMet()`` with ``waitUntilMet()`` would have the effect that the
  workflow execution is delayed until no other concurrent run of the same workflow is
  executing.
- This schedule does not specify properties for the workflow execution.
- This schedule does not configure a timeout, such that the default timeout of one day
  is used.

If more than one conditions are required to construct the *Trigger* for the schedule,
to an application extended from ``AbstractApplication``, call method `getTriggerFactory
<../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#getTriggerFactory()>`__
to get a ``TriggerFactory`` to create a specific *Trigger* as follows::

  schedule(
    buildSchedule("Workflow1AndWorkflow2CompletedSchedule", ProgramType.WORKFLOW, "TriggeredWorkflow")
      .triggerOn(getTriggerFactory().and(getTriggerFactory().onProgramStatus(ProgramType.WORKFLOW, "Workflow1",
                                                                             ProgramStatus.COMPLETED),
                                         getTriggerFactory().onProgramStatus(ProgramType.WORKFLOW, "Workflow2",
                                                                             ProgramStatus.COMPLETED)));

This schedule uses an `and` trigger that can only be satisfied when both Workflow1
is completed and Workflow2 is completed.

Schedules can be added and controlled by the :ref:`CDAP CLI <cli>` and the :ref:`Lifecycle
HTTP RESTful API <http-restful-api-lifecycle>`. The :ref:`status of a schedule
<http-restful-api-lifecycle-status>` can be retrieved, and individual schedules can be
:ref:`added <http-restful-api-lifecycle-schedule-add>`,
:ref:`enabled, or disabled <http-restful-api-lifecycle-schedule-disable-enable>`.

When a schedule is initially deployed, it is in a *disabled* state; it needs to be *enabled* before
it starts executing workflows.

.. _schedules-jobs-lifecycle:

Jobs and their Lifecycle
========================

Execution of workflows is initiated when the trigger of a schedule fires. This creates
a *Job* for the workflow in the scheduler's *Job Queue*. This job will not necessarily
execute immediately; instead, each job goes through a life cycle:

- When a job is initially created, it is in state *pending trigger*. Most triggers are
  fulfilled immediately when the job is created. But some triggers may require additional
  input: For example, a partition trigger can specify a minimum number of new partitions
  to be present in a dataset. When one or more partitions are added to the dataset, then
  this creates an event that leads to the creation of a job. But the number of partitions
  may not be sufficient yet, and it would require more partition events until the trigger
  is fulfilled. Until then, the job will remain in *pending trigger* state.
- When the job’s trigger is fulfilled, the job’s state changes to *pending constraints*.
  If the job has no constraints, then it will not remain in this state; however, if it
  has constraints, then it remains *pending constraints* until all constraints are fulfilled.
  The scheduling system now continuously checks whether its constraints are fulfilled.
  During this check, if any constraint is not fulfilled and it was added with
  ``abortIfNotMet()``, then the job is aborted and removed from the job queue.
- When all of a job’s constraints are fulfilled, the job’s state changes to *pending launch*.
  At this time, the scheduling system will prepare the execution of the workflow, and once
  it is started successfully, the job is complete and removed from the job queue. Note that
  the workflow itself can still fail during its execution, but if the scheduler
  successfully submitted the workflow for execution, then the job is considered complete from
  the scheduler’s point of view. If starting the workflow fails, however, the job remains
  *pending launch* and the system will retry execution.
- If a job does not reach *pending launch* state before its configured timeout, it is
  aborted and removed from the job queue.
- If a schedule is deleted, modified or disabled, then all jobs for that schedule are
  aborted and removed from the job queue, regardless of their state. However, due to
  timing and concurrency, a job that is *pending launch* may still execute around the same
  time that the schedule was modified.
- At any given time, there is only one job in state *pending trigger* or *pending constraints*
  for a given schedule. That means that if the schedule’s trigger fires again, it does not
  create a new job in the job queue. Only after the job transitions into *pending launch*
  state can the schedule's trigger create a new job.

.. _schedules-events:

Events and Notifications
========================

Triggers are fired by events such as creation of a new partition in a dataset, or
fulfillment of a cron expression of a time trigger, or the status of a program. 
Events reach the scheduling system as notifications on the Transactional Messaging System (TMS). 
A single notification can contain multiple events, for example, two new partitions for a dataset.
For a time trigger, the event contains the logical start time, that is, the time when the cron
expression fired. This logical start time is given to the workflow as a runtime argument.
For a program status trigger, the event contains the triggering program status and
the triggering program run id.

.. _schedules-run-constraints:

Run Constraints
===============

A run constraint can either delay or prevent the execution of a schedule’s workflow, based
on a condition represented by the constraint. The default behavior of whether the execution
is delayed or aborted is different for each type of run constraint |---| it can be configured
explicitly by specifying either ``.waitUntilMet()`` or ``.abortIfNotMet()``
when adding the constraint to the schedule builder. Every individual type of run constraint
also has its own default for this behavior. These constraints are available:

- ``withConcurrency(int n)``: Fulfilled if less than n runs of the same workflow are
  currently executing. This is useful to limit the frequency and resource utilization
  of a single workflow. By default, this aborts the job if not fulfilled.
- ``withDelay(long n, TimeUnit unit)``: Fulfilled at least n time units after the job
  is created. This is useful to delay the execution of a workflow after its trigger
  fires, for example, if it is known that after some new data arrives, more new data
  may arrive within short time, and the workflow should wait for that.
- ``withTimeWindow(String startTime, String endTime)``: Fulfilled only in the time
  window between the given start and end time. Both times are given in “HH:mm” form,
  and an optional timezone can be given to interpret these times. By default, this
  delays the execution of the job, but it can be configured to abort the job
  if the trigger fires outside the time window. This is useful to limit the execution
  of certain workflows to times when the load on the cluster is low.
- ``withDurationSinceLastRun(long n, TimeUnit unit)``: Fulfilled only after n time
  units since the start of the last successful run of the same workflow. This is useful
  to limit the frequency of execution of the workflow. By default, this aborts the
  execution if not met.

.. _schedules-triggers:

Triggers
========

A trigger can be based on time or data availability, These are the available trigger types:

- ``time``: This is triggered every time the cron expression
  is met. A time trigger is fulfilled immediately, allowing the job to transition into
  *pending constraints* state right away. When the workflow executes, the time at which
  the cron expression was fulfilled is passed to the workflow as its logical start time.
- ``partition``: This is triggered by new partitions in the named dataset (it must be a
  ``PartitionedFileSet`` or a ``TimePartitionedFileSet``). While a single notification for
  new partitions in the dataset suffices to to create a job for the schedule, it will
  remain in *pending trigger* state until enough partitions have arrived to exceed
  the ``numPartitions`` parameter.
- ``program status``: This is triggered when a given program in the specific namespace,
  application, and application version transitions to any one of the given program statuses.
- ``and``: This is triggered when all the triggers contained in it are satisfied.
- ``or``: This is triggered when any of the given triggers contained in it is/are satisfied.

.. _schedules-examples:

Examples
========

To schedule a workflow whenever a new partition is added to a dataset, but delay execution
to the time window between 10pm and 6am::

  schedule(buildSchedule("runOnlyAtNight", ProgramType.WORKFLOW, "cleanupWorkflow")
             .withTimeWindow("22:00", "06:00”).waitUntilMet()
             .triggerOnPartitions("myDataset", 1));

The same as before, but ensure that it runs only once in that time window::

  schedule(buildSchedule("runOnlyAtNight", ProgramType.WORKFLOW, "cleanupWorkflow")
             .withTimeWindow("22:00", "06:00”).waitUntilMet()
             .withDurationSinceLastRun(6, TimeUnit.HOURS).abortIfNotMet()
             .triggerOnPartitions("myDataset", 1));

To schedule a workflow whenever there are four new partitions, with 15 minutes delay
to allow additional data to arrive::

  schedule(buildSchedule("onPartitionWithDelay", ProgramType.WORKFLOW, "myWorkflow")
             .withDelay(15, TimeUnit.MINUTES)
             .triggerOnPartitions("myDataset", 4));

To schedule a workflow named "cleanupWorkflow" to run whenever "dataProcessingWorkflow"
(in the same namespace, application, and application version as "cleanupWorkflow")
fails, and pass in the `src` directory in the "dataProcessingWorkflow" as the 
`cleanup_dir` directory::

  schedule(buildSchedule("onDataProcessingFail", ProgramType.WORKFLOW, "cleanupWorkflow")
              .withProperties(ImmutableMap.of("triggering.properties.mapping", 
                                              ImmutableMap.of("cleanup_dir", "src"))
              .triggerOnProgramStatus(ProgramType.WORKFLOW, "dataProcessingWorkflow");

To ensure that the workflow runs at least once per hour::

  schedule(buildSchedule("onPartitionWithDelay", ProgramType.WORKFLOW, "myWorkflow")
             .setProperties(ImmutableMap.of("triggered.by", "data"))
             .withDelay(15, TimeUnit.MINUTES)
             .withConcurrency(1).abortIfNotMet()
             .triggerOnPartitions("myDataset", 4));
  schedule(buildSchedule("onceHourly", ProgramType.WORKFLOW, "myWorkflow")
             .setProperties(ImmutableMap.of("triggered.by", "time"))
             .withConcurrency(1).abortIfNotMet()
             .withDurationSinceLastRun(1, TimeUnit.HOURS).abortIfNotMet()
             .triggerByTime("0 * * * *"));

We added another schedule that runs once hourly, but only if no other run of the workflow
in the last hour succeeded and no concurrent run is happening. We also add the concurrency
constraint to the first schedule to make sure it does not kick off when the second schedule is
just executing a job.

Note that through the properties we can indicate to the workflow which schedule triggered
it |---| we could also pass arbitrary other properties.


.. _schedules-manage:

Managing Schedules
==================

.. rubric:: Schedules Lifecycle

These actions can be performed on a schedule:

- *Create*: This happens either as part of application deployment or through the Lifecycle HTTP
  RESTful API. After creating a schedule, it is initially disabled and will not execute any jobs.
- *Disable*: Disabling a schedule will delete all pending jobs for the schedule from the job
  queue, and prevent new jobs from being created. This action will not suspend or abort any
  current execution of the workflow.
- *Enable*: This action will put the schedule back into an active state, after a *Disable*
  action. Note that if the schedule was previously disabled, that aborted all pending jobs
  for the schedule. Therefore new triggers have to create new jobs for this schedule before
  its workflow is executed again.
- *Delete*: This first disables the schedule and then permanently deletes it.
- *Update*: This is equivalant to deleting the current schedule and creating a new one. It happens
  either when an application which contains a schedule is redeployed in CDAP, or through the
  Lifecycle HTTP RESTful API.

.. rubric:: Application Deployment and Schedules

Schedules can be defined as part of application deployment, by calling ``schedule()``
in the application's ``configure()`` method; or they can be managed separately from
application deployment through the HTTP RESTful API. This can create a dilemma: After
modifiying schedules through REST, redeploying the application, which may happen for
reasons unrelated to the schedules, would undo all those changes and reinstate the schedules
defined by the ``configure()`` method. Because that is undesired, CDAP provides an option
to configure whether schedules are controlled by the ``configure()`` method or not. This
option is called ``app.deploy.update.schedules`` and is given as a field of the
:ref:`application deployment request <http-restful-api-lifecycle-create-app>`.

- If this option is true, then application deployment will replace all schedules for
  the application with the schedules given by the ``configure()`` method;
- if it is false, application deployment will not change any schedules, except that if
  the application is updated, all schedules for programs that do not exist any longer
  after the update are deleted.

.. rubric:: Special Runtime Arguments

When a schedule with a time trigger executes a workflow it passes in the ``logicalStartTime``
runtime argument: this is the timestamp, in milliseconds, at which the schedule's cron expression
was fulfilled and triggered the job. 
