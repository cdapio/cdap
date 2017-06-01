.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

.. _schedules:

=========
Schedules
=========

*Schedules* are used to execute :ref:`workflows <workflows>` on a recurring basis. You
can add a schedule to a workflow using the ``scheduleWorkflow`` method of the
`AbstractApplication class <../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#scheduleWorkflow(co.cask.cdap.api.schedule.Schedule,%20java.lang.String)>`__

The `Schedules <../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.html>`__
class contains a builder to create schedules based on time, or schedules based on data availability.

The name of a schedule must be unique in the application that it is in; the same name can
be used in different applications.

Schedules can be added and controlled by the :ref:`CDAP CLI <cli>` and the :ref:`Lifecycle HTTP
RESTful API <http-restful-api-lifecycle>`. The :ref:`status of a schedule
<http-restful-api-lifecycle-status>` can be retrieved, and individual schedules can be 
:ref:`added <http-restful-api-lifecycle-schedule-add>`, 
:ref:`resumed, or suspended <http-restful-api-lifecycle-schedule-suspend-resume>`. 

When a schedule is initially deployed, it is in a *suspended* state; a *resume* command needs to be
issued to change it to *scheduled* before it will begin.

.. _schedules-time:

Time Schedules
==============

**Time schedules** will execute based on a
`crontab schedule <../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.Builder.html#createTimeSchedule(java.lang.String)>`__.
You can add such a schedule to a workflow::

    scheduleWorkflow(Schedules.builder("FiveHourSchedule")
                       .setDescription("Schedule running every 5 hours")
                       .createTimeSchedule("0 */5 * * *"),
                     "MyWorkflow");

The ``MyWorkflow`` will then be executed every 5 hours.

Optionally, you can specify the properties for the schedule::

    ...
    Map<String, String> scheduleProperties = Maps.newHashMap();
    scheduleProperties.put("myProperty", "10");
    scheduleProperties.put("anotherProperty", "anotherValue");

    scheduleWorkflow(Schedules.builder("FiveHourSchedule")
                       .setDescription("Schedule running every 5 hours")
                       .createTimeSchedule("0 */5 * * *"),
                     "MyWorkflow", scheduleProperties);
    ...

Every time the ``FiveHourSchedule`` triggers, it passes the ``scheduleProperties`` as runtime arguments to the ``MyWorkflow``.


.. _schedules-stream-size:
.. _stream-size-schedules:

Stream-size Schedules
=====================

.. rubric:: Definition and Usage

**Stream-size schedules** will execute based on data ingested in :ref:`streams <streams>`, using the
`createDataSchedule API <../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.Builder.html#createDataSchedule(co.cask.cdap.api.schedule.Schedules.Source,%20java.lang.String,%20int)>`__.
Here is an example to add a stream-size schedule based on a stream named ``purchaseStream`` that triggers
every time the stream has ingested 1MB of data::

    scheduleWorkflow(Schedules.builder("1MBStreamSchedule")
                       .setDescription("Schedule triggered every 1MB of ingested data")
                       .createDataSchedule(Schedules.Source.STREAM, "purchaseStream", 1),
                     "MyWorkflow");

The ``purchaseStream`` will either have to already exist in CDAP when deploying your application, or you will have to
`add it <../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#addStream(co.cask.cdap.api.data.stream.Stream)>`__
when configuring your application. If you try to create a stream-size schedule on a stream which does not exist,
your application will fail to deploy.

You can optionally specify the properties for the schedule::

    ...
    Map<String, String> scheduleProperties = Maps.newHashMap();
    scheduleProperties.put("myProperty", "10");
    scheduleProperties.put("anotherProperty", "anotherValue");

    scheduleWorkflow(Schedules.builder("1MBStreamSchedule")
                       .setDescription("Schedule triggered every 1MB of ingested data")
                       .createDataSchedule(Schedules.Source.STREAM, "purchaseStream", 1),
                     "MyWorkflow", scheduleProperties);
    ...

Every time the ``1MBStreamSchedule`` triggers, it passes the ``scheduleProperties`` as runtime arguments to the ``MyWorkflow``.

The stream-size schedule uses two sources of information to know how much data has been ingested by a Stream.
One is :ref:`stream size notifications <streams-notifications>`; the other is the aggregated metric that
records the total size of events ingested by a stream.
Notifications are only used as an indication that data is available. Stream-size schedules always confirm the
information by querying the metric system, which is the canonical source of information.


.. rubric:: Stream-size Schedules Lifecycle

A stream-size schedule will execute a workflow every time the stream it references ingests an increment of data,
also defined in the
`schedule <../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.Builder.html#createDataSchedule(co.cask.cdap.api.schedule.Schedules.Source,%20java.lang.String,%20int)>`__.

When a stream-size schedule is first created, during the deployment of an application, it will wait for the
increment of data that it defined, starting from the current size of the stream as given by the Metric system.

These actions can be performed on a schedule:

- *Suspend*: Suspending a schedule will prevent the workflow from being executed again, in the event that the Stream
  ingests enough data. This action will not suspend any current execution of the workflow, if the schedule
  triggered it recently. Suspending an already-suspended schedule has no effect.
- *Resume*: This action will put the schedule back into an active state, after a *Suspend* action. Resuming an
  already-active schedule has no effect. If |---| while the schedule was suspended, and since the last time the
  schedule was triggered |---| the Stream has ingested more than the increment of data defined by the schedule,
  the workflow will be immediately executed upon resume.
- *Update*: this action is triggered when an application which contains a schedule is redeployed in CDAP.
  If the schedule name has not been modified and one of its parameters has changed |---| such as the `dataTrigger` |---| the
  *update* action will be called. If the `dataTrigger` is updated, and the Stream has ingested more data than defined
  by the `dataTrigger` since the last time the schedule was triggered, then the workflow will be executed upon
  update. A similar behaviour triggers a time schedule, if the time has passed for when it should have been executed.

.. rubric:: Special Runtime Arguments

When a stream-size schedule executes a workflow, it passes in these runtime arguments:

- ``logicalStartTime``: the timestamp, in milliseconds, at which the schedule received the information that the Stream
  had ingested enough data for the workflow to be executed;
- ``runDataSize``: the size of the Stream, in bytes, seen by the schedule when it triggered the current execution of
  the workflow;
- ``lastScheduledRunLogicalStartTime``: the ``logicalStartTime`` of the previous execution of the workflow by this schedule.
  This argument is not present if the workflow has never been executed by the schedule;
- ``lastScheduledRunDataSize``: the ``runDataSize`` of the previous execution of the workflow by this schedule.
  This argument is not present if the workflow has never been executed by the schedule;
- ``runBaseCountTime``: the timestamp, in milliseconds, at which the schedule started to wait for an increment of data
  being ingested by the Stream, before executing the workflow. Most of the time, this argument and
  ``lastScheduledRunLogicalStartTime`` will be identical, either ing the first execution of the workflow by the
  schedule, or if the counting logic in the schedule is disrupted |---| for example, by the deletion of Stream metrics;
- ``runBaseCountSize``: the size of the Stream, in bytes, recorded when the schedule started to wait for an increment of data
  to be ingested by the Stream, before executing the workflow. Most of the time, this argument and
  ``lastScheduledRunDataSize`` will be identical, except either during the first execution of the workflow by the
  schedule, or if the counting logic in the schedule is disrupted—for example, by the deletion of Stream metrics.

.. _run-constraints:

Run Constraints
===============

.. rubric:: Maximum Concurrent Runs

Normally, there is no limit on the number of concurrent runs of a workflow.
If you want to limit the number of concurrent workflows,
you can do so when creating the schedule::

    scheduleWorkflow(Schedules.builder("FiveHourSchedule")
                       .setDescription("Schedule running every 5 hours")
                       .setMaxConcurrentRuns(1)
                       .createTimeSchedule("0 */5 * * *"),
                     "MyWorkflow", scheduleProperties);

In this example, we have set the maximum number of concurrent runs for this schedule
to one. When the schedule is triggered, the scheduler will check how many active
runs of the schedule exist. If there is at least one, the scheduled run will
be skipped. An active run is any run that is not completed, failed, or killed.
This includes suspended runs. This number also only includes runs of the program
started by this schedule. In the example above, if ``MyWorkflow`` was started manually
or by another schedule, those runs will not be counted.

