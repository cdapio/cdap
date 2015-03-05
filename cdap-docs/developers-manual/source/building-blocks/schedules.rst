.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _schedules:

============================================
Schedules
============================================

**Schedules** are used to execute :ref:`Workflows <workflows>` on a recurring basis. You can add a Schedule
to the Workflow using
`this API <../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#scheduleWorkflow(co.cask.cdap.api.schedule.Schedule,%20java.lang.String)>`__

The `Schedules <../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.html>`__
class contains static methods to create schedules based on time, or schedules based on data availability.

Time Schedules
==============

**Time Schedules** will execute based on a
`crontab schedule <../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.html#createTimeSchedule(java.lang.String,%20java.lang.String,%20java.lang.String)>`__.
You can add such a Schedule to a Workflow::

    scheduleWorkflow(Schedules.createTimeSchedule("FiveHourSchedule", "Schedule running every 5 hours", "0 */5 * * *"),
                     "MyWorkflow");

The ``MyWorkflow`` will then be executed every 5 hours.

Optionally, you can specify the properties for the Schedule::

    ...
    Map<String, String> scheduleProperties = Maps.newHashMap();
    scheduleProperties.put("myProperty", "10");
    scheduleProperties.put("anotherProperty", "anotherValue");

    scheduleWorkflow(Schedules.createTimeSchedule("FiveHourSchedule", "Schedule running every 5 hours", "0 */5 * * *"),
                     "MyWorkflow", scheduleProperties);
    ...

Every time the ``FiveHourSchedule`` triggers, it passes the ``scheduleProperties`` as runtime arguments to the ``MyWorkflow``.
The properties specified by the Schedule can be overridden by the Workflow's runtime arguments, with the Workflow's having precedence.


.. _stream-size-schedules:

Stream Size Schedules
=====================

.. rubric:: Definition and Usage of Stream Size Schedules

**Stream Size Schedules** will execute based on data ingested in :ref:`Streams <streams>`, using the
`createDataSchedule API <../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.html#createDataSchedule(java.lang.String,%20java.lang.String,%20co.cask.cdap.api.schedule.Source,%20java.lang.String,%20int)>`__.
Here is an example to add a **Stream Size Schedule** based on a Stream named ``purchaseStream`` that triggers
every time the Stream ingests 1MB of data::

    scheduleWorkflow(Schedules.createDataSchedule("1MBStreamSchedule", "Schedule triggered every 1MB of ingested data",
                                                  Schedules.Source.STREAM, "purchaseStream", 1),
                     "MyWorkflow");

The ``purchaseStream`` will either have to exist in CDAP when deploying your application, or you will have to
`add it <../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#addStream(co.cask.cdap.api.data.stream.Stream)>`__
when configuring your application. If you try to create a **Stream Size Schedule** on a Stream which does not exist,
your application will fail to deploy.

Optionally, you can specify the properties for the Schedule::

    ...
    Map<String, String> scheduleProperties = Maps.newHashMap();
    scheduleProperties.put("myProperty", "10");
    scheduleProperties.put("anotherProperty", "anotherValue");

    scheduleWorkflow(Schedules.createDataSchedule("1MBStreamSchedule", "Schedule triggered every 1MB of ingested data",
                                                  Schedules.Source.STREAM, "purchaseStream", 1),
                     "MyWorkflow", scheduleProperties);
    ...

Every time the ``1MBStreamSchedule`` triggers, it passes the ``scheduleProperties`` as runtime arguments to the ``MyWorkflow``.
The properties specified by the Schedule can be overridden by the Workflow's runtime arguments, with the Workflow's having precedence.

The **Stream Size Schedule** uses two sources of information to know how much data has been ingested by a Stream.
One is :ref:`Stream size notifications <streams-notifications>`, and the other one is the aggregated metric that
records the total size of events ingested by a Stream.
Notifications are only used as an indication that data is available. Stream Size Schedules always confirm there
information by querying the Metric system, which is the reliable source of information.


.. rubric:: Stream Size Schedules Lifecycle

A **Stream Size Schedule** will execute a Workflow every time the Stream it references ingests an increment of data,
also defined in the
`Schedule. <../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.html#createDataSchedule(java.lang.String,%20java.lang.String,%20co.cask.cdap.api.schedule.Source,%20java.lang.String,%20int)>`__

A few actions can be performed on a Schedule:

- *Suspend*: suspending a schedule will prevent the Workflow from being executed again, in the event that the stream
  ingests enough data. This action will not suspend the current execution of the Workflow, if the Schedule triggered
  it recently. Suspending an already suspended Schedule has no effect;
- *Resume*: this action will put the Schedule back into an active state, after a *Suspend* action. Resuming an already
  active Schedule has no effect. If the Stream has ingested more than the increment of data defined by the Schedule
  since the last time the Schedule was triggered, while the Schedule was suspended, then the Workflow will be executed
  upon resume;
- *Update*: this action is triggered when an application which contains a Stream Size Schedule is redeployed in CDAP.
  If the Schedule name has not been modified and one of its parameter has changed - such as the `dataTrigger` - the
  *Update* action will be called. If the `dataTrigger` is updated, and the Stream has ingested more data than defined
  by the `dataTrigger` since the last time the Schedule was triggered, then the Workflow will be executed upon
  update.

.. rubric:: Stream Size Schedules Special Runtime Arguments

When a **Stream Size Schedule** executes a Workflow, it passes to its context the following set of runtime arguments:

- `logicalStartTime`: the timestamp, in milliseconds, at which the Schedule received the information that the Stream
  had ingested enough data for the Workflow to be executed;
- `runDataSize`: the size of the Stream, in bytes, seen by the Schedule when it triggered the current execution of
  the Workflow;
- `lastScheduledRunLogicalStartTime`: the `logicalStartTime` of the previous execution of the Workflow by this Schedule.
  This argument is not present if the Workflow has never been executed by the Schedule;
- `lastScheduledRunDataSize`: the `runDataSize` of the previous execution of the Workflow by this Schedule.
  This argument is not present if the Workflow has never been executed by the Schedule;
- `runBaseCountTime`: the timestamp, in milliseconds, at which the Schedule started to wait for an increment of data
  being ingested by the Stream, before executing the Workflow. Most of the time, this argument and
  `lastScheduledRunLogicalStartTime` will be identical, except during the first execution of the Workflow by the
  Schedule, or if the counting logic in the Schedule is disrupted - for example by the deletion of Stream metrics;
- `runBaseCountSize`: the size of the Stream, in bytes, recorded when the Schedule started to wait for an increment of data
  to be ingested by the Stream, before executing the Workflow. Most of the time, this argument and
  `lastScheduledRunDataSize` will be identical, except during the first execution of the Workflow by the
  Schedule, or if the counting logic in the Schedule is disrupted - for example by the deletion of Stream metrics.

