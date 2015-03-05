.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _schedules:

============================================
Schedules
============================================

**Schedules** are used to execute :ref:`Workflows <workflows>` on a recurring basis. You can add a Schedule
to the Workflow using
`this API<../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#scheduleWorkflow(co.cask.cdap.api.schedule.Schedule,%20java.lang.String)>`__

The `Schedules<../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.html>`__
class contains static methods to create schedules based on time, or schedules based on data availability.

.. rubric::  Time Schedules

**Time Schedules** will execute based on a
`crontab schedule<../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.html#createTimeSchedule(java.lang.String,%20java.lang.String,%20java.lang.String)>`__.
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

.. rubric::  Stream Size Schedules

**Stream Size Schedules** will execute based on data ingested in :ref:`Streams <streams>`, using the
`createDataSchedule API<../../reference-manual/javadocs/co/cask/cdap/api/schedule/Schedules.html#createDataSchedule(java.lang.String,%20java.lang.String,%20co.cask.cdap.api.schedule.Source,%20java.lang.String,%20int)>`__.
Here is an example to add a **Stream Size Schedule** based on a Stream named ``purchaseStream`` that triggers
every time the Stream ingests 1MB of data::

    scheduleWorkflow(Schedules.createDataSchedule("1MBStreamSchedule", "Schedule triggered every 1MB of ingested data", Schedules.Source.STREAM, "purchaseStream"),
                     "MyWorkflow");

The ``purchaseStream`` will either have to exist in CDAP when deploying your application, or you will have to
`add it<../../reference-manual/javadocs/co/cask/cdap/api/app/AbstractApplication.html#addStream(co.cask.cdap.api.data.stream.Stream)>`__
when configuring your application. If you try to create a **Stream Size Schedule** on a Stream which does not exist,
your application will fail to deploy.

Optionally, you can specify the properties for the Schedule::

    ...
    Map<String, String> scheduleProperties = Maps.newHashMap();
    scheduleProperties.put("myProperty", "10");
    scheduleProperties.put("anotherProperty", "anotherValue");

    scheduleWorkflow(Schedules.createDataSchedule("1MBStreamSchedule", "Schedule triggered every 1MB of ingested data", Schedules.Source.STREAM, "purchaseStream"),
                     "MyWorkflow", scheduleProperties);
    ...

Every time the ``1MBStreamSchedule`` triggers, it passes the ``scheduleProperties`` as runtime arguments to the ``MyWorkflow``.
The properties specified by the Schedule can be overridden by the Workflow's runtime arguments, with the Workflow's having precedence.

The **Stream Size Schedule** uses two sources of information to know how much data has been ingested by a Stream.
One is :ref:`Stream size notifications <streams-notifications>`, and the other one is the aggregated metric that
records the total size of events ingested by a Stream.
Notifications are only used as an indication that data is available. Stream Size Schedules always confirm there
information by querying the Metric system, which is the reliable source of information.

// TODO add a smaller rubric for the stream size schedule lifecycle
