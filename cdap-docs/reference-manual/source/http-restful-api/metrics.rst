.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _http-restful-api-metrics:

===========================================================
Metrics HTTP RESTful API
===========================================================

.. highlight:: console

As Applications process data, CDAP collects metrics about the Application’s behavior and performance. Some of these
metrics are the same for every Application—how many events are processed, how many data operations are performed,
etc.—and are thus called system or CDAP metrics.

Other metrics are user-defined and differ from Application to Application. 
For details on how to add metrics to your Application, see the section on 
:ref:`User-Defined Metrics <operations-metrics>` in
the :ref:`CDAP Administration Manual. <admin-index>`

Metrics Requests
----------------
The general form of a metrics request is::

  GET <base-url>/metrics/<scope>/<context>/<run-id>/<metric>?<time-range>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<scope>``
     - Either ``system`` (system metrics) or ``user`` (user-defined metrics)
   * - ``<context>``
     - Hierarchy of context; see `Available Contexts`_
   * - ``<run-id>``
     - Run-ID of the program; see `Querying by Run-ID`_
   * - ``<metric>``
     - Metric being queried; see `Available Metrics`_
   * - ``<time-range>``
     - A `Time Range`_ or ``aggregate=true`` for all since the Application was deployed

Examples
........
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/metrics/system/apps/HelloWorld/flows/``
       ``WhoFlow/flowlets/saver/process.busyness?aggregate=true``
   * - Description
     - Using a *System* metric, *process.busyness*
   * - 
     - 
   * - HTTP Method
     - ``GET <base-url>/metrics/user/apps/HelloWorld/flows/``
       ``WhoFlow/runs/13ac3a50-a435-49c8-a752-83b3c1e1b9a8/flowlets/saver/names.bytes?aggregate=true``
   * - Description
     - Querying the *User-Defined* metric *names.bytes*, of the Flow *saver*, by its run-ID
   * - 
     - 
   * - HTTP Method
     - ``GET <base-url>/metrics/user/apps/HelloWorld/services/``
       ``WhoService/runnables/WhoRun/names.bytes?aggregate=true``
   * - Description
     - Using a *User-Defined* metric, *names.bytes* in a Service's Handler

Comments
........
The scope must be either ``system`` for system metrics or ``user`` for user-defined metrics.

System metrics are either Application metrics (about Applications and their Flows, Procedures, MapReduce and Workflows) or they are Data metrics (relating to Streams or Datasets).

User metrics are always in the Application context.

For example, to retrieve the number of input data objects (“events”) processed by a Flowlet named *splitter*,
in the Flow *CountRandomFlow* of the Application *CountRandom*, over the last 5 seconds, you can issue an HTTP
GET method::

  GET <base-url>/metrics/system/apps/CountRandom/flows/CountRandomFlow/flowlets/
          splitter/process.events.processed?start=now-5s&count=5

This returns a JSON response that has one entry for every second in the requested time interval. It will have
values only for the times where the metric was actually emitted (shown here "pretty-printed")::

  HTTP/1.1 200 OK
  Content-Type: application/json
  {"start":1382637108,"end":1382637112,"data":[
  {"time":1382637108,"value":6868},
  {"time":1382637109,"value":6895},
  {"time":1382637110,"value":6856},
  {"time":1382637111,"value":6816},
  {"time":1382637112,"value":6765}]}

Each run of a flow is identified by a run-ID. To retrieve the aggregate of events processed by the
run of a flow, you can issue an HTTP GET method::

  GET <base-url>/metrics/system/apps/CountRandom/flows/CountRandomFlow/runs/13ac3a50-a435-49c8-a752-83b3c1e1b9a8/flowlets/
          splitter/process.events.processed?aggregate=true

If the run-ID is not specified, we aggregate the events processed for all the runs of this flow.

If you want the number of input objects processed across all Flowlets of a Flow, you address the metrics
API at the Flow context::

  GET <base-url>/metrics/system/apps/CountRandom/flows/
    CountRandomFlow/process.events.processed?start=now-5s&count=5

Similarly, you can address the context of all flows of an Application, an entire Application, or the entire CDAP::

  GET <base-url>/metrics/system/apps/CountRandom/
    flows/process.events.processed?start=now-5s&count=5
  GET <base-url>/metrics/system/apps/CountRandom/
    process.events.processed?start=now-5s&count=5
  GET <base-url>/metrics/system/process.events?start=now-5s&count=5

To request user-defined metrics instead of system metrics, specify ``user`` instead of ``cdap`` in the URL
and specify the user-defined metric at the end of the request.

For example, to request a user-defined metric for the *HelloWorld* Application's *WhoFlow* Flow::

  GET <base-url>/metrics/user/apps/HelloWorld/flows/
    WhoFlow/flowlets/saver/names.bytes?aggregate=true

To retrieve multiple metrics at once, instead of a GET, issue an HTTP POST, with a JSON list as the request body that enumerates the name and attributes for each metrics. For example::

  POST <base-url>/metrics

with the arguments as a JSON string in the body::

  Content-Type: application/json
  [ "/system/collect.events?aggregate=true",
  "/system/apps/HelloWorld/process.events.processed?start=1380323712&count=6000" ]

If the context of the requested metric or metric itself doesn't exist the system returns status 200 (OK) with JSON formed as per above description and with values being zeroes.

Time Range
----------
The time range of a metric query can be specified in various ways:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Time Range
     - Description
   * - ``start=now-30s&end=now``
     - The last 30 seconds. The start time is given in seconds relative to the current time.
       You can apply simple math, using ``now`` for the current time, 
       ``s`` for seconds, ``m`` for minutes, ``h`` for hours and ``d`` for days. 
       For example: ``now-5d-12h`` is 5 days and 12 hours ago.
   * - ``start=1385625600&`` ``end=1385629200``
     - From ``Thu, 28 Nov 2013 08:00:00 GMT`` to ``Thu, 28 Nov 2013 09:00:00 GMT``,
       both given as since the start of the Epoch
   * - ``start=1385625600&`` ``count=3600``
     - The same as before, but with the count given as a number of seconds

Instead of getting the values for each second of a time range, you can also retrieve the
aggregate of a metric over time. The following request will return the total number of input objects processed since the Application *CountRandom* was deployed, assuming that CDAP has not been stopped or restarted (you cannot specify a time range for aggregates)::

  GET <base-url>/metrics/system/apps/CountRandom/process.events.processed?aggregate=true

If the metric is a gauge type, the aggregate will return the latest value set for the metric.

This request will retrieve the completion percentage for the map-stage of the MapReduce ``PurchaseHistoryWorkflow_PurchaseHistoryBuilder``::

  GET <base-ur>/metrics/system/apps/PurchaseHistory/mapreduce/PurchaseHistoryWorkflow_PurchaseHistoryBuilder/mappers/process.completion?aggregate=true

Available Contexts
------------------
The context of a metric is typically enclosed into a hierarchy of contexts. For example, the Flowlet context is enclosed in the Flow context, which in turn is enclosed in the Application context. A metric can always be queried (and aggregated) relative to any enclosing context. These are the available Application contexts of CDAP:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - System Metric
     - Context
   * - One Flowlet of a Flow
     - ``/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>``
   * - All Flowlets of a Flow
     - ``/apps/<app-id>/flows/<flow-id>``
   * - All Flowlets of all Flows of an Application
     - ``/apps/<app-id>/flows``
   * - One Procedure
     - ``/apps/<app-id>/procedures/<procedure-id>``
   * - All Procedures of an Application
     - ``/apps/<app-id>/procedures``
   * - All Mappers of a MapReduce
     - ``/apps/<app-id>/mapreduce/<mapreduce-id>/mappers``
   * - All Reducers of a MapReduce
     - ``/apps/<app-id>/mapreduce/<mapreduce-id>/reducers``
   * - One MapReduce
     - ``/apps/<app-id>/mapreduce/<mapreduce-id>``
   * - All MapReduce of an Application
     - ``/apps/<app-id>/mapreduce``
   * - One Spark Program
     - ``/apps/<app-id>/spark/<spark-id>``
   * - One Service Handler/Worker
     - ``/apps/<app-id>/services/<service-id>/runnables/<runnable-id>``
   * - One Service
     - ``/apps/<app-id>/services/<service-id>``
   * - All Services of an Application
     - ``/apps/<app-id>/services``
   * - All components of an Application
     - ``/apps/<app-id>``
   * - All components of all Applications
     - ``/``

Stream metrics are only available at the Stream level and the only available context is:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Stream Metric
     - Context
   * - A single Stream
     - ``/streams/<stream-id>``

Dataset metrics are available at the Dataset level, but they can also be queried down to the
Flowlet, Procedure, Mapper, or Reducer level:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Dataset Metric
     - Context
   * - A single Dataset in the context of a single Flowlet
     - ``/datasets/<dataset-id>/apps/<app-id>/flows/``
       ``<flow-id>/flowlets/<flowlet-id>``
   * - A single Dataset in the context of a single Flow
     - ``/datasets/<dataset-id>/apps/<app-id>/flows/<flow-id>``
   * - A single Dataset in the context of a specific Application
     - ``/datasets/<dataset-id>/<any application context>``
   * - A single Dataset across all Applications
     - ``/datasets/<dataset-id>``
   * - All Datasets across all Applications
     - ``/``

Querying by Run-ID
------------------

Each execution of an program (Flow, MapReduce, Spark, Services, Procedure) has an associated run-ID that uniquely identifies that program's run.
We can query metrics for an program by its run-ID to see the metrics for a particular run.
Please see the :ref:`Run Records and Schedule <rest-program-runs>` on retrieving active and historical program runs.

When querying by run-ID, it is specified after the ``program-id`` in the path::

  /apps/<app-id>/<program-type>/<program-id>/runs/<run-id>/

Examples ::

    GET /apps/<app-id>/flows/<flow-id>/runs/<run-id>/flowlets/<flowlet-id>/
    GET /apps/<app-id>/mapreduce/<mapreduce-id>/runs/<run-id>/

Available Metrics
-----------------
For CDAP metrics, the available metrics depend on the context.
User-defined metrics will be available at whatever context that they are emitted from.

These metrics are available in the Flowlet context:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Flowlet Metric
     - Description
   * - ``process.busyness``
     - A number from 0 to 100 indicating how “busy” the Flowlet is;
       note that you cannot aggregate over this metric
   * - ``process.errors``
     - Number of errors while processing
   * - ``process.events.processed``
     - Number of events/data objects processed
   * - ``process.events.in``
     - Number of events read in by the Flowlet
   * - ``process.events.out``
     - Number of events emitted by the Flowlet
   * - ``store.bytes``
     - Number of bytes written to Datasets
   * - ``store.ops``
     - Operations (writes and read) performed on Datasets
   * - ``store.reads``
     - Read operations performed on Datasets
   * - ``store.writes``
     - Write operations performed on Datasets

These metrics are available in the Mappers and Reducers context:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Mappers and Reducers Metric
     - Description
   * - ``process.completion``
     - A number from 0 to 100 indicating the progress of the Map or Reduce phase
   * - ``process.entries.in``
     - Number of entries read in by the Map or Reduce phase
   * - ``process.entries.out``
     - Number of entries written out by the Map or Reduce phase

These metrics are available in the Spark context:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Spark Metric
     - Description
   * - ``<spark-id>.BlockManager.disk.diskSpaceUsed_MB``
     - Disk space used by the Block Manager
   * - ``<spark-id>.BlockManager.memory.maxMem_MB``
     - Maximum memory given to the Block Manager
   * - ``<spark-id>.BlockManager.memory.memUsed_MB``
     - Memory used by the Block Manager
   * - ``<spark-id>.BlockManager.memory.remainingMem_MB``
     - Memory remaining to the Block Manager
   * - ``<spark-id>.DAGScheduler.job.activeJobs``
     - Number of active jobs
   * - ``<spark-id>.DAGScheduler.job.allJobs``
     - Total number of jobs
   * - ``<spark-id>.DAGScheduler.stage.failedStages``
     - Number of failed stages
   * - ``<spark-id>.DAGScheduler.stage.runningStages``
     - Number of running stages
   * - ``<spark-id>.DAGScheduler.stage.waitingStages``
     - Number of waiting stages

These metrics are available in the Procedures context:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Procedures Metric
     - Description
   * - ``query.requests``
     - Number of requests made to the Procedure
   * - ``query.failures``
     - Number of failures seen by the Procedure

These metrics are available in the Streams context:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Streams Metric
     - Description
   * - ``collect.events``
     - Number of events collected by the Stream
   * - ``collect.bytes``
     - Number of bytes collected by the Stream

These metrics are available in the Datasets context:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Datasets Metric
     - Description
   * - ``store.bytes``
     - Number of bytes written
   * - ``store.ops``
     - Operations (reads and writes) performed
   * - ``store.reads``
     - Read operations performed
   * - ``store.writes``
     - Write operations performed

