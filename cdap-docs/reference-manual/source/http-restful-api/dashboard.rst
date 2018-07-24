.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2018 Cask Data, Inc.

.. _http-restful-api-dashboard:

==========================
Dashboard HTTP RESTful API
==========================
.. topic::  **Note:**

    Dashboard is currently a **beta** feature of CDAP |release|, and is subject to change without notice.

The CDAP Dashboard HTTP API is used to retrieve real time dashboard details.

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


Dashboard HTTP REST API
=======================

Dashboard endpoint to query the dashboard for the programs that ran in a time range and programs schedules that will run in the future.

.. highlight:: console

::

  GET /v3/dashboard?start=<query-start-time>&duration=<query-duration>&namespace=<namespaces>

.. list-table::
 :widths: 20 80
 :header-rows: 1

 * - Query Parameter
   - Description
 * - ``start``
   - Query start time in seconds
 * - ``duration``
   - Duration in seconds, query end time is inferred as startTime + duration in seconds
 * - ``namespace``
   - Set of namespaces, used for filtering by namespace, only programs that ran in this namespace or schedules that will be run in this namespace will be returned.

For the given query timerange, for the time between start time and the current-time, the response will
return historical runs which were either started, running or reached end state (stopped/killed/failed) in that time window.

For the future time range, time range after current time till the end time,
The response will return all the programs that are scheduled to run in that time window.
If a program is scheduled to be run multiple times in the query time range, then all of those will be returned.

Example response for dashboard API query:

.. highlight:: json

::

 [
    // completed program run
    {
        "application": {
            "name": "HDFSPipeline",
            "version": "1.0-SNAPSHOT"
        },
        "artifact": {
            "name": "cdap-data-pipeline",
            "scope": "SYSTEM",
            "version": "5.0.0-SNAPSHOT"
        },
        "end": 1532483112,
        "namespace": "default",
        "program": "phase-1",
        "run": "5b89e259-8fac-11e8-bba1-acde48001122",
        "running": 1532483108,
        "start": 1532483106,
        "startMethod": "SCHEDULED",
        "status": "COMPLETED",
        "type": "SPARK"
    },
    ....
    // scheduled program run in future
    {
        "application": {
            "name": "testPipeline",
            "version": "1.1-SNAPSHOT"
        },
        "artifact": {
            "name": "cdap-data-pipeline",
            "scope": "SYSTEM",
            "version": "5.0.0-SNAPSHOT"
        },
        "namespace": "default",
        "program": "DataPipelineWorkflow",
        "start": 1532487600,
        "startMethod": "SCHEDULED",
        "type": "WORKFLOW"
    }
 ]
