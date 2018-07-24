.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2018 Cask Data, Inc.

.. _http-restful-api-reports:

==========================
Reports HTTP RESTful API
==========================
.. topic::  **Note:**

    Reports is currently a **beta** feature of CDAP |release|, and is subject to change without notice.

Overview
========

Reports provides administrators and developers a way to generate comprehensive report of program runs,
specify filters based on various options such as namespaces, program status and type of program,
within a given time range such as 24 hours or one week or a custom time range. You can also specify the columns
that you would like to include in the generated report.

Reports are backed by the ReportGenerationApp system application, which is a Spark service (named ReportGenerationSpark)
with endpoints to generate, list, view, download, share, save or delete reports.

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


Reports HTTP REST API
=======================

Generating Reports
-------------------
.. highlight:: console

::

 POST:
 /v3/namespaces/system/apps/ReportGenerationApp/spark/ReportGenerationSpark/methods/reports

With Request Json provided as part of the request body.



Request Json with explanation of JSON fields.

.. highlight:: console

::

 {
   "name":<Name of the report>,
   "start":<start-time-in-seconds>,
   "end":<end-time-in-seconds>,

   "fields":[
      <include-this-field-1-in-report>,
       <...>,
      <include-this-field-N-in-report>
   ],

   "filters":[
      {
         // value filter
         "fieldName":<name-of-the-field-to-filter-on>,
         "whitelist":[
            <acceptable-value-1>,
            <..>
            <acceptable-value-N>
         ]
      },
      {
         // value filter
         "fieldName":<name-of-the-field-to-filter-on>,
         "blacklist":[
            <non-acceptable-value-1>,
            <...>
            <non-acceptable-value-N>
         ]
      },
      {
         // range filter
         "fieldName":"<name-of-the-field-to-filter-on>",
         "range":{
            "min":<minimum-allowed-value>
            "max":<maximum-allowed-value>
         }
      }
   ],
   "sort":[
      {
         "order":"ASCENDING (or) DESCENDING",
         "fieldName":"<name-of-the-field-to-sort-by>"
      }
   ]
 }

Example request Json

.. highlight:: json

::

 {
   "name":"Failed, Stopped, Running, Succeeded runs - Jul 23, 2018 00:00am to Jul 24, 2018 21:00pm",
   "start":1532329200,
   "end":1532491200,
   "fields":[
      "artifactName",
      "applicationName",
      "program",
      "programType",
      "namespace",
      "status",
      "start",
      "end"
   ],
   "filters":[
      {
         "fieldName":"status",
         "whitelist":[
            "FAILED",
            "STOPPED",
            "RUNNING",
            "COMPLETED",
            "KILLED"
         ]
      },
      {
         "fieldName":"namespace",
         "whitelist":[
            "default"
         ]
      }
   ]
 }

On submitting this request, the backend asynchronously launches a spark job to process and generate the report.
The immediate response contains the report id.

.. highlight:: json

::

 {
    "id": <report-id>
 }

You can use this report-id to check the status of the report, if it is running, completed or failed using the `info` endpoint
and download completed reports to get information using `download` endpoint.

**Note** : Currently sort cannot be performed on more than one field.

Listing Reports
---------------

.. highlight:: console

::

 GET :
 /v3/namespaces/system/apps/ReportGenerationApp/spark/ReportGenerationSpark/methods/reports

.. list-table::
 :header-rows: 1
 :widths: 30 70

 * - Query Param
   - Description
 * - offset
   - offset to read reports from
 * - limit
   - limit on the number of reports to return

Response Json with explanation for fields

.. highlight:: console

::

 {
   "offset":<offset>,
   "limit":<limit>,
   "total":<total-reports-size-returned>,
   "reports":[
      {
         "id":<report-id>,
         "name":<report-name>,
         "created":<creation-time-seconds>,
         "expiry":<report-expiry-time-seconds>,
         "status":<report-status>
      },
      ...
   ]
 }

Example Response JSON

.. highlight:: json

::

 {
   "offset":0,
   "limit":20,
   "total":7,
   "reports":[
      {
         "id":"18a782b5-8fd4-11e8-ae71-acde48001122",
         "name":"Failed, Succeeded, Running, Stopped runs - Jul 24, 2018 22:29pm to Jul 24, 2018 23:29pm",
         "created":1532500174,
         "expiry":1532672986,
         "status":"COMPLETED"
      },
      ...
      {
         "id":"09ec25f4-8fd4-11e8-9d5d-acde48001122",
         "name":"Failed, Succeeded, Running, Stopped runs - Jul 24, 2018 22:29pm to Jul 24, 2018 23:29pm",
         "created":1532500149,
         "expiry":1532672965,
         "status":"COMPLETED"
      }
   ]
 }

Sharing Reports
---------------

On Authentication enabled clusters, reports are isolated by logged-in users who generated the reports,
If you wish, you can generate an encrypted share-id for your report, that can be used by others to access the report.

.. highlight:: console

::

 POST /v3/namespaces/system/apps/ReportGenerationApp/spark/ReportGenerationSpark/methods/reports/<report-id>/share

Response JSON:

.. highlight:: json

::

 {
    "shareId":<share-id>
 }


Viewing Report Status
----------------------

The info endpoint can be used to get the report status and summary for a report.
It can be queried either by providing the report-id or by providing a share-id through query param.

.. highlight:: console

::

 GET using Report-Id : /v3/namespaces/system/apps/ReportGenerationApp/spark/ReportGenerationSpark/methods/reports/info?report-id=<report-id>
 GET using Share-Id : /v3/namespaces/system/apps/ReportGenerationApp/spark/ReportGenerationSpark/methods/reports/info?share-id=<share-id>

.. highlight:: json

::

 {
   "request":{
      <report-generation-request>
   },
   "summary":{
      "namespaces":[
         {
            "namespace":"<namespace>",
            "runs":<runs-in-report-for-the-namespace>
         },
         ...
      ],
      "start":<start-time-seconds>,
      "end":<end-time-seconds>,
      "artifacts":[
         {
            "name":<artifactName>,
            "version":<artifactVersion>,
            "scope":<artifactScope,
            "runs":<runs-in-report-belonging-to-the-artifact>
         },
         ...
      ],
      "durations":{
         "min":<minimum-duration-of-runs-in-seconds>,
         "max":<max-duration-of-runs-in-seconds>,
         "average":<average-duration-of-runs-in-seconds>
      },
      "starts":{
         "oldest":<earliest-started-program-time>,
         "newest":<latest-started-program-time>
      },
      "owners":[
         {
            "user":<userName>,
            "runs":<runs-started-by-this-user>
         },
         ...
      ],
      "startMethods":[
         {
            "method":"MANUAL/SCHEDULED/TRIGGERED",
            "runs":<runs-started-by-this-start-method>
         },
         ...
      ],
      "recordCount":<total-records-in-report>,
      "creationTimeMillis":<time-when-report-was-created>,
      "expirationTimeMillis":<time-when-report-will-be-expired>
   },
   "name":<name-of-the-report>,
   "created":<report-creation-time-seconds>,
   "expiry":<report-expiry-time-seconds>,
   "status":<report-status>
 }

For reports that are currently running, the response does not contain a report summary

.. highlight:: json

::

 {
   "request":{
      <report-generation-request>
   },
   "name":<name-of-the-report>,
   "created":<creation-time>,
   "status":"RUNNING"
 }

For failed reports, the response contains the error information.

.. highlight:: json

::

 {
   "request":{
      <report-generation-request>
   },
   "error" : <error-message>,
   "name":<name-of-the-report>,
   "created":<creation-time>,
   "status":"FAILED"
 }

Downloading Report contents
----------------------------

The download endpoint can be used to download the report contents.
You can either provide a report id or a share id through a query parameter.

.. highlight:: console

::

 GET using report-id : /v3/namespaces/system/apps/ReportGenerationApp/spark/ReportGenerationSpark/methods/reports/download?report-id=<report-id>
 GET using share-id : /v3/namespaces/system/apps/ReportGenerationApp/spark/ReportGenerationSpark/methods/reports/download?share-id=<share-id>

.. highlight:: json

::

 {
   "offset":<offset>,
   "limit":<limit-of-records-to-return>,
   "total":<total-number-of-records-in-report>,
   "details":[
      {
         "field1":<value>,
         ...
         "fieldN":<value>
      },
    ...
   ]
 }

Example JSON response

.. highlight:: json

::

 {
   "offset":0,
   "limit":20,
   "total":25,
   "details":[
      {
         "duration":21,
         "program":"DataPipelineWorkflow",
         "programType":"Workflow",
         "applicationName":"testPipeline",
         "artifactName":"cdap-data-pipeline",
         "status":"COMPLETED",
         "end":1532499023,
         "start":1532499002,
         "namespace":"default"
      },
      ...
   ]
 }

Saving Reports
--------------
By default reports expire after 2 weeks, however they can be saved by using the following REST endpoint,

.. highlight:: console

::

 POST : /v3/namespaces/system/apps/ReportGenerationApp/spark/ReportGenerationSpark/methods/reports/<report-id>/save

with request body containing the name and description for saving the report

.. highlight:: json

::

 {
   "name" : <report-name>,
   "description" : <description-for-report>
 }

Deleting Reports
----------------
Unneeded reports (whether generated or failed) can be deleted by making a call to the following endpoint

.. highlight:: console

::

 DELETE : /v3/namespaces/system/apps/ReportGenerationApp/spark/ReportGenerationSpark/methods/reports/<report-id>
