.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015-2017 Cask Data, Inc.

.. _http-restful-api-workflow:

=========================
Workflow HTTP RESTful API
=========================

.. highlight:: console

Use the CDAP Workflow HTTP RESTful API to access workflow tokens; list and delete local
datasets used in a workflow; retrieve the state of nodes that were executed; and retrieve
statistics across *successful* runs of a workflow.

Additional details and examples are found in the :ref:`Developers' Manual: Workflows <developers:workflows>`.

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


Workflow Tokens
===============
This interface supports accessing the value of workflow tokens set during runs of a workflow.
Returned values can be specified for a particular scope, node or key.

Obtaining a Token's Values
--------------------------
To retrieve the values that were put into the workflow token for a particular run,
use an HTTP GET request to the query's URL::

  GET /v3/namespaces/<namespace-id>/apps/{app-id}/workflows/{workflow-id}/runs/{run-id}[/nodes/{node-id}]/token

The request can (optionally) contain a *node-id* to limit the request to a particular node in workflow.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application
   * - ``workflow-id``
     - Name of the workflow
   * - ``run-id``
     - UUID of the workflow run
   * - ``node-id``
     - Name of a node in the workflow (optional)

.. rubric:: Extending the Request

The request can be extended with query parameters:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``scope``
     - *user* (default) returns the token for ``Scope.USER``; *system* returns the token for
       ``Scope.SYSTEM``
   * - ``key``
     - Returns the value for a specified key; if unspecified, returns all keys (optional)


.. rubric:: Comments

.. highlight:: json-ellipsis

If the query was successful, the body will contain the workflow token for a particular workflow
run, such as::

  {
    "key1": [
        {
          "nodeName": "node1",
          "value": "value1"
        }, {
          "nodeName": "node2",
          "value": "value2"
        }
      ],
    "key2": [
        {
            "nodeName": "node2",
            "value": "v2"
        }
      ]
  }

.. highlight:: console

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The query was successfully received and the workflow token was returned in the body
   * - ``404 Not Found``
     - The app, workflow, run-id, scope or key was not found
   * - ``500``
     - Internal server error

.. rubric:: Examples

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Request
     - ``GET /v3/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/runs/57c...75e2/token``
   * - HTTP Response
     - ``{"key1":[{"nodeName": "node1", "value":"1"}]}``
   * - Description
     - | Retrieves the token for a specific run of *PurchaseHistoryWorkflow*
       |

   * - HTTP Request
     - ``GET /v3/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/runs/57c...75e2/token?scope=system``
   * - HTTP Response
     - ``{"key1":[{"nodeName": "node1", "value":"1"}]}``
   * - Description
     - | Retrieves the token in the scope *System* for a specific run of *PurchaseHistoryWorkflow*
       |

   * - HTTP Request
     - ``GET /v3/namespaces/default/apps/Purchase/workflows/MyWorkflow/runs/57c...75e2/token?key=key1``
   * - HTTP Response
     - ``{"key1":[{"nodeName": "node1", "value":"1"}]}``
   * - Description
     - | Retrieves the values for the key "key1" from the token in the scope *User* for a specific run of *MyWorkflow*
       |

   * - HTTP Request
     - ``GET /v3/namespaces/default/apps/Purchase/workflows/MyWorkflow/runs/57c...75e2/nodes/MyExitNode/token?key=key1``
   * - HTTP Response
     - ``{"key1":[{"nodeName": "MyExitNode", "value":"1"}]}``
   * - Description
     - Retrieves the value for the key *key1* at node *MyExitNode* from the token in the scope *User* for a specific run of *MyWorkflow*


Workflow Local Datasets
=======================
This interface supports listing and deleting the local datasets associated with a particular workflow run.

Listing Local Datasets
----------------------
To retrieve the local datasets associated with a particular run, use an HTTP GET request to the query's URL::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/workflows/<workflow-id>/runs/<run-id>/localdatasets

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application
   * - ``workflow-id``
     - Name of the workflow
   * - ``run-id``
     - UUID of the workflow run

Deleting Local Datasets
-----------------------
To delete the local datasets associated with a particular run, use an HTTP DELETE request to the query's URL::

  DELETE /v3/namespaces/<namespace-id>/apps/<app-id>/workflows/<workflow-id>/runs/<run-id>/localdatasets

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application
   * - ``workflow-id``
     - Name of the workflow
   * - ``run-id``
     - UUID of the workflow run

Workflow State
==============
The workflow state interface allows you to retrieve the state of all nodes that were executed as
a part of a given workflow run.

To retrieve the state of a particular workflow run, use an HTTP GET request to the query's URL::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/workflows/<workflow-id>/runs/<run-id>/nodes/state

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application
   * - ``workflow-id``
     - Name of the workflow
   * - ``run-id``
     - UUID of the workflow run

.. rubric:: Example

The query::

  GET /v3/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/runs/53280521-0ce3-11e6-873e-da6a50dd7318/nodes/state

.. highlight:: json-ellipsis

would return results similar to these, pretty-printed for display::

  {
      "PurchaseHistoryBuilder": {
          "nodeId": "PurchaseHistoryBuilder",
          "nodeStatus": "COMPLETED",
          "runId": "53504da2-0ce3-11e6-92bb-da6a50dd7318"
      },
      "PurchaseReader": {
          "nodeId": "PurchaseReader",
          "nodeStatus": "COMPLETED"
      }
  }

.. highlight:: console


Workflow Statistics
===================
These requests provide statistics across *successful* runs of a workflow, in a time
interval, for a succession of runs, or between two specific runs. These requests will help
in determining which jobs are responsible for delays or problems.

Statistics of Successful Runs
-----------------------------
This request returns general statistics about all *successful* workflow runs in a particular time interval,
with an analysis based on a series of (optionally) provided percentiles::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/workflows/<workflow-id>/statistics?
    start=<start-time>&end=<end-time>&percentile=<percentile-1>&percentile=<percentile-2>...

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application
   * - ``workflow-id``
     - Name of the workflow
   * - ``start-time``
     - Start time of runs (in seconds); if not provided, defaults to ``now`` (optional)
   * - ``end-time``
     - End time of runs (in seconds); if not provided, defaults to ``now-1d`` (optional)
   * - ``percentile-1``
     - List of percentiles (each greater than zero and less than 100) to be used for generating statistics;
       if not provided, defaults to 90 (optional)

If the query was successful, the body will contain a JSON structure of statistics.

**Note:** When specifying start and end times, in addition to giving an absolute timestamp
in seconds, you can specify a relative time, using ``now`` minus an increment with units.
Examples: ``now-<n>s``, ``now-<n>m``,  ``now-<n>h``, or ``now-<n>d``.

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The query was successfully received and the statistics were returned in the body in a JSON format
   * - ``404 Not Found``
     - The app, workflow, run-id, scope or key was not found
   * - ``500``
     - Internal server error

.. rubric:: Example

The query::

  GET /v3/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/statistics?
    start=1441918778&end=1442005182&percentile=80&percentile=90&percentile=95&percentile=99

.. highlight:: json-ellipsis

would return results similar to these, pretty-printed for display::

  {
      "startTime": 0,
      "endTime": 1442008469,
      "runs": 4,
      "avgRunTime": 7.5,
      "percentileInformationList": [
          {
              "percentile": 80.0,
              "percentileTimeInSeconds": 9,
              "runIdsOverPercentile": [
                  "e0cc5b98-58cc-11e5-84a1-8cae4cfd0e64"
              ]
          },
          {
              "percentile": 90.0,
              "percentileTimeInSeconds": 9,
              "runIdsOverPercentile": [
                  "e0cc5b98-58cc-11e5-84a1-8cae4cfd0e64"
              ]
          },
          {
              "percentile": 95.0,
              "percentileTimeInSeconds": 9,
              "runIdsOverPercentile": [
                  "e0cc5b98-58cc-11e5-84a1-8cae4cfd0e64"
              ]
          },
          {
              "percentile": 99.0,
              "percentileTimeInSeconds": 9,
              "runIdsOverPercentile": [
                  "e0cc5b98-58cc-11e5-84a1-8cae4cfd0e64"
              ]
          }
      ],
      "nodes": {
          "PurchaseHistoryBuilder": {
              "avgRunTime": "7.0",
              "99.0": "8",
              "80.0": "8",
              "95.0": "8",
              "runs": "4",
              "90.0": "8",
              "type": "MapReduce"
          }
      }
  }

.. highlight:: console

Comparing a Run to Runs Before and After
----------------------------------------
This request returns a list of workflow metrics, based on a workflow run and a surrounding
number of *successful* runs of the workflow that are spaced apart by a time interval from
each other::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/workflows/<workflow-id>/runs/<run-id>/statistics?
    limit=<limit>&interval=<interval>

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application
   * - ``workflow-id``
     - Name of the workflow
   * - ``run-id``
     - UUID of the workflow run
   * - ``limit``
     - The number of the records to compare against (before and after) the run; if not provided, defaults to ``10`` (optional)
   * - ``interval``
     - The time interval with which to space out the runs before and after, with units; if not provided, defaults to ``10s`` (optional)

If the query was successful, the body will contain a JSON structure of statistics.

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The query was successfully received and the statistics were returned in the body in a JSON format
   * - ``404 Not Found``
     - The app, workflow, run-id, scope or key was not found
   * - ``500``
     - Internal server error

.. rubric:: Example

The query::

  GET /v3/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/runs/
    1873ade0-58d9-11e5-b79d-8cae4cfd0e64/statistics?limit=10&interval=10s'

.. highlight:: json-ellipsis

would return results similar to these, pretty-printed for display::

  {
      "startTimes": {
          "1dd36962-58d9-11e5-82ac-8cae4cfd0e64": 1442012523,
          "2523aa44-58d9-11e5-90fd-8cae4cfd0e64": 1442012535,
          "1873ade0-58d9-11e5-b79d-8cae4cfd0e64": 1442012514
      },
      "programNodesList": [
          {
              "programName": "PurchaseHistoryBuilder",
              "workflowProgramDetailsList": [
                  {
                      "workflowRunId": "1dd36962-58d9-11e5-82ac-8cae4cfd0e64",
                      "programRunId": "1e1c3233-58d9-11e5-a7ff-8cae4cfd0e64",
                      "programRunStart": 1442012524,
                      "metrics": {
                          "MAP_INPUT_RECORDS": 19,
                          "REDUCE_OUTPUT_RECORDS": 3,
                          "timeTaken": 9,
                          "MAP_OUTPUT_BYTES": 964,
                          "MAP_OUTPUT_RECORDS": 19,
                          "REDUCE_INPUT_RECORDS": 19
                      }
                  },
                  {
                      "workflowRunId": "1873ade0-58d9-11e5-b79d-8cae4cfd0e64",
                      "programRunId": "188a9141-58d9-11e5-88d1-8cae4cfd0e64",
                      "programRunStart": 1442012514,
                      "metrics": {
                          "MAP_INPUT_RECORDS": 19,
                          "REDUCE_OUTPUT_RECORDS": 3,
                          "timeTaken": 7,
                          "MAP_OUTPUT_BYTES": 964,
                          "MAP_OUTPUT_RECORDS": 19,
                          "REDUCE_INPUT_RECORDS": 19
                      }
                  }
              ],
              "programType": "Mapreduce"
          }
      ]
  }

.. highlight:: console

Comparing Two Runs
------------------
This request compares the metrics of two runs of a workflow::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/workflows/<workflow-id>/runs/<run-id>/compare?
    other-run-id=<other-run-id>

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application
   * - ``workflow-id``
     - Name of the workflow
   * - ``run-id``
     - UUID of the workflow run
   * - ``other-run-id``
     - UUID of the other workflow run to be used in the comparison

If the query was successful, the body will contain a JSON structure of statistics. Note that if either of
the run-ids is from an *unsuccessful* run, an error message will be returned::

  'The other run-id provided was not found : dbd59091-58cb-11e5-a7c6-8cae4cfd0e64' was not found

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The query was successfully received and the statistics were returned in the body in a JSON format
   * - ``404 Not Found``
     - The app, workflow, run-id, scope or key was not found
   * - ``500``
     - Internal server error

.. rubric:: Example

Comparing two runs (``14b8710a-58cd-11e5-98ca-8cae4cfd0e64`` and ``e0cc5b98-58cc-11e5-84a1-8cae4cfd0e64``)::

  GET /v3/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/
    runs/14b8710a-58cd-11e5-98ca-8cae4cfd0e64/compare?other-run-id=e0cc5b98-58cc-11e5-84a1-8cae4cfd0e64

.. highlight:: json-ellipsis

would return results similar to these, pretty-printed for display::

  [
      {
          "programName": "PurchaseHistoryBuilder",
          "workflowProgramDetailsList": [
              {
                  "workflowRunId": "14b8710a-58cd-11e5-98ca-8cae4cfd0e64",
                  "programRunId": "14c9d62b-58cd-11e5-9105-8cae4cfd0e64",
                  "programRunStart": 1442007354,
                  "metrics": {
                      "MAP_INPUT_RECORDS": 19,
                      "REDUCE_OUTPUT_RECORDS": 3,
                      "timeTaken": 7,
                      "MAP_OUTPUT_BYTES": 964,
                      "MAP_OUTPUT_RECORDS": 19,
                      "REDUCE_INPUT_RECORDS": 19
                  }
              },
              {
                  "workflowRunId": "e0cc5b98-58cc-11e5-84a1-8cae4cfd0e64",
                  "programRunId": "e1497ad9-58cc-11e5-9dfa-8cae4cfd0e64",
                  "programRunStart": 1442007268,
                  "metrics": {
                      "MAP_INPUT_RECORDS": 19,
                      "REDUCE_OUTPUT_RECORDS": 3,
                      "timeTaken": 8,
                      "MAP_OUTPUT_BYTES": 964,
                      "MAP_OUTPUT_RECORDS": 19,
                      "REDUCE_INPUT_RECORDS": 19
                  }
              }
          ],
          "programType": "Mapreduce"
      }
  ]

.. highlight:: console
