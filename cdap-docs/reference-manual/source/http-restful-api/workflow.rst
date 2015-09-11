.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-workflow:

=========================
Workflow HTTP RESTful API
=========================

.. highlight:: console

Workflow Tokens
===============

This interface supports accessing the value of workflow tokens set during runs of a workflow.
Returned values can be specified for a particular scope, node or key.

Obtaining a Token's Values
--------------------------
To get the values that were put into the workflow token for a particular run, 
usen an HTTP GET request to the query's URL::

  GET <base-url>/namespaces/<namespace>/apps/{app-id}/workflows/{workflow-id}/runs/{run-id}[/nodes/{node-id}]/token
  
The request can (optionally) contain a *node-id* to limit the request to a particular node in workflow.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<workflow-id>``
     - Name of the workflow
   * - ``<run-id>``
     - UUID of the workflow run
   * - ``<node-id>``
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
     - Returns the value for a specified key; if unspecified, returns all keys.


.. rubric:: Comments

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

Examples
--------

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Request
     - ``GET <base-url>/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/runs/57c...75e2/token``
   * - HTTP Response
     - ``{"key1":[{"nodeName": "node1", "value":"1"}]}``
   * - Description
     - | Retrieves the token for a specific run of *PurchaseHistoryWorkflow* 
       |

   * - HTTP Request
     - ``GET <base-url>/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/runs/57c...75e2/token?scope=system``
   * - HTTP Response
     - ``{"key1":[{"nodeName": "node1", "value":"1"}]}``
   * - Description
     - | Retrieves the token in the scope *System* for a specific run of *PurchaseHistoryWorkflow*
       |

   * - HTTP Request
     - ``GET <base-url>/namespaces/default/apps/Purchase/workflows/MyWorkflow/runs/57c...75e2/token?key=key1``
   * - HTTP Response
     - ``{"key1":[{"nodeName": "node1", "value":"1"}]}``
   * - Description
     - | Retrieves the values for the key "key1" from the token in the scope *User* for a specific run of *MyWorkflow*
       |
       
   * - HTTP Request
     - ``GET <base-url>/namespaces/default/apps/Purchase/workflows/MyWorkflow/runs/57c...75e2/nodes/MyExitNode/token?key=key1``
   * - HTTP Response
     - ``{"key1":[{"nodeName": "MyExitNode", "value":"1"}]}``
   * - Description
     - Retrieves the value for the key *key1* at node *MyExitNode* from the token in the scope *User* for a specific run of *MyWorkflow*


Workflow Statistics
===================
These requests provide statistics across *successful* runs of a Workflow in a time
interval. This will helps in detecting which jobs might be responsible for delays or
problems.

General Statistics of a Workflow program across all Successful Runs
-------------------------------------------------------------------

This request returns basic statistics about all *successful* workflow runs in a particular time interval, 
with an analysis (optionally) based on a series of provided percentiles::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/workflows/<workflow-id>/statistics?
    start=<start-time>&end=<end-time>&percentile=<percentile-1>&percentile=<percentile-2>...
    
where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<workflow-id>``
     - Name of the workflow
   * - ``<start-time>``
     - Start time of runs; default is ``now`` (optional)
   * - ``<end-time>``
     - End time of runs; default is ``now-1d`` (optional)
   * - ``<percentile-1>``
     - List of percentiles (each greater than zero and less than 100) to be used for generating statistics;
       if not provided, defaults to 90 (optional)

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

Example
-------
The query

::

  GET <base-url>/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/statistics?
    start=1441918778&end=1442005182&percentile=80&percentile=90&percentile=95&percentile=99
  
could return results similar to these, pretty-printed for display::
    
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
  
Statistics comparing one Run to other Runs both before and after
----------------------------------------------------------------

This request returns a list of workflow metrics, based on a workflow run and a surrounding number of runs
of the workflow that are spaced apart by a time interval from each other::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/workflows/<workflow-id>/runs/<run-id>/statistics?
    limit=<limit>&interval=<interval>
    
where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<workflow-id>``
     - Name of the workflow
   * - ``<run-id>``
     - UUID of the workflow run
   * - ``<limit>``
     - The number of the records to compare against (before and after) the run
   * - ``<interval>``
     - The time interval with which to space out the runs before and after

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

Example
-------

  GET <base-url>/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/runs/[run-id]
    statistics?limit=20&interval=[]
  
  
could return results similar to these, pretty-printed for display::

  {
    "start_time": 1439200000,
    "end_time":  1439279804,
    "num_runs": 20,
    "nodes": {
       "node1" : {
        "name": "PurchaseEventParser",
        "p2": {  "runid" : "13fdssvqe423",
                 "run_times_seconds": 15,
                 "mappers": 4,
                 "reducers": 3,
                 "file_read_data_bytes": 800,
                 "file_write_data_bytes": 900,
                 "file_read_data_ops": 80,
                 "file_write_data_ops": 90,
                 "file_large_read_data_ops": 8,
                 "hdfs_read_data_bytes": 700,
                 "hdfs_write_data_bytes": 800,
                 "hdfs_read_data_ops": 70,
                 "hdfs_write_data_ops": 80,
                 "hdfs_read_data_ops": 5,
        "total_time_spnt_map_tasks_seconds": 100,
        "total_time_spnt_red_tasks_seconds": 140
                },
        "p1": {
                 "runid" : "245fdasfsdaw5ee",
                 "run_time_seconds": 20,
                 "mappers": 7,
                 "reducers": 8,
                 "file_read_data_bytes": 800,
                 "file_write_data_bytes": 900,
                 "file_read_data_ops": 80,
                 "file_write_data_ops": 90,
                 "file_large_read_data_ops": 8,
                 "hdfs_read_data_bytes": 700,
                 "hdfs_write_data_bytes": 800,
                 "hdfs_read_data_ops": 70,
                 "hdfs_write_data_ops": 80,
                 "hdfs_read_data_ops": 5,
        "total_time_spnt_by_map_tasks_seconds": 11,
        "total_time_spnt_by_red_tasks_seconds": 9
       },
        "current":{},
        "n1": {},
        "n2": {}   
      },
     "node2" : {"name": "PurchaseSpark",
                    "p2": {},
                    "p1": {},
                    "curr": {},
                    "n1": {},
                    "n2": {}
        }
   }
  }

Statistics comparing two runs against each other
------------------------------------------------

Compare the metrics of two runs of a workflow::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/workflows/<workflow-id>/runs/<run-id>/compare?
    other-run-id=<other-run-id>
    
where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<workflow-id>``
     - Name of the workflow
   * - ``<run-id>``
     - UUID of the workflow run
   * - ``<other-run-id>``
     - UUID of the other workflow run to be used in the comparison

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

Example
-------

  GET <base-url>/namespaces/default/apps/Purchase/workflows/PurchaseHistoryWorkflow/run-id/[run-id]/
    compare?other-run-id=<other-run-id>
  
  
could return results similar to these, pretty-printed for display::

[{"node1":[
{"run1": {
      "name": "PurchaseEventParser",
      "type": "MapReduce",
      "runid" : "13fdssvqe423",
      "run_time_seconds": 15,
       "mappers": 4,
       "reducers": 3,
       "file_read_data_bytes": 800,
       "file_write_data_bytes": 900,
       "file_read_data_ops": 80,
       "file_write_data_ops": 90,
       "file_large_read_data_ops": 8,
       "hdfs_read_data_bytes": 700,
       "hdfs_write_data_bytes": 800,
       "hdfs_read_data_ops": 70,
       "hdfs_write_data_ops": 80,
       "hdfs_read_data_ops": 5,
   "total_time_spnt_by_map_tasks_seconds": 100,
       "total_time_spnt_by_red_tasks_seconds": 140
    }},
{"run2": {
      "name": "PurchaseEventParser",
      "type": "MapReduce",
      "run_time_seconds": 50,
      "mappers": 10,
      "reducers": 10,
      "file_read_data_bytes": 800,
       "file_write_data_bytes": 900,
       "file_read_data_ops": 80,
       "file_write_data_ops": 90,
       "file_large_read_data_ops": 8,
       "hdfs_read_data_bytes": 700,
       "hdfs_write_data_bytes": 800,
       "hdfs_read_data_ops": 70,
       "hdfs_write_data_ops": 80,
       "hdfs_read_data_ops": 5,
      "total_time_spnt_by_map_tasks_seconds": 20,
      "total_time_spnt_by_red_tasks_seconds": 30
      }}
]},
{"node2":[
{"run1": {
       "name": "PurchaseSpark",
       "type": "Spark",
       "executors" : 2,
       "driver_running_stages": 4,
       "driver_mem_used_megabytes": 17,
       "driver_disk_space_used_megabytes": 12,
       "driver_all_jobs": 7,
       "driver_failed_stages": 2,
       "filesystem.file_write_ops": 1500,
       "filesystem.file_read_ops": 1000,
        "filesystem.file_large_read_ops": 100,
       "filesystem.file_write_bytes": 15000,
       "filesystem.file_read_bytes": 10000,
       "hdfs.file_write_ops": 1500,
       "hdfs.file_read_ops": 1000,
       "hdfs.file_large_read_ops": 100,
       "hdfs.file_write_bytes": 15000,
       "hdfs.file_read_bytes": 10000,
       "current_pool_size": 5,
       "max_pool_size": 8
    }},
{"run2" : {}}]
}
]
