.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-workflow:

=========================
Workflow HTTP RESTful API
=========================

.. highlight:: console

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
 