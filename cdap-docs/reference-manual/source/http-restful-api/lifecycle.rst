.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _http-restful-api-lifecycle:

===========================================================
Lifecycle HTTP RESTful API
===========================================================

Use the CDAP Lifecycle HTTP API to deploy or delete Applications and manage the lifecycle of 
Flows, Procedures, MapReduce programs, Workflows, and Custom Services.

.. highlight:: console

Deploy an Application
---------------------
To deploy an Application from your local file system, submit an HTTP POST request::

  POST <base-url>/apps

with the name of the JAR file as a header::

  X-Archive-Name: <JAR filename>

and its content as the body of the request::

  <JAR binary content>

Invoke the same command to update an Application to a newer version.
However, be sure to stop all of its Flows, Procedures and MapReduce programs before updating the Application.


Deployed Applications
---------------------

To list all of the deployed applications, issue an HTTP GET request::

  GET <base-url>/apps

This will return a JSON String map that lists each Application with its name and description.


Details of a Deployed Application
---------------------------------

For detailed information on an application that has been deployed, use::

  GET <base-url>/apps/<app-id>

The information will be returned in the body of the response.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<app-id>``
     - Name of the Application

HTTP Responses
..............
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results


Delete an Application
---------------------
To delete an Application together with all of its Flows, Procedures and MapReduce programs, submit an HTTP DELETE::

  DELETE <base-url>/apps/<application-name>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<application-name>``
     - Name of the Application to be deleted

**Note:** The ``<application-name>`` in this URL is the name of the Application
as configured by the Application Specification,
and not necessarily the same as the name of the JAR file that was used to deploy the Application.
Also, this does not delete the Streams and Datasets associated with the Application
because they belong to your account, not the Application.


Start, Stop, Status, and Runtime Arguments
------------------------------------------
After an Application is deployed, you can start and stop its Flows, Procedures, MapReduce 
programs, Workflows, and Custom Services, and query for their status using HTTP POST and GET methods::

  POST <base-url>/apps/<app-id>/<program-type>/<program-id>/<operation>
  GET <base-url>/apps/<app-id>/<program-type>/<program-id>/status

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<app-id>``
     - Name of the Application being called
   * - ``<program-type>``
     - One of ``flows``, ``procedures``, ``mapreduce``, ``workflows`` or ``services``
   * - ``<program-id>``
     - Name of the *Flow*, *Procedure*, *MapReduce*, *Workflow*, or *Custom Service*
       being called
   * - ``<operation>``
     - One of ``start`` or ``stop``

You can retrieve the status of multiple programs from different applications and program types
using an HTTP POST method::

  POST <base-url>/status

with a JSON array in the request body consisting of multiple JSON objects with these parameters:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``"appId"``
     - Name of the Application being called
   * - ``"programType"``
     - One of ``flow``, ``procedure``, ``mapreduce``, ``workflow`` or ``service``
   * - ``"programId"``
     - Name of the *Flow*, *Procedure*, *MapReduce*, *Workflow*, or *Custom Service*
       being called

The response will be the same JSON array with additional parameters for each of the underlying JSON objects:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``"status"``
     - Maps to the status of an individual JSON object's queried program
       if the query is valid and the program was found.
   * - ``"statusCode"``
     - The status code from retrieving the status of an individual JSON object.
   * - ``"error"``
     - If an error, a description of why the status was not retrieved (the specified program was not found, etc.)

The ``status`` and ``error`` fields are mutually exclusive meaning if there is an error,
then there will never be a status and vice versa.

Examples
........

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST <base-url>/apps/HelloWorld/flows/WhoFlow/start``
   * - Description
     - Start a Flow *WhoFlow* in the Application *HelloWorld*
   * - 
     - 
   * - HTTP Method
     - ``POST <base-url>/apps/Count/procedures/GetCounts/stop``
   * - Description
     - Stop the Procedure *GetCounts* in the Application *Count*
   * - 
     - 
   * - HTTP Method
     - ``GET <base-url>/apps/HelloWorld/flows/WhoFlow/status``
   * - Description
     - Get the status of the Flow *WhoFlow* in the Application *HelloWorld*
   * - 
     - 
   * - HTTP Method
     - ``POST <base-url>/status``
   * - HTTP Body
     - ``[{"appId": "MyApp", "programType": "flow", "programId": "MyFlow"},``
       ``{"appId": "MyApp2", "programType": "procedure", "programId": "MyProcedure"}]``
   * - HTTP Response
     - ``[{"appId":"MyApp", "programType":"flow", "programId":"MyFlow", "status":"RUNNING", "statusCode":200},``
       ``{"appId":"MyApp2", "programType":"procedure", "programId":"MyProcedure",``
       ``"error":"Program not found", "statusCode":404}]``
   * - Description
     - Attempt to get the status of the Flow *MyFlow* in the Application *MyApp* and of the Procedure *MyProcedure*
       in the Application *MyApp2*

When starting an program, you can optionally specify runtime arguments as a JSON map in the request body::

  POST <base-url>/apps/HelloWorld/flows/WhoFlow/start

with the arguments as a JSON string in the body::

  {"foo":"bar","this":"that"}

CDAP will use these these runtime arguments only for this single invocation of the
program. To save the runtime arguments so that CDAP will use them every time you start the program,
issue an HTTP PUT with the parameter ``runtimeargs``::

  PUT <base-url>/apps/HelloWorld/flows/WhoFlow/runtimeargs

with the arguments as a JSON string in the body::

  {"foo":"bar","this":"that"}

To retrieve the runtime arguments saved for an Application's program, issue an HTTP GET 
request to the program's URL using the same parameter ``runtimeargs``::

  GET <base-url>/apps/HelloWorld/flows/WhoFlow/runtimeargs

This will return the saved runtime arguments in JSON format.

Container Information
---------------------

To find out the address of an program's container host and the container’s debug port, you can query
CDAP for a Procedure, Flow or Service’s live info via an HTTP GET method::

  GET <base-url>/apps/<app-id>/<program-type>/<program-id>/live-info

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<app-id>``
     - Name of the Application being called
   * - ``<program-type>``
     - One of ``flows``, ``procedures`` or ``services``
   * - ``<program-id>``
     - Name of the program (*Flow*, *Procedure* or *Custom Service*)

Example::

  GET <base-url>/apps/WordCount/flows/WordCounter/live-info

The response is formatted in JSON; an example of this is shown in 
:ref:`CDAP Testing and Debugging. <developers:debugging-distributed>`


.. _http-restful-api-lifecycle-scale:

Scaling
-------

You can retrieve the instance count executing different components from various applications and
different program types using an HTTP POST method::

  POST <base-url>/instances

with a JSON array in the request body consisting of multiple JSON objects with these parameters:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``"appId"``
     - Name of the Application being called
   * - ``"programType"``
     - One of ``flow``, ``procedure``, or ``service``
   * - ``"programId"``
     - Name of the program (*Flow*, *Procedure*, or *Custom Service*) being called
   * - ``"runnableId"``
     - Name of the *Flowlet* or *Service* if querying either a *Flow* or *User Service*. This parameter
       does not apply to *Procedures* because the ``programId`` is the same as the ``runnableId`` for a *Procedure*

The response will be the same JSON array with additional parameters for each of the underlying JSON objects:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``"requested"``
     - Number of instances the user requested for the program defined by the individual JSON object's parameters
   * - ``"provisioned"``
     - Number of instances that are actually running for the program defined by the individual JSON object's parameters.
   * - ``"statusCode"``
     - The status code from retrieving the instance count of an individual JSON object.
   * - ``"error"``
     - If an error, a description of why the status was not retrieved (the specified program was not found,
       the requested JSON object was missing a parameter, etc.)

**Note:** The ``requested`` and ``provisioned`` fields are mutually exclusive of the ``error`` field.

Example
.......

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST <base-url>/instances``
   * - HTTP Body
     - ``[{"appId":"MyApp1","programType":"Flow","programId":"MyFlow1","runnableId":"MyFlowlet5"},``
       ``{"appId":"MyApp1","programType":"Procedure","programId":"MyProc2"},``
       ``{"appId":"MyApp3","programType":"Service","programId":"MySvc1,"runnableId":"MyHandler1"}]``
   * - HTTP Response
     - ``[{"appId":"MyApp1","programType":"Flow","programId":"MyFlow1",``
       ``"runnableId":"MyFlowlet5","provisioned":2,"requested":2,"statusCode":200},``
       ``{"appId":"MyApp1","programType":"Procedure","programId":"MyProc2",``
       ``"provisioned":0,"requested":1,"statusCode":200},``
       ``{"appId":"MyApp3","programType":"Service","programId":"MySvc1,``
       ``"runnableId":"MyHandler1","statusCode":404,"error":"Runnable: MyHandler1 not found"}]``
   * - Description
     - Try to get the instances of the Flowlet *MyFlowlet5* in the Flow *MyFlow1* in the Application *MyApp1*, the
       Procedure *MyProc2* in the Application *MyApp1*, and the Service Handler *MyHandler1* in the
       User Service *MySvc1* in the Application *MyApp3*

.. _rest-scaling-flowlets:

Scaling Flowlets
................
You can query and set the number of instances executing a given Flowlet
by using the ``instances`` parameter with HTTP GET and PUT methods::

  GET <base-url>/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances
  PUT <base-url>/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<app-id>``
     - Name of the Application being called
   * - ``<flow-id>``
     - Name of the Flow
   * - ``<flowlet-id>``
     - Name of the Flowlet
   * - ``<quantity>``
     - Number of instances to be used

Examples
........
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/apps/HelloWorld/flows/WhoFlow/flowlets/saver/``
       ``instances``
   * - Description
     - Find out the number of instances of the Flowlet *saver*
       in the Flow *WhoFlow* of the Application *HelloWorld*

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``PUT <base-url>/apps/HelloWorld/flows/WhoFlow/flowlets/saver/``
       ``instances``

       with the arguments as a JSON string in the body::

         { "instances" : 2 }

   * - Description
     - Change the number of instances of the Flowlet *saver*
       in the Flow *WhoFlow* of the Application *HelloWorld*

Scaling Procedures
..................
In a similar way to `Scaling Flowlets`_, you can query or change the number of instances 
of a Procedure by using the ``instances`` parameter with HTTP GET and PUT methods::

  GET <base-url>/apps/<app-id>/procedures/<procedure-id>/instances
  PUT <base-url>/apps/<app-id>/procedures/<procedure-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<app-id>``
     - Name of the Application
   * - ``<procedure-id>``
     - Name of the Procedure
   * - ``<quantity>``
     - Number of instances to be used

Example
.......
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/apps/HelloWorld/procedures/Greeting/instances``
       ``instances``
   * - Description
     - Find out the number of instances of the Procedure *Greeting*
       in the Application *HelloWorld*

Scaling Services
................
You can query or change the number of instances of a Service
by using the ``instances`` parameter with HTTP GET or PUT methods::

  GET <base-url>/apps/<app-id>/services/<service-id>/runnables/<runnable-id>/instances
  PUT <base-url>/apps/<app-id>/services/<service-id>/runnables/<runnable-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<app-id>``
     - Name of the Application
   * - ``<service-id>``
     - Name of the Service
   * - ``<runnable-id>``
     - Name of the Service
   * - ``<quantity>``
     - Number of instances to be used

**Note:** In this release the ``runnable-id`` is the same as the ``service-id``.

Example
.......
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/apps/PurchaseHistory/services/CatalogLookup/runnables/CatalogLookup/instances``
   * - Description
     - Retrieve the number of instances of the Service *CatalogLookup* in the application *PurchaseHistory*.

.. _rest-program-runs:

Run Records and Schedules
-------------------------

To see all the runs of a selected program (Flows, Procedures, MapReduce programs, Spark programs, Workflows, and
Services), issue an HTTP GET to the program’s URL with the ``runs`` parameter.
This will return a JSON list of all runs for the program, each with a start time,
end time and program status::

  GET <base-url>/apps/<app-id>/<program-type>/<program-id>/runs

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<app-id>``
     - Name of the Application
   * - ``<program-type>``
     - One of ``flows``, ``procedures``, ``mapreduce``, ``spark``, ``workflows`` or ``services``
   * - ``<program-id>``
     - Name of the program

You can filter the runs either by the status of a program or the start and end times, 
and can limit the number of returned records.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Query Parameter
     - Description
   * - ``<status>``
     - running/completed/failed
   * - ``<start>``
     - start timestamp
   * - ``<end>``
     - end timestamp
   * - ``<limit>``
     - maximum number of returned records


Example
.......
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/apps/HelloWorld/flows/WhoFlow/runs``
   * - Description
     - Retrieve the run records of the Flow *WhoFlow* of the Application *HelloWorld*
   * - Returns

     - ``{"runid":"...","start":1382567598,"status":"RUNNING"},``
       ``{"runid":"...","start":1382567447,"end":1382567492,"status":"STOPPED"},``
       ``{"runid":"...","start":1382567383,"end":1382567397,"status":"STOPPED"}``

The *runid* field is a UUID that uniquely identifies a run within CDAP,
with the start and end times in seconds since the start of the Epoch (midnight 1/1/1970).

For Services, you can retrieve the history of successfully completed Twill Service using::

  GET <base-url>/apps/<app-id>/services/<service-id>/runs?status=completed

For Workflows, you can also retrieve:

- the schedules defined for a workflow (using the parameter ``schedules``)::

    GET <base-url>/apps/<app-id>/workflows/<workflow-id>/schedules

- the next time that the workflow is scheduled to run (using the parameter ``nextruntime``)::

    GET <base-url>/apps/<app-id>/workflows/<workflow-id>/nextruntime

Schedules can be suspended or resumed:

- to suspend a schedule::

    POST <base-url>/apps/<app-id>/schedules/<schedule-name>/suspend

- to resume a schedule::

    POST <base-url>/apps/<app-id>/schedules/<schedule-name>/resume

Example
.......
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/apps/PurchaseHistory/services/CatalogLookup/runs?status=completed&limit=1``
   * - Description
     - Retrieve the most recent successful completed run of the Service *CatalogLookup* of the Application *PurchaseHistory*
   * - Returns
     - ``[{"runid":"cad83d45-ecfb-4bf8-8cdb-4928a5601b0e","start":1415051892,"end":1415057103,"status":"STOPPED"}]``
   * - 
     - 
   * - HTTP Method
     - ``GET <base-url>/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/schedules``
   * - Description
     - Retrieves the schedules of the Workflow *PurchaseHistoryWorkflow* of the Application *PurchaseHistory*
   * - Returns
     - ``[{"schedule":{"name":"DailySchedule","description":"DailySchedule with crontab 0 4 * * *","cronEntry":"0 4 * * *"},``
              ``"program":{"programName":"PurchaseHistoryWorkflow","programType":"WORKFLOW"},"properties":{}}]``
   * - 
     - 
   * - HTTP Method
     - ``GET <base-url>/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/nextruntime``
   * - Description
     - Retrieves the next runtime of the Workflow *PurchaseHistoryWorkflow* of the Application *PurchaseHistory*
   * - Returns
     - ``[{"id":"DEFAULT.WORKFLOW:developer:PurchaseHistory:PurchaseHistoryWorkflow:0:DailySchedule","time":1415102400000}]``

