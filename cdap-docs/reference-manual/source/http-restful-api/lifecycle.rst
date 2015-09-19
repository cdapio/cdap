.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _http-restful-api-lifecycle:

==========================
Lifecycle HTTP RESTful API
==========================

Use the CDAP Lifecycle HTTP API to deploy or delete applications and manage the lifecycle of 
flows, MapReduce programs, workflows, workers, and custom services.

.. highlight:: console

.. _http-restful-api-lifecycle-create-app:

Create an Application
---------------------
To create an application, submit an HTTP PUT request::

  PUT <base-url>/namespaces/<namespace>/apps/<app-name>

The request body is a JSON object specifying the artifact to use to create the application,
and an optional application configuration. For example:
 
.. container:: highlight

  .. parsed-literal::
    |$| PUT <base-url>/namespaces/default/apps/purchaseWordCount -H "Content-Type: application/json" -d
    {
      "artifact": {
        "name": "WordCount",
        "version": "|release|",
        "scope": "user"
      },
      "config": {
        "stream": "purchaseStream"
      }
    } 

will create an application named ``purchaseWordCount`` from the example ``WordCount`` artifact. The application
will receive the specified config, which will configure the application to create a stream named
``purchaseStream`` instead of using the default stream name. 

Note that the ``Content-Type`` header must be set to ``application/json``. If not, the API will
revert to a deprecated API which expects the request body to contain the contents
of a JAR file. 

Update an Application
---------------------
To update an application, submit an HTTP POST request::

  POST <base-url>/namespaces/<namespace>/apps/<app-name>/update

The request body is a JSON object specifying the updated artifact version and the updated application
config. For example, a request body of:

.. container:: highlight

  .. parsed-literal::
    |$| POST <base-url>/namespaces/default/apps/purchaseWordCount -d 
    {
      "artifact": {
        "name": "WordCount",
        "version": "|release|",
        "scope": "user"
      },
      "config": {
        "stream": "logStream";
      }
    }

will update the ``purchaseWordCount`` application to use version |release| of the ``WordCount`` artifact,
and update the name of the stream to ``logStream``. If no artifact is given, the current artifact will be
used. Only changes to artifact version are supported; changes to the artifact name are not allowed. If no
config is given, the current config will be used. If the config key is present, the current config will be
overwritten by the config specified in the request.

Deploy an Artifact and Application
----------------------------------
To deploy an application from your local file system into the namespace *<namespace>*,
submit an HTTP POST request::

  POST <base-url>/namespaces/<namespace>/apps

with the name of the JAR file as a header::

  X-Archive-Name: <JAR filename>

This will add the JAR file as an artifact and then create an application from that artifact.
The archive name must be in the form ``<artifact-name>-<artifact-version>.jar``.
An optional header can supply a configuration object as a serialized JSON string:

::

  X-App-Config: <JSON Serialization String of the Configuration Object>

The application's content is the body of the request::

  <JAR binary content>

Invoke the same command to update an application to a newer version.
However, be sure to stop all of its flows, Spark and MapReduce programs before updating the application.

For an application that has a configuration class such as::

  public static class MyAppConfig extends Config {
    String streamName;
    String datasetName;
  }

we can deploy it with this RESTful call::

  POST <bas-url>/namespaces/<namespace>/apps
  -H "X-Archive-Name: <jar-name>"
  -H "X-App-Config: "{\"streamName\" : \"newStream\", \"datasetName\" : \"newDataset\" }"
  --data-binary "@<jar-location>"

Note that the ``X-App-Config`` header contains the JSON serialization string of the ``MyAppConfig`` object.

Deployed Applications
---------------------

To list all of the deployed applications in the namespace *<namespace>*, issue an HTTP
GET request::

  GET <base-url>/namespaces/<namespace>/apps

This will return a JSON String map that lists each application with its name and description.


Details of a Deployed Application
---------------------------------

For detailed information on an application that has been deployed in the namespace
*<namespace>*, use::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>

The information will be returned in the body of the response. It includes the name and description
of the application, the streams and datasets it uses, and all of its programs.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results


Delete an Application
---------------------
To delete an application |---| together with all of its flows, MapReduce or Spark
programs, services, workflows, schedules |---| submit an HTTP DELETE::

  DELETE <base-url>/namespaces/<namespace>/apps/<application-name>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<application-name>``
     - Name of the application to be deleted

**Note:** The ``<application-name>`` in this URL is the name of the application
as configured by the application Specification,
and not necessarily the same as the name of the JAR file that was used to deploy the application.
This does not delete the streams and datasets associated with the application
because they belong to the namespace, not the application.


Start, Stop, Status, and Runtime Arguments
------------------------------------------
After an application is deployed, you can start and stop its flows, MapReduce 
programs, workflows, workers, and custom services, and query for their status using HTTP POST and GET methods::

  POST <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/<operation>
  GET <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/status

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application being called
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``workers``, or ``services``
   * - ``<program-id>``
     - Name of the *flow*, *MapReduce*, *Spark*, *workflow*, or *custom service*
       being called
   * - ``<operation>``
     - One of ``start`` or ``stop``

You can retrieve the status of multiple programs from different applications and program types
using an HTTP POST method::

  POST <base-url>/namespaces/<namespace>/status

with a JSON array in the request body consisting of multiple JSON objects with these parameters:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``"appId"``
     - Name of the application being called
   * - ``"programType"``
     - One of ``flow``, ``mapreduce``, ``spark``, ``workflow`` or ``service``
   * - ``"programId"``
     - Name of the *flow*, *MapReduce*, *Spark*, *workflow*, or *custom service*
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

.. rubric::  Examples

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST <base-url>/namespaces/default/apps/HelloWorld/flows/WhoFlow/start``
   * - Description
     - Start a flow *WhoFlow* in the application *HelloWorld* in the namespace *default*
   * - 
     - 
   * - HTTP Method
     - ``POST <base-url>/namespaces/default/apps/Count/services/GetCounts/stop``
   * - Description
     - Stop the service *GetCounts* in the application *Count* in the namespace *default*
   * - 
     - 
   * - HTTP Method
     - ``GET <base-url>/namespaces/default/apps/HelloWorld/flows/WhoFlow/status``
   * - Description
     - Get the status of the flow *WhoFlow* in the application *HelloWorld* in the namespace *default*
   * - 
     - 
   * - HTTP Method
     - ``POST <base-url>/namespaces/default/status``
   * - HTTP Body
     - ``[{"appId": "MyApp", "programType": "flow", "programId": "MyFlow"},``
       ``{"appId": "MyApp2", "programType": "service", "programId": "MyService"}]``
   * - HTTP Response
     - ``[{"appId":"MyApp", "programType":"flow", "programId":"MyFlow", "status":"RUNNING", "statusCode":200},``
       ``{"appId":"MyApp2", "programType":"service", "programId":"MyService",``
       ``"error":"Program not found", "statusCode":404}]``
   * - Description
     - Attempt to get the status of the flow *MyFlow* in the application *MyApp* and of the service *MyService*
       in the application *MyApp2* in the namespace *default*

When starting an program, you can optionally specify runtime arguments as a JSON map in the request body::

  POST <base-url>/namespaces/default/apps/HelloWorld/flows/WhoFlow/start

with the arguments as a JSON string in the body::

  {"foo":"bar","this":"that"}

CDAP will use these these runtime arguments only for this single invocation of the
program.

.. topic::  **Note: Runtime Arguments RESTful API Deprecated**

    As of *CDAP v2.8.0*, *Runtime Arguments RESTful API* have been deprecated, pending removal in a later version.
    Replace all use of *Runtime Arguments RESTful API* with :ref:`Preferences RESTful API <http-restful-api-v3-preferences>`.
    *Preferences RESTful API* will have feature-parity with *Runtime Arguments RESTful API* as of the version in which
    *Runtime Arguments RESTful API* are removed.

To save the runtime arguments so that CDAP will use them every time you start the program,
issue an HTTP PUT with the parameter ``runtimeargs``::

  PUT <base-url>/namespaces/default/apps/HelloWorld/flows/WhoFlow/runtimeargs

with the arguments as a JSON string in the body::

  {"foo":"bar","this":"that"}

To retrieve the runtime arguments saved for an application's program, issue an HTTP GET 
request to the program's URL using the same parameter ``runtimeargs``::

  GET <base-url>/namespaces/default/apps/HelloWorld/flows/WhoFlow/runtimeargs

This will return the saved runtime arguments in JSON format.

Container Information
---------------------

To find out the address of an program's container host and the container’s debug port, you can query
CDAP for a flow or service’s live info via an HTTP GET method::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/live-info

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application being called
   * - ``<program-type>``
     - One of ``flows``, ``workers``, or ``services``
   * - ``<program-id>``
     - Name of the program (*flow* or *custom service*)

Example::

  GET <base-url>/namespaces/default/apps/WordCount/flows/WordCounter/live-info

The response is formatted in JSON; an example of this is shown in 
:ref:`CDAP Testing and Debugging. <developers:debugging-distributed>`


.. _http-restful-api-lifecycle-scale:

Scaling
-------

You can retrieve the instance count executing different components from various applications and
different program types using an HTTP POST method::

  POST <base-url>/namespaces/<namespace>/instances

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID

with a JSON array in the request body consisting of multiple JSON objects with these parameters:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``"appId"``
     - Name of the application being called
   * - ``"programType"``
     - One of ``flow``, ``service``, or ``worker``
   * - ``"programId"``
     - Name of the program (*flow*, *service*, or *worker*) being called
   * - ``"runnableId"``
     - Name of the *flowlet*, only required if the program type is ``flow``

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

.. rubric:: Example

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST <base-url>/namespaces/default/instances``
   * - HTTP Body
     - ``[{"appId":"MyApp1","programType":"Flow","programId":"MyFlow1","runnableId":"MyFlowlet5"},``
       ``{"appId":"MyApp3","programType":"Service","programId":"MySvc1,"runnableId":"MyHandler1"}]``
   * - HTTP Response
     - ``[{"appId":"MyApp1","programType":"Flow","programId":"MyFlow1",``
       ``"runnableId":"MyFlowlet5","provisioned":2,"requested":2,"statusCode":200},``
       ``{"appId":"MyApp3","programType":"Service","programId":"MySvc1,``
       ``"runnableId":"MyHandler1","statusCode":404,"error":"Runnable: MyHandler1 not found"}]``
   * - Description
     - Try to get the instances of the flowlet *MyFlowlet5* in the flow *MyFlow1* in the
       application *MyApp1*, and the service handler *MyHandler1* in the user service
       *MySvc1* in the application *MyApp3*, all in the namespace *default*

.. _rest-scaling-flowlets:

Scaling Flowlets
................
You can query and set the number of instances executing a given flowlet
by using the ``instances`` parameter with HTTP GET and PUT methods::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances
  PUT <base-url>/namespaces/<namespace>/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application being called
   * - ``<flow-id>``
     - Name of the flow
   * - ``<flowlet-id>``
     - Name of the flowlet
   * - ``<quantity>``
     - Number of instances to be used

.. rubric:: Examples

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/namespaces/default/apps/HelloWorld/flows/WhoFlow/flowlets/saver/``
       ``instances``
   * - Description
     - Find out the number of instances of the flowlet *saver*
       in the flow *WhoFlow* of the application *HelloWorld* in the namespace *default*

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``PUT <base-url>/namespaces/default/apps/HelloWorld/flows/WhoFlow/flowlets/saver/``
       ``instances``

       with the arguments as a JSON string in the body::

         { "instances" : 2 }

   * - Description
     - Change the number of instances of the flowlet *saver* in the flow *WhoFlow* of the
       application *HelloWorld* in the namespace *default*

Scaling Services
................
You can query or change the number of instances of a service
by using the ``instances`` parameter with HTTP GET or PUT methods::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/services/<service-id>/instances
  PUT <base-url>/namespaces/<namespace>/apps/<app-id>/services/<service-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<service-id>``
     - Name of the service
   * - ``<quantity>``
     - Number of instances to be used

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/namespaces/default/apps/PurchaseHistory/services/CatalogLookup/instances``
   * - Description
     - Retrieve the number of instances of the service *CatalogLookup* in the application
       *PurchaseHistory* in the namespace *default*

Scaling Workers
...............
You can query or change the number of instances of a worker by using the ``instances``
parameter with HTTP GET or PUT methods::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/workers/<worker-id>/instances
  PUT <base-url>/namespaces/<namespace>/apps/<app-id>/workers/<worker-id>/instances

with the arguments as a JSON string in the body::

  { "instances" : <quantity> }

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<worker-id>``
     - Name of the worker
   * - ``<quantity>``
     - Number of instances to be used

Example
.......
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/namespaces/default/apps/HelloWorld/workers/DataWorker/instances``
       ``instances``
   * - Description
     - Retrieve the number of instances of the worker *DataWorker*
       in the application *HelloWorld* in the namespace *default*

.. _rest-program-runs:

Run Records and Schedules
-------------------------

To see all the runs of a selected program (flows, MapReduce programs, Spark programs, workflows, and
services), issue an HTTP GET to the program’s URL with the ``runs`` parameter.
This will return a JSON list of all runs for the program, each with a start time,
end time and program status::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/runs

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows`` or ``services``
   * - ``<program-id>``
     - Name of the program

You can filter the runs by the status of a program, the start and end times, 
and can limit the number of returned records:

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

The result returned will include the *runid* field, a UUID that uniquely identifies a run within CDAP,
with the start and end times in seconds since the start of the Epoch (midnight 1/1/1970).
Use that runid in subsequent calls to obtain additional information.

.. container:: table-block-example

  .. list-table::
     :widths: 99 1
     :stub-columns: 1

     * - Example: Retrieving Run Records
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table

     * - Description
       - Retrieve the run records of the flow *WhoFlow* of the application *HelloWorld*
      
     * - HTTP Method
       - ``GET <base-url>/namespaces/default/apps/HelloWorld/flows/WhoFlow/runs``
         
     * - Returns
       - | ``{"runid":"...","start":1382567598,"status":"RUNNING"},``
         | ``{"runid":"...","start":1382567447,"end":1382567492,"status":"STOPPED"},``
         | ``{"runid":"...","start":1382567383,"end":1382567397,"status":"STOPPED"}``


Retrieving Specific Run Information
...................................

To fetch the run record for a particular run of a program, use::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/runs/<run-id>


.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows`` or ``services``
   * - ``<program-id>``
     - Name of the program
   * - ``<run-id>``
     - Run id of the run


.. container:: table-block-example

  .. list-table::
     :widths: 99 1
     :stub-columns: 1

     * - Example: Retrieving A Particular Run Record
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table

     * - Description
       - Retrieve the run record of the flow *WhoFlow* of the application *HelloWorld* for run *b78d0091-da42-11e4-878c-2217c18f435d*
      
     * - HTTP Method
       - ``GET <base-url>/namespaces/default/apps/HelloWorld/flows/WhoFlow/runs/b78d0091-da42-11e4-878c-2217c18f435d``
         
     * - Returns
       - | ``{"runid":"...","start":1382567598,"status":"RUNNING"}``


For services, you can retrieve:

- the history of successfully completed Twill service runs using::

    GET <base-url>/namespaces/<namespace>/apps/<app-id>/services/<service-id>/runs?status=completed

For workflows, you can retrieve:

- the information about the currently running node(s) in the workflow:

  .. container:: table-block-example

    .. list-table::
       :widths: 99 1
       :stub-columns: 1

       * - Note: Workflow Current Node(s) RESTful API Deprecated
         - 

    .. list-table::
       :widths: 100
       :class: triple-table

       * - As of **CDAP v3.1.0**, the *Workflow Current Node(s) RESTful API* has been
           deprecated, pending removal in a later version.
       * - Replace all use of the *Workflow Current Node(s) RESTful API*::
           
             GET <base-url>/namespaces/<namespace>/apps/<app-id>/workflows/<workflow-id>/<run-id>/current

           with the revised API shown below for the *currently running node(s) of the workflow.*
           Note the addition of a ``/runs/`` component in the path::

             GET <base-url>/namespaces/<namespace>/apps/<app-id>/workflows/<workflow-id>/runs/<run-id>/current

- the schedules defined for a workflow (using the parameter ``schedules``)::

    GET <base-url>/namespaces/<namespace>/apps/<app-id>/workflows/<workflow-id>/schedules

- the next time that the workflow is scheduled to run (using the parameter ``nextruntime``)::

    GET <base-url>/namespaces/<namespace>/apps/<app-id>/workflows/<workflow-id>/nextruntime


.. rubric:: Examples

.. container:: table-block-example

  .. list-table::
     :widths: 99 1
     :stub-columns: 1

     * - Example: Retrieving The Most Recent Run
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table

     * - Description
       - Retrieve the most recent successful completed run of the service *CatalogLookup* of the application *PurchaseHistory*
      
     * - HTTP Method
       - ``GET <base-url>/namespaces/default/apps/PurchaseHistory/services/CatalogLookup/runs?status=completed&limit=1``
         
     * - Returns
       - | ``[{"runid":"cad83d45-ecfb-4bf8-8cdb-4928a5601b0e","start":1415051892,"end":1415057103,"status":"STOPPED"}]``


.. container:: table-block-example

  .. list-table::
     :widths: 99 1
     :stub-columns: 1

     * - Example: Retrieving a Schedule
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table

     * - Description
       - Retrieves the schedules of the workflow *PurchaseHistoryWorkflow* of the application *PurchaseHistory*
      
     * - HTTP Method
       - ``GET <base-url>/namespaces/default/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/schedules``
         
     * - Returns
       - | ``[{"schedule":{"name":"DailySchedule","description":"DailySchedule with crontab 0 4 * * *","cronEntry":"0 4 * * *"},``
         | `` "program":{"programName":"PurchaseHistoryWorkflow","programType":"WORKFLOW"},"properties":{}}]``
         

.. container:: table-block-example

  .. list-table::
     :widths: 99 1
     :stub-columns: 1

     * - Example: Retrieving The Next Runtime
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table

     * - Description
       - Retrieves the next runtime of the workflow *PurchaseHistoryWorkflow* of the application *PurchaseHistory*
      
     * - HTTP Method
       - ``GET <base-url>/namespaces/default/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/nextruntime``
         
     * - Returns
       - | ``[{"id":"DEFAULT.WORKFLOW:developer:PurchaseHistory:PurchaseHistoryWorkflow:0:DailySchedule","time":1415102400000}]``
       

Schedules: Suspend and Resume
.............................

For schedules, you can suspend and resume them using the RESTful API.

To *suspend* a schedule means that the program associated with that schedule will not
trigger again until the schedule is resumed.

To *resume* a schedule means that the trigger is reset, and the program associated will
run again at the next scheduled time.

To suspend or resume a schedule use::

  POST <base-url>/namespaces/<namespace>/apps/<app-id>/schedules/<schedule-name>/suspend
  POST <base-url>/namespaces/<namespace>/apps/<app-id>/schedules/<schedule-name>/resume

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<schedule-name>``
     - Name of the schedule

.. container:: table-block-example

  .. list-table::
     :widths: 99 1
     :stub-columns: 1

     * - Example: Suspending a Schedule
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table

     * - Description
       - Suspends the schedule *DailySchedule* of the application *PurchaseHistory*
      
     * - HTTP Method
       - ``POST <base-url>/namespaces/default/apps/PurchaseHistory/schedules/DailySchedule/suspend``
         
     * - Returns
       - | ``OK`` if successfully set as suspended


Workflows: Suspend and Resume
...........................................

For workflows, you can suspend and resume them using the RESTful API.

To *suspend* means that the current activity will be taken to completion, but no further 
programs will be initiated. Programs will not be left partially uncompleted, barring any errors.

In the case of a workflow with multiple MapReduce programs, if one of them is running (first of
three perhaps) and you suspend the workflow, that first MapReduce will be completed but the
following two will not be started.

To *resume* means that activity will start up where it was left off, beginning with the start
of the next program in the sequence.

In the case of the workflow mentioned above, resuming it after suspension would start up with the
second of the three MapReduce programs, which is where it would have left off when it was suspended.

With workflows, *suspend* and *resume* require a *run-id* as the action takes place on
either a currently running or suspended workflow.

To suspend or resume a workflow, use::
  
  POST <base-url>/namespaces/<namespace>/apps/<app-id>/workflows/<workflow-name>/runs/<run-id>/suspend
  POST <base-url>/namespaces/<namespace>/apps/<app-id>/workflows/<workflow-name>/runs/<run-id>/resume

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<workflow-name>``
     - Name of the workflow
   * - ``<run-id>``
     - UUID of the workflow run

.. container:: table-block-example

  .. list-table::
     :widths: 99 1
     :stub-columns: 1

     * - Example: Suspending A workflow
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table

     * - Description
       - Suspends the run ``0ce13912-e980-11e4-a7d7-8cae4cfd0e64`` of the workflow
         *PurchaseHistoryWorkflow* of the application *PurchaseHistory*
      
     * - HTTP Method
       - ``POST <base-url>/namespaces/default/apps/PurchaseHistory/workflows/PurchaseHistoryWorkflow/runs/0ce13912-e980-11e4-a7d7-8cae4cfd0e64/suspend``
         
     * - Returns
       - | ``Program run suspended.`` if successfully set as suspended
