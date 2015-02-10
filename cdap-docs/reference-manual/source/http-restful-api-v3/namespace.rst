.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-namespace:
.. _http-restful-api-v3-namespace:

===========================================================
Namespace HTTP RESTful API
===========================================================

.. highlight:: console

Use the CDAP Namespace HTTP API to create, list or delete namespaces in the CDAP instance.

Namespaces, their use and examples, are described in the :ref:`Developers' Manual: Building Blocks
<>`.


Create a Namespace
------------------
To create a namespace, submit an HTTP PUT request::

  PUT <base-url>/<namespace-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace

HTTP Responses
..............
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results


Existing Namespaces
---------------------

To list all of the existing namespaces, issue an HTTP GET request::

  GET <base-url>

This will return a JSON String map that lists each namespace [ with its name and description].


Details of a Namespace
---------------------------------

For detailed information on a namespace, use::

  GET <base-url>/<namespace-id>

The information will be returned in the body of the response.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace

HTTP Responses
..............
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results


Delete a Namespace
------------------
To delete a Namespace together with all of its Flows, Procedures and MapReduce programs, submit an HTTP DELETE::

  DELETE <base-url>/<namespace-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace

**Note:** The ``<namespace-id>`` in this URL is the ...



Examples
........

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``PUT <base-url>/myNamespace``
   * - Description
     - Creates a new namespace, identified as *myNamespace*
   * - 
     - 
   * - HTTP Method
     - ``GET <base-url>``
   * - Description
     - Gets a list of all existing namespaces
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

