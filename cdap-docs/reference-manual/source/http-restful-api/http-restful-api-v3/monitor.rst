.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _http-restful-api-monitor:

===========================================================
Monitor HTTP RESTful API
===========================================================

.. highlight:: console

CDAP internally uses a variety of System Services that are critical to its functionality.
This section describes the RESTful APIs that can be used to see into System Services.

Details of All Available System Services
----------------------------------------

For the detailed information of all available System Services, use::

  GET <base-url>/system/services

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results

Checking Status of All CDAP System Services
-------------------------------------------
To check the status of all the System Services, use::

  GET <base-url>/system/services/status

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results

Checking Status of a Specific CDAP System Service
-------------------------------------------------
To check the status of a specific System Service, use::

  GET <base-url>/system/services/<service-name>/status

The status of these CDAP System Services can be checked:

.. list-table::
   :header-rows: 1
   :widths: 25 25 50
   
   * - Service 
     - Service-Name
     - Description of the Service
   * - ``Metrics``
     - ``metrics``
     - Service that handles metrics related HTTP requests
   * - ``Transaction``
     - ``transaction``
     - Service that handles transactions
   * - ``Streams``
     - ``streams``
     - Service that handles Stream management
   * - ``App Fabric``
     - ``appfabric``
     - Service that handles Application Fabric requests
   * - ``Log Saver``
     - ``log.saver``
     - Service that aggregates all system and application logs
   * - ``Metrics Processor``
     - ``metrics.processor``
     - Service that aggregates all system and application metrics 
   * - ``Dataset Executor``
     - ``dataset.executor``
     - Service that handles all data-related HTTP requests 
   * - ``Explore Service``
     - ``explore.service``
     - Service that handles all HTTP requests for ad-hoc data exploration

**Note:** The Service status checks are more useful when CDAP is running in a distributed cluster mode.

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The service is up and running
   * - ``404 Not Found``
     - The service is either not running or not found

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1
   
   * - HTTP Method
     - ``GET <base-url>/system/services/metrics/status``
   * - Description
     - Returns the status of the Metrics Service

Scaling System Services
-----------------------
In distributed CDAP installations, the number of instances for system services 
can be queried and changed by using these commands::

  GET <base-url>/system/services/<service-name>/instances
  PUT <base-url>/system/services/<service-name>/instances

with the arguments as a JSON string in the body::

        { "instances" : <quantity> }

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<system-name>``
     - Name of the system service 
   * - ``<quantity>``
     - Number of instances to be used
     
**Note:** In standalone CDAP, trying to set the instances of system services will return a Status Code ``400 Bad Request``.

.. rubric:: Examples
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/system/services/metrics/instances``
   * - Description
     - Determine the number of instances being used for the metrics HTTP service 
   * - 
     - 
   * - HTTP Method
     - ``PUT <base-url>/system/services/metrics/instances``
       ``instances``

       with the arguments as a JSON string in the body::

          { "instances" : 2 }

   * - Description
     - Sets the number of instances of the metrics HTTP service to 2
