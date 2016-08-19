.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _http-restful-api-monitor:

========================
Monitor HTTP RESTful API
========================

.. highlight:: console

Use the CDAP Monitor HTTP RESTful API to examine the CDAP system services used internally by CDAP.
Additional details on monitoring can be found in the :ref:`Administration Manual: Logging
and Monitoring <logging-monitoring>`.

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


Listing all System Services and their Details
=============================================

For the detailed information of all available CDAP system services, use::

  GET /v3/system/services

The response body will contain a JSON-formatted list of the existing system services::

  [
      {
          "name": "appfabric",
          "description": "Service for managing application lifecycle.",
          "status": "OK",
          "logs": "OK",
          "min": 1,
          "max": 1,
          "requested": 1,
          "provisioned": 1
      }
      ...
  ]
  
See :ref:`downloading System Logs <http-restful-api-logging_downloading_system_logs>` for
information and an example of using these system services.

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results


Checking the Status of all System Services
==========================================
To check the status of all the CDAP system services, use::

  GET /v3/system/services/status

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results


Checking the Status of a System Service
=======================================
To check the status of a specific CDAP system service, use::

  GET /v3/system/services/<service-id>/status

The status of these CDAP system services can be checked:

.. list-table::
   :header-rows: 1
   :widths: 25 25 50
   
   * - Service
     - Service ID
     - Description of the service
   * - ``Metrics``
     - ``metrics``
     - Service that handles metrics related HTTP requests
   * - ``Transaction``
     - ``transaction``
     - Service that handles transactions
   * - ``Streams``
     - ``streams``
     - Service that handles stream management
   * - ``App Fabric``
     - ``appfabric``
     - Service that handles application fabric requests
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

**Note:** The service status checks are more useful when CDAP is running in a distributed cluster mode.

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
     - ``GET /v3/system/services/metrics/status``
   * - Description
     - Returns the status of the metrics service


Container Information of a System Service
=========================================
If you are trying to debug a CDAP system service, you can retrieve container info for a system service with::

  GET /v3/system/services/<service-id>/live-info
  
where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``service-id``
     - Name (ID) of the system service
     
**Note:** This returns useful information only for Distributed CDAP installations.


Restarting System Service Instances
===================================
To restart all instances of a CDAP system service, you can issue an HTTP POST request to the URL::

  POST /v3/system/services/<service-id>/restart

You can restart a particular instance of a system service in CDAP, using its instance id, by issuing
an HTTP POST request to the URL::

  POST /v3/system/services/<service-id>/instances/<instance-id>/restart

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``service-id``
     - Name (ID) of the system service whose instances are to be restarted
   * - ``instance-id``
     - Specific instance of a service that needs to be restarted;
       ``instance-id`` runs from 0 to (the number of instances-per-service -1)

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``403 Bad Request``
     - The service is unavailable because it was not enabled
   * - ``404 Service not found``
     - The service name is not valid
   * - ``500 Internal error``
     - Internal error encountered when processing the request
   * - ``503 Service Unavailable``
     - The service is unavailable. For example, it may not yet have been started

To retrieve details of the last restart attempt made for a particular service, issue an HTTP GET request to the URL::

  GET /v3/system/services/<service-id>/latest-restart

The response body will contain a JSON-formatted status of the last restart attempt for that service::

  {
      "instanceIds":[0],
      "serviceName":"dataset.executor",
      "startTimeInMs":1437070039984,
      "endTimeInMs":1437070039992,
      "status":"SUCCESS"}
  }

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``service-id``
     - Name (ID) of the system service for which details of last restart are to be retrieved

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``404 Service not found``
     - The service name is not valid
   * - ``500 Internal error``
     - Internal error encountered when processing the request

.. _http-restful-api-monitor-scaling-system-services:

Scaling System Services
=======================
In distributed CDAP installations, the number of instances for CDAP system services 
can be queried and changed by using these commands::

  GET /v3/system/services/<service-id>/instances
  PUT /v3/system/services/<service-id>/instances

with the arguments as a JSON string in the body::

        { "instances" : <quantity> }

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``service-id``
     - Name (ID) of the system service 
   * - ``quantity``
     - Number of instances to be used
     
**Note:** In standalone CDAP, trying to set the instances of system services will return a Status Code ``400 Bad Request``.

.. rubric:: Examples
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET /v3/system/services/metrics/instances``
   * - Description
     - Determine the number of instances being used for the metrics HTTP service 
   * - 
     - 
   * - HTTP Method
     - ``PUT /v3/system/services/metrics/instances``
       ``instances``

       with the arguments as a JSON string in the body::

          { "instances" : 2 }

   * - Description
     - Sets the number of instances of the metrics HTTP service to 2
