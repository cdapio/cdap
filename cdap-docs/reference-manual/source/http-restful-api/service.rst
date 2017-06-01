.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _http-restful-api-service:

========================
Service HTTP RESTful API
========================

.. highlight:: console

Use the CDAP Service HTTP RESTful API to list all services and making requests to the
methods of an application’s services. See the :ref:`http-restful-api-lifecycle` for how to
control the lifecycle of services.

For system services, see the :ref:`http-restful-api-monitor` and its methods.

See the :ref:`Route Config HTTP RESTful API <http-restful-api-route-config>` for
allocating requests between different versions of a service.

Additional details and examples are found in the :ref:`Developer Manual: Services <developer:user-services>`.

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


.. _http-restful-api-service-listing:

Listing all Services
====================

You can list all services in a namespace in CDAP by issuing an HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/services

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID

.. highlight:: json-ellipsis

The response body will contain a JSON-formatted list of the existing services::

  [
      {
          "app": "PurchaseHistory",
          "description": "Service to retrieve Product IDs.",
          "id": "CatalogLookup",
          "name": "CatalogLookup",
          "type": "Service"
      }
      ...
  ]

.. highlight:: console

Checking Service Availability
=============================
Once a service is started, you can can check whether the service is ready to accept service method requests by issuing
an HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/services/<service-id>/available

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application
   * - ``service-id``
     - Name of the service whose availability needs to be checked

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``503 Service Unavailable``
     - The service is unavailable to take requests at the moment. For example, it might not have been started, or
       if the service has been started, it might not have become available yet to take requests.
   * - ``200 OK``
     - Service is ready to accept requests.

Note that when the service availability check returns ``200``, it is expected that calling the service
methods will work. However, there is still a possibility for a service method call to fail; for example, if the
service fails just after the availability call returns. It is highly recommended that error conditions
(a ``503`` status code) be handled when making requests to service methods simply by retrying the request.

Requesting Service Methods
==========================
To make a request to a service's method, send the value of the method's ``@Path`` annotation
as part of the request URL along with any additional headers, body, and query parameters.

The request type is defined by the service's method::

  <request-type> /v3/namespaces/<namespace-id>/apps/<app-id>/services/<service-id>/methods/<endpoint-path>

**Note:** Any reserved or unsafe characters in the path parameters should be encoded using
:ref:`percent-encoding <http-restful-api-conventions-reserved-unsafe-characters>`. See the
section on :ref:`Path Parameters<services-path-parameters>` for suggested approaches to
encoding parameters.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``request-type``
     - One of GET, POST, PUT, or DELETE. This is defined by the handler method.
   * - ``app-id``
     - Name of the application being called
   * - ``service-id``
     - Name of the service being called
   * - ``endpoint-path``
     - Endpoint path of the method being called

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``503 Service Unavailable``
     - The service is unavailable. For example, it may not yet have been started.

Other responses are defined by the service's method.

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET /v3/namespaces/default/apps/WordCount/services/RetrieveCounts/methods/count/Cask?limit=2``
   * - Description
     - Make a request to the ``count/{word}`` endpoint of the ``RetrieveCounts`` service
       in the application ``WordCount`` in the namespace *default* to get a count of the
       word "Cask" and its associated words with a limit of 2.
   * - Response Status Code
     - ``200 OK``
   * - Response Body
     - ``{ "assocs": { "CaskData": 1, "CaskInc": 1 }, "count": 5, "word": "Cask"}``
