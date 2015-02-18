.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _http-restful-api-service:

========================
Service HTTP RESTful API
========================

.. highlight:: console

This interface supports listing all Services and making requests to the methods of an Application’s Services.
See the :ref:`http-restful-api-lifecycle` for how to control the lifecycle of Services.

Listing all Services
--------------------

You can list all Services in CDAP by issuing an HTTP GET request to the URL::

  GET <base-url>/namespaces/<namespace-id>/services

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
     
The response body will contain a JSON-formatted list of the existing Services::

  [
      {
          "app": "PurchaseHistory",
          "description": "Service to lookup product ids.",
          "id": "CatalogLookup",
          "name": "CatalogLookup",
          "type": "Service"
      }
      ...
  ]

Requesting Service Methods
--------------------------
To make a request to a Service's method, send the value of the method's ``@Path`` annotation
as part of the request URL along with any additional headers, body and query parameters.

The request type is defined by the Service's method::

  <request-type> <base-url>/namespaces/<namespace-id>/apps/<app-id>/services/<service-id>/methods/<endpoint-path>
  
**Note:** Any reserved or unsafe characters in the path parameters should be encoded using 
:ref:`percent-encoding <http-restful-api-conventions-reserved-unsafe-characters>`. See the
section on :ref:`Path Parameters<services-path-parameters>` for suggested approaches to
encoding parameters.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<request-type>``
     - One of GET, POST, PUT and DELETE. This is defined by the handler method.
   * - ``<app-id>``
     - Name of the Application being called
   * - ``<service-id>``
     - Name of the Service being called
   * - ``<endpoint-path>``
     - Endpoint path of the method being called

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``503 Service Unavailable``
     - The Service is unavailable. For example, it may not yet have been started.

Other responses are defined by the Service's method.

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/namespaces/default/apps/WordCount/services/RetrieveCounts/methods/count/Cask?limit=2``
   * - Description
     - Make a request to the ``count/{word}`` endpoint of the ``RetrieveCounts`` Service
       in the application ``WordCount`` in the namespace *default* to get a count of the
       word "Cask" and its associated words with a limit of 2.
   * - Response Status Code
     - ``200 OK``
   * - Response Body
     - ``{ "assocs": { "CaskData": 1, "CaskInc": 1 }, "count": 5, "word": "Cask"}``
