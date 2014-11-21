.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _http-restful-api-service:

===========================================================
Service HTTP RESTful API
===========================================================

.. highlight:: console

This interface supports making requests to the methods of an Application’s Services.
See the :ref:`http-restful-api-lifecycle` for how to control the life cycle of
Services.

Requesting Service Methods
--------------------------
To make a request to a Service's method, send the method's path as part of the request URL along with any additional
headers and body.

The request type is defined by the Service's method::

  <REQUEST-TYPE> <base-url>/apps/<app-id>/services/<service-id>/methods/<method-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<REQUEST-TYPE>``
     - One of GET, POST, PUT and DELETE. This is defined by the handler method.
   * - ``<app-id>``
     - Name of the Application being called
   * - ``<service-id>``
     - Name of the Service being called
   * - ``<method-id>``
     - Name of the method being called

HTTP Responses
..............
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``503 Service Unavailable``
     - The Service is unavailable. For example, it may not yet have been started.

Other responses are defined by the Service's method.

Example
.......
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/apps/AnalyticsApp/services/IpGeoLookupService/methods/lookup/46.19.42.110``
   * - Description
     - Make a request to the ``lookup/{ip}`` endpoint of the ``IpGeoLookupService`` in ``AnalyticsApp``.
   * - Response Status Code
     - ``200 OK``
   * - Response Body
     - ``{"latitude": "76.9285", "longitude": "76.9285"}``
