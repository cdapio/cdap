.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _http-restful-api-route-config:

=============================
Route Config HTTP RESTful API
=============================

.. highlight:: console

Use the CDAP Route Config HTTP RESTful API to create, fetch, and delete route
configurations (also known as *route configs*), which allocate requests between different
versions of a service.

Additional details on using route configurations with services are found in the :ref:`Developer
Manual: Service Routing <services-routing>`.


.. Base URL explanation
.. --------------------
.. include:: base-url.txt


.. _http-restful-api-route-config-uploading:

Uploading a Route Config
========================
A route configuration for a user service can be uploaded using an HTTP PUT request to the URL::

  PUT /v3/namespaces/<namespace-id>/apps/<app-id>/services/<service-id>/routeconfig

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the Application
   * - ``service-id``
     - Name of the Service (must be already existing)

The request body is a JSON object which contains a map of version strings to integers specifying the percentage of
requests that should be routed to that version of the service. Note that the percentages should total *100*.
All versions specified in the route configuration should already be deployed in CDAP.

.. rubric:: Example

For example, to upload a route configuration for the service *MyService*, with deployed versions *v1* and *v2*:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X PUT "http://example.com:11015/v3/namespaces/default/apps/MyApp/services/MyService/routeconfig" \
    -H 'Content-Type: application/json' -d \
    '{
      "v1" : 50,
      "v2" : 50
    }'

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - A RouteConfig was successfully uploaded for that service
   * - ``404 BAD REQUEST``
     - If service is not present, if a version of the service is not present, or if the percentages don't total 100


.. _http-restful-api-routeconfig-fetching-routeconfig:

Fetching a Route Config
=======================
To fetch the route configuration of a service, issue an HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/services/<service-id>/routeconfig

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the Application
   * - ``service-id``
     - Name of the Service

The response body is a JSON object with the map of versions to percentages of requests to be routed to each version.
If a route configuration for that service is not found, an empty map is returned.

.. rubric:: Example

For example, to retrieve the route configuration of the service *MyService*, use:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X GET "http://example.com:11015/v3/namespaces/default/apps/MyApp/services/MyService/routeconfig"

  { "v1" : 50, "v2" : 50 }

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Map of versions to percentage of requests to be routed to each version


Deleting a Route Config
=======================
To delete a route configuration for a service, issue an HTTP DELETE request to the URL::

  DELETE /v3/namespaces/<namespace-id>/apps/<app-id>/services/<service-id>/routeconfig

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the Application
   * - ``service-id``
     - Name of the Service

.. rubric:: Example

For example, to delete the route configuration of the service *MyService*, use:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X DELETE "http://example.com:11015/v3/namespaces/default/apps/MyApp/services/MyService/routeconfig"

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The route configuration of the service was successfully deleted
