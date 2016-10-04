.. meta::
:author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
      :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. _http-restful-api-routeconfig:

============================
RouteConfig HTTP RESTful API
============================

.. highlight:: console

Use the CDAP RouteConfig HTTP RESTful API to create, fetch, and delete route configs.


Additional details are found in the :ref:`Developers' Manual: Views <developers:services>`.


.. Base URL explanation
.. --------------------
.. include:: base-url.txt


.. _http-restful-api-routeconfig-uploading-routeconfig:

Uploading a Stream View
======================
A routeconfig for a user service can be uploading using an HTTP PUT request to the URL::

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

The request body is a JSON object which contains a map of version string to an integer specifying the percentage of
requests that should be routed to that version of the service. Note, that the percentages should add up to 100.
Also, all the versions specified in the request body should be existing in CDAP already.

.. rubric:: Example

For example, to upload a routeConfig for the service *MyService*:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X PUT "http://example.com:10000/v3/namespaces/default/apps/MyApp/services/MyService/routeconfig" \
    -H 'Content-Type: application/json' -d \
    "{
      "v1" : 50,
      "v2" : 50
    }"

.. rubric:: HTTP Responses

.. list-table::
:widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - A RouteConfig was successfully uploaded for that service
   * - ``404 BAD REQUEST``
     - If service is not present, or if a version of the service is not present or if the percentages don't add upto 100


.. _http-restful-api-routeconfig-fetching-routeconfig:

Fetching RouteConfig
====================
To fetch the RouteConfig of a service, issue an HTTP GET request to the URL::

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

The response body is a JSON object with the map of version to the percentage of requests to be routed to that version.
If a RouteConfig for that service is not found, an empty map is returned.

.. rubric:: Example

For example, to see to routeConfig of *MyService*, you could use:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X GET "http://example.com:10000/v3/namespaces/default/apps/MyApp/services/MyService/routeconfig"

  { "v1" : 50, "v2" : 50 }

.. rubric:: HTTP Responses

.. list-table::
:widths: 20 80
   :header-rows: 1

     * - Status Codes
       - Description
     * - ``200 OK``
       - Map of version to percentage of requests to be routed to that version

Deleting a RouteConfig
======================
To delete a RouteConfig for a service, issue an HTTP DELETE request to the URL::

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

For example, to delete the RouteConfig of *MyService*, you could use:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X DELETE "http://example.com:10000/v3/namespaces/default/apps/MyApp/services/MyService/routeconfig"

.. rubric:: HTTP Responses

.. list-table::
:widths: 20 80
   :header-rows: 1

     * - Status Codes
       - Description
     * - ``200 OK``
       - The route config of the service was successfully deleted
