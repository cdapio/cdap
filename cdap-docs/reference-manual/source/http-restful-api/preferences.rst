.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. _http-restful-api-preferences:
.. _http-restful-api-v3-preferences:

============================
Preferences HTTP RESTful API
============================

.. highlight:: console

Use the CDAP Preferences HTTP RESTful API to save, retrieve, and delete preferences in CDAP.
Preferences, their use, and examples of using them, are described in the
:ref:`Administration Manual: Preferences <preferences>`.

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


Set Preferences
===============
To set preferences for the CDAP instance, namespace, application, or program, submit an HTTP PUT request::

  PUT /v3/preferences/

  PUT /v3/namespaces/<namespace-id>/preferences

  PUT /v3/namespaces/<namespace-id>/apps/<app-id>/preferences

  PUT /v3/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/preferences

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of  application
   * - ``program-type``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services`` or ``workers``
   * - ``program-id``
     - Name of program

Properties, as a map of string-string pairs, are passed in the JSON request body.

Preferences can be set only for entities that exist. For example, preferences cannot be set for a namespace
that does not exist or an application that has not yet been deployed.

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the preferences were set
   * - ``400 BAD REQUEST``
     - The JSON body has an invalid format
   * - ``404 NOT FOUND``
     - The entity for which preferences are being set was not found


Get Preferences
===============

To retrieve the current preferences, issue an HTTP GET request::

  GET /v3/preferences/

  GET /v3/namespaces/<namespace-id>/preferences

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/preferences

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/preferences

This will return a JSON String map of the preferences::

  {"key1":"value1", "key2":"value2"}

To retrieve the resolved preferences (collapsing preferences from higher levels into a single level), set the
``resolved`` query parameter to ``true``::

  GET /v3/preferences?resolved=true

  GET /v3/namespaces/<namespace-id>/preferences?resolved=true

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/preferences?resolved=true

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/preferences?resolved=true

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of application
   * - ``program-type``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services`` or ``workers``
   * - ``program-id``
     - Name of  program

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the preferences were retrieved
   * - ``404 NOT FOUND``
     - The entity for which preferences are being set was not found

Delete Preferences
==================
To delete preferences, issue an HTTP DELETE. Preferences can be deleted only at one level with each request::

  DELETE /v3/preferences/

  DELETE /v3/namespaces/<namespace-id>/preferences

  DELETE /v3/namespaces/<namespace-id>/apps/<app-id>/preferences

  DELETE /v3/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/preferences

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of application
   * - ``program-type``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services`` or ``workers``
   * - ``program-id``
     - Name of program

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the preferences were deleted
   * - ``404 NOT FOUND``
     - The entity for which preferences are being deleted was not found
