.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-preferences:
.. _http-restful-api-v3-preferences:

============================
Preferences HTTP RESTful API
============================

.. highlight:: console

Use the CDAP Preferences HTTP RESTful API to save, retrieve, and delete preferences in CDAP.

Preferences, their use and examples of using them, are described in the :ref:`Administration Manual: Preferences <preferences>`.

For the remainder of this API, it is assumed that the preferences you are using are defined
by the ``<base-url>``, as described under :ref:`Conventions <http-restful-api-conventions>`.

Set Preferences
---------------
To set preferences for the CDAP instance, namespace, application, or program, submit an HTTP PUT request::

  PUT <base-url>/preferences/

  PUT <base-url>/namespaces/<namespace>/preferences

  PUT <base-url>/namespaces/<namespace>/apps/<app-id>/preferences

  PUT <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/preferences

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of  application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services`` or ``workers``
   * - ``<program-id>``
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
---------------

To retrieve the current preferences, issue an HTTP GET request::

  GET <base-url>/preferences/

  GET <base-url>/namespaces/<namespace>/preferences

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/preferences

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/preferences

This will return a JSON String map of the preferences::

  {"key1":"value1", "key2":"value2"}

To retrieve the resolved preferences (collapsing preferences from higher levels into a single level), set the
``resolved`` query parameter to ``true``::

  GET <base-url>/preferences?resolved=true

  GET <base-url>/namespaces/<namespace>/preferences?resolved=true

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/preferences?resolved=true

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/preferences?resolved=true

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services`` or ``workers``
   * - ``<program-id>``
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
------------------
To delete preferences, issue an HTTP DELETE. Preferences can be deleted only at one level with each request::

  DELETE <base-url>/preferences/

  DELETE <base-url>/namespaces/<namespace>/preferences

  DELETE <base-url>/namespaces/<namespace>/apps/<app-id>/preferences

  DELETE <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/preferences

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services`` or ``workers``
   * - ``<program-id>``
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
