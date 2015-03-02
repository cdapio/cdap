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

For the remainder of this API, it is assumed that the preferences you are using is defined
by the ``<base-url>``, as described under :ref:`Conventions <http-restful-api-conventions>`.

Set Preferences
---------------
To set preferences for the CDAP Instance, Namespace, Application, or Program, submit an HTTP PUT request::

  PUT http://<host>:<port>/v3/configuration/preferences/

  PUT http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>

  PUT http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>/apps/<app-id>

  PUT http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>

.. list-table::
:widths: 20 80
   :header-rows: 1

     * - Parameter
       - Description
     * - ``<namespace-id>``
     - Namespace ID
     * - ``<app-id>``
     - Application ID
     * - ``<program-type>``
     - One of ``flows``, ``map reduce``, ``spark``, ``workflows`` or ``services``
     * - ``<program-id>``
     - Program ID

Properties, as a map of string-string pairs, are passed in the JSON request body.

Preferences can be set only for entities that exist. For example, Preferences cannot be set for a Namespace
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
     - The entity for which Preferences are being set was not found


Get Preferences
---------------

To retrieve the current preferences, issue an HTTP GET request::

  GET http://<host>:<port>/v3/configuration/preferences/

  GET http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>

  GET http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>/apps/<app-id>

  GET http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>

This will return a JSON String map of the preferences::

  {"key1":"value1", "key2":"value2"}

To retrieve the Resolved Preferences (collapsing Preferences from higher levels into a single level), set the
``resolved`` query parameter to ``true``::

  GET http://<host>:<port>/v3/configuration/preferences?resolved=true

  GET http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>?resolved=true

  GET http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>/apps/<app-id>?resolved=true

  GET http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>?resolved=true

.. list-table::
:widths: 20 80
   :header-rows: 1

       * - Parameter
         - Description
       * - ``<namespace-id>``
     - Namespace ID
     * - ``<app-id>``
     - Application ID
     * - ``<program-type>``
     - Program Type (flows, services, etc)
     * - ``<program-id>``
     - Program ID

.. rubric:: HTTP Responses

.. list-table::
:widths: 20 80
   :header-rows: 1

       * - Status Codes
         - Description
       * - ``200 OK``
     - The event successfully called the method, and the preferences was retrieved
     * - ``404 NOT FOUND``
     - The entity, for which Preferences is being retrieved, is not present

Delete Preferences
------------------
To delete preferences, issue an HTTP DELETE::

  DELETE http://<host>:<port>/v3/configuration/preferences/

  DELETE http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>

  DELETE http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>/apps/<app-id>

  DELETE http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>

.. list-table::
:widths: 20 80
   :header-rows: 1

     * - Parameter
     - Description
     * - ``<namespace-id>``
     - Namespace ID
     * - ``<app-id>``
     - Application ID
     * - ``<program-type>``
     - Program Type (flows, services, etc)
     * - ``<program-id>``
     - Program ID

.. rubric:: HTTP Responses

.. list-table::
:widths: 20 80
   :header-rows: 1

     * - Status Codes
     - Description
     * - ``200 OK``
     - The event successfully called the method, and the preferences was retrieved
     * - ``404 NOT FOUND``
     - The entity, for which Preferences is being retrieved, is not present
