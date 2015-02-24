.. meta::
:author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
      :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-preferences:
.. _http-restful-api-v3-namespace:

===========================================================
Preferences HTTP RESTful API
===========================================================

.. highlight:: console

Use the CDAP Preferences HTTP API to save, retrieve, delete preferences in CDAP.

Preferences, their use and examples, are described in the :ref:`Admin' Manual: Preferences
  <preferences>`.

  For the remainder of this API, it is assumed that the preferences you are using is defined
  by the ``<base-url>``, as descibed under :ref:`Conventions <http-restful-api-conventions>`.

Set Preferences
------------------
To set preferences for the CDAP Instance, Namespace, Application, Program submit an HTTP PUT request::

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
     - Program Type (flows, services, etc)
     * - ``<program-id>``
     - Program ID

Preferences can be set only for entities that exist. For example, Preferences cannot be set for a Namespace
that does not exist or an application that has not be deployed. Properties, which are map of string-string pairs are
passed in the JSON request body.

.. rubric:: HTTP Responses

.. list-table::
:widths: 20 80
   :header-rows: 1

     * - Status Codes
       - Description
     * - ``200 OK``
     - The event successfully called the method, and the preferences was set
     * - ``400 BAD REQUEST``
     - The JSON body has invalid format
     * - ``404 NOT FOUND``
     - The entity, for which Preferences is being set, is not present


Get Preferences
---------------

To get the preferences, issue an HTTP GET request::

  GET http://<host>:<port>/v3/configuration/preferences/

  GET http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>

  GET http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>/apps/<app-id>

  GET http://<host>:<port>/v3/configuration/preferences/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>

This will return a JSON String map that has the preferences in a JSON map format::

  {"key1":"value1", "key2":"value2"}

In order to get Resolved Preferences (collapsing Preferences from higher levels), one can set the ``resolved`` query parameter to ``true``::

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
