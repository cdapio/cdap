.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _http-restful-api-summary:

===========================================================
Summary of HTTP RESTful API
===========================================================

.. highlight:: console

This is a list of all the HTTP RESTful API methods, grouped into the **namespace** API, then
the **namespaced** APIs, and then the **non-namespaced** APIs. Within, each are ordered by
API. Each is summarized with a link back to the reference page.

The **namespaced** APIs are those APIs that include a ``namespace`` in the HTTP Method.

Namespace API
=============

:ref:`Namespace API <http-restful-api-v3-namespace>`

All URLs referenced in this API have this base URL (``<base-url>``)::

  http://<host>:<port>/v3/


Namespaced APIs
===============
The APIs in this group all use namespaces, and the namespace to be used is included as
part of their URL. All URLs referenced in these APIs have this base URL (``<base-url>``)::

  http://<host>:<port>/v3/namespaces/<namespace-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<host>``
     - Host name of the CDAP server
   * - ``<port>``
     - Port set as the ``router.bind.port`` in ``cdap-site.xml`` (default: ``10000``)
   * - ``<namespace-id>``
     - Namespace ID, a valid and existing namespace in the CDAP instance

.. role:: raw-html(raw)
   :format: html
   
.. |br-space| replace:: :raw-html:`<br />            `

Lifecycle HTTP RESTful API
--------------------------

.. list-table::
   :widths: 80 20
   
   * - **Description** / HTTP Method
     - :ref:`Reference Page <http-restful-api-lifecycle>`

.. list-table::
   :widths: 100

   * - **Deploy an Application**
       |br-space| ``POST <base-url>/apps``
   * - **List of deployed Applications**
       |br-space| ``GET <base-url>/apps``
   * - **Details of a deployed Application**
       |br-space| ``GET <base-url>/apps/<app-id>``
   * - **Delete a deployed Application with all its elements**
       |br-space| ``DELETE <base-url>/apps/<application-name>``
   * - **Operate (start or stop) a deployed Application**
       |br-space| ``POST <base-url>/apps/<app-id>/<program-type>/<program-id>/<operation>``
   * - **Get the status of a deployed Application**
       |br-space| ``GET <base-url>/apps/<app-id>/<program-type>/<program-id>/status``
   * - **Get the status of multiple deployed Applications**
       |br-space| ``POST <base-url>/status``
   * - **Get the live-info of a deployed Application**
       |br-space| ``GET <base-url>/apps/<app-id>/<program-type>/<program-id>/live-info``
   * - **Get the instance count of different elements of a deployed Application**
       |br-space| ``POST <base-url>/instances``
   * - **Get the instance count of a given Flowlet of a deployed Application**
       |br-space| ``GET <base-url>/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances``
   * - **Set the instance count of a given Flowlet of a deployed Application**
       |br-space| ``PUT <base-url>/apps/<app-id>/flows/<flow-id>/flowlets/<flowlet-id>/instances``
   * - **Get the instance count of a given Procedure of a deployed Application**
       |br-space| ``GET <base-url>/apps/<app-id>/procedures/<procedure-id>/instances``
   * - **Set the instance count of a given Procedure of a deployed Application**
       |br-space| ``PUT <base-url>/apps/<app-id>/procedures/<procedure-id>/instances``
   * - **Get the instance count of a given Service of a deployed Application**
       |br-space| ``GET <base-url>/apps/<app-id>/services/<service-id>/runnables/<runnable-id>/instances``
   * - **Set the instance count of a given Service of a deployed Application**
       |br-space| ``PUT <base-url>/apps/<app-id>/services/<service-id>/runnables/<runnable-id>/instances``
   * - **Get the runs of a selected program of a deployed Application**
       |br-space| ``GET <base-url>/apps/<app-id>/<program-type>/<program-id>/runs``
   * - **Get the history of successfully completed Twill Services**
       |br-space| ``GET <base-url>/apps/<app-id>/services/<service-id>/runs?status=completed``
   * - **Get the schedules defined for a Workflow**
       |br-space| ``GET <base-url>/apps/<app-id>/workflows/<workflow-id>/schedules``
   * - **Get the next time the schedule for a Workflow is to run**
       |br-space| ``GET <base-url>/apps/<app-id>/workflows/<workflow-id>/nextruntime``
   * - **Suspend a Schedule**
       |br-space| ``POST <base-url>/apps/<app-id>/schedules/<schedule-name>/suspend``
   * - **Resume a Schedule**
       |br-space| ``POST <base-url>/apps/<app-id>/schedules/<schedule-name>/resume``



Non-namespaced APIs
===================
The APIs in this group **do not** use namespaces. All URLs referenced in these APIs have
this base URL (``<base-url>``)::

  http://<host>:<port>/v3/namespaces/<namespace-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<host>``
     - Host name of the CDAP server
   * - ``<port>``
     - Port set as the ``router.bind.port`` in ``cdap-site.xml`` (default: ``10000``)
   * - ``<namespace-id>``
     - Namespace ID, a valid and existing namespace in the CDAP instance