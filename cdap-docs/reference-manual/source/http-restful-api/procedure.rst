.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _http-restful-api-procedure:

===========================================================
Procedure HTTP RESTful API
===========================================================

.. include:: ../../../_common/_include/include-v260-deprecate-procedures.rst

.. highlight:: console

This interface supports sending calls to the methods of an Application’s Procedures.
See the :ref:`http-restful-api-lifecycle` for how to control the life cycle of
Procedures.

Executing Procedures
--------------------

To call a method in an Application's Procedure, send the method name as part of the request URL
and the arguments as a JSON string in the body of the request.

The request is an HTTP POST::

  POST <base-url>/apps/<app-id>/procedures/<procedure-id>/methods/<method-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<app-id>``
     - Name of the Application being called
   * - ``<procedure-id>``
     - Name of the Procedure being called
   * - ``<method-id>``
     - Name of the method being called

HTTP Responses
..............
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results
   * - ``400 Bad Request``
     - The Application, Procedure and method exist, but the arguments are not as expected
   * - ``404 Not Found``
     - The Application, Procedure, or method does not exist
   * - ``503 Service Unavailable``
     - The Procedure method is unavailable. For example, the procedure may not have been started yet.

Example
.......
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``POST <base-url>/apps/WordCount/procedures/RetrieveCounts/methods/getCount``
   * - Description
     - Call the ``getCount()`` method of the *RetrieveCounts* Procedure in the *WordCount* Application
       with the arguments as a JSON string in the body::

       {"word":"a"}

