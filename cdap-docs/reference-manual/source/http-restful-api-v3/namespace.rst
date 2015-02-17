.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-namespace:
.. _http-restful-api-v3-namespace:

===========================================================
Namespace HTTP RESTful API
===========================================================

.. highlight:: console

Use the CDAP Namespace HTTP API to create, list or delete namespaces in the CDAP instance.

Namespaces, their use and examples, are described in the :ref:`Developers' Manual: Namespaces
<namespaces>`.

For the remainder of this API, it is assumed that the namespace you are using is defined
by the ``<base-url>``, as descibed under :ref:`Conventions <http-restful-api-conventions>`. 

Create a Namespace
------------------
To create a namespace, submit an HTTP PUT request::

  PUT http://<host>:<port>/v3/namespaces/<namespace-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID

The ``<namespace-id>`` must be of the limited character set for namespaces, as 
described in the :ref:`Introduction <http-restful-api-namespace-characters>`.
Properties for the namespace are passed in the JSON request body:

.. list-table::
   :widths: 20 40 40
   :header-rows: 1

   * - Parameter
     - Description
     - Default Value (if not defined)
   * - ``name``
     - Display name for the namespace
     - The Namespace ID
   * - ``description``
     - Display description of the namespace
     - An empty string ("")

In this initial release of namespaces, once a namespace has been created with a particular
ID and properties, its properties cannot be edited. To change the display name and
description for a particular ID, you need to delete the namespace and recreate it. A
future release of CDAP will allow these properties to be edited.

If a namespace with the same ID already exists, the method will still return ``200 OK``,
but with a message that the ``Namespace '<namespace-id>' already exists``.

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the namespace was created



List Existing Namespaces
------------------------

To list all of the existing namespaces, issue an HTTP GET request::

  GET http://<host>:<port>/v3/namespaces

This will return a JSON String map that lists each namespace with its name and description
(reformatted to fit)::

  [{"id":"default","name":"default","description":"default"},
   {"id":"myNamespace","name":"My Demo Namespace","description":"Demonstration of namespaces"}]


Details of a Namespace
---------------------------------

For detailed information on a specific namespace, use::

  GET http://<host>:<port>/v3/namespaces/<namespace-id>

The information will be returned in the body of the response::

  {"id":"myNamespace","name":"myNamespace Demo","description":"Demonstration of the namespace"}

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results


Delete a Namespace
------------------
To delete a Namespace—together with all of its Flows, Datasets and MapReduce 
programs, any and all entities associated with that namespace—submit an HTTP DELETE::

  DELETE http://<host>:<port>/v3/namespaces/<namespace-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID

**Note:** This is an **unrecoverable operation**. As the deletion of a namespace occurs in
a transaction, if a delete for any of the entities that a namespace contains fails, the
result is a failure of the namespace deletion. The transaction will be rolled back and it
will be as if the deletion had not be attempted.
     
