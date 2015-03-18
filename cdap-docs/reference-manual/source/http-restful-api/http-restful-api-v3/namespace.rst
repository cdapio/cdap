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

  PUT <base-url>/namespaces/<namespace>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace

The ``<namespace>`` must be of the limited character set for namespaces, as 
described in the :ref:`Introduction <http-restful-api-namespace-characters>`.
Properties for the namespace are passed in the JSON request body:

.. list-table::
   :widths: 20 40 40
   :header-rows: 1

   * - Parameter
     - Description
     - Default Value (if not defined)
   * - ``description``
     - Display description of the namespace
     - An empty string ("")
   * - ``config``
     - :ref:`Configuration preferences <preferences>` for the namespace
     - A JSON string of configuration key-value pairs

If a namespace with the same name already exists, the method will still return ``200 OK``,
but with a message that the ``Namespace '<namespace>' already exists``.

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

  GET <base-url>/namespaces

This will return a JSON String map that lists each namespace with its name and description
(reformatted to fit)::

  [{"name":"default","description":"Default Namespace","config":{"scheduler.queue.name":""},
   {"name":"demo_namespace","description":"My Demo Namespace","config":{"scheduler.queue.name":"demo"}]

Details of a Namespace
---------------------------------

For detailed information on a specific namespace, use::

  GET <base-url>/namespaces/<namespace>

The information (*namespace*, *description*, *config*) will be returned in the body of the
response, such as::

  {"name":"default","description":"Default Namespace","config":{"scheduler.queue.name":""}}

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The event successfully called the method, and the body contains the results

Editing a Namespace
-------------------
To edit an existing namespace, submit an HTTP PUT request to::

  PUT <base-url>/namespaces/<namespace>/properties

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace

The ``<namespace>`` must be of the limited character set for namespaces, as 
described in the :ref:`Introduction <http-restful-api-namespace-characters>`.
Properties for the namespace are passed in the JSON request body, as described
for when you `Create a Namespace`_.
