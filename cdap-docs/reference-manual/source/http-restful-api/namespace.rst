.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-namespace:
.. _http-restful-api-v3-namespace:

==========================
Namespace HTTP RESTful API
==========================

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
     - An empty string (``""``)
   * - ``config``
     - Configuration preferences for the namespace
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

.. _http-restful-api-namespace-editing:

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

The ``<namespace>`` must be the name of an existing namespace.
Properties for the namespace are passed in the JSON request body, as described
for when you `Create a Namespace`_.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Property
     - Description
   * - ``description``
     - Display description of the namespace
   * - ``config``
     - Configuration properties, with a JSON map of name-value pairs. Currently, the only
       supported configuration property is ``scheduler.queue.name``: 
       :ref:`Scheduler queue <resource-guarantees>` for CDAP Programs and Explore Queries in the namespace.
    
.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Namespace properties were changed successfully
   * - ``400 Bad Request``
     - The request was not created correctly
   * - ``404 Not Found``
     - The Namespace does not exist

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``PUT <base-url>/namespaces/dev/properties``::

         { 
           "description" : "Namespace for development of applications",
           "config": {
             "scheduler.queue.name": "A",
           },
         }
     
   * - Description
     - Set the *description* property of the Namespace named *dev*,
       and set the *scheduler.queue.name* to *A*. 

.. _http-restful-api-namespace-deleting:

Deleting a Namespace
--------------------

To delete an existing namespace (including all applications, streams, flows, datasets, metrics,
and other components), submit an HTTP DELETE request to::

  DELETE <base-url>/unrecoverable/namespaces/<namespace>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace

To prevent accidental use of this method, it will only work if the ``cdap-site.xml`` parameter
``enable.unrecoverable.reset`` has been enabled.
This method must be exercised with extreme caution, as there is no recovery from it.

Both the CDAP CLI and the CDAP UI expose this function; they also require the parameter to be 
enabled in order for their functions to work.

In the case of the ``default`` namespace, after everything has been deleted, the ``default`` 
namespace is retained, as it is always available in CDAP.

To delete only the datasets of a namespace, there is a 
:ref:`specific dataset endpoint <http-restful-api-dataset-deleting-all>` for that.
