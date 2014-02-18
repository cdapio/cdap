:orphan:

.. index::
   single: REST API: Hardware
==================
REST API: Hardware
==================

.. include:: /rest/rest-links.rst

Loom REST APIs allow you to manage the mapping of hardware capabilities to "flavors" supported by configured hardwares. The Loom hardware type maps to multiple flavors as specified by 
different hardwares. By using hardware Loom REST APIs, you can manage or specify the hardware specifications.

Each hardware configured in the system has a unique name, a short description, and a list of key-value pairs that are required by the backend hardware provisioner.

.. _hardware-create:
Create a Hardware Type
======================

To create a new hardware type, make a HTTP POST request to URI:
::
 /hardwaretypes

POST Parameters
^^^^^^^^^^^^^^^^

Required Parameters

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Parameter
     - Description
   * - name
     - Specifies the name for the hardware type. The assigned name must have only
       alphanumeric, dash(-), dot(.), and underscore(_) characters.
   * - description
     - Provides a description for the hardware type.
   * - providermap
     - Provider map is map of providers and equivalent flavor type for current hardware type being configured.
       It's currently a map of map.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table:: 
   :widths: 15 10 
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - Successfully created
   * - 400 (BAD_REQUEST)
     - Bad request, server is unable to process the request or a hardware with the name already exists 
       in the system.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X POST 
        -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        -d '{"name":"small.example", "description":"Example 1 vCPU, 1 GB RAM, 30+ GB Disk", "providermap": {"openstack": {"flavor":"m1.small"}}}' 
        http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes

.. _hardware-retrieve:
View a Hardware Type
=====================

To retrieve details about a hardware type, make a GET HTTP request to URI:
::
 /hardwaretypes/{name}

This resource request represents an individual hardware for viewing.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - Successful
   * - 404 (NOT FOUND)
     - If the resource requested is not configured and available in system.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes/small.example
 $ {"name":"small.example","description":"Example 1 vCPU, 1 GB RAM, 30+ GB Disk","providermap":{"openstack":{"flavor":"m1.small"}}}


.. _hardware-delete:
Delete a Hardware Type
======================

To delete a hardware type, make a DELETE HTTP request to URI:
::
 /hardwaretypes/{name}

This resource request represents an individual hardware type for deletion.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - If delete was successful
   * - 404 (NOT FOUND)
     - If the resource requested is not found.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X DELETE
        -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes/example

.. _hardware-modify:
Update a Hardware Type
=======================

To update a hardware type, make a PUT HTTP request to URI:
::
 /hardwaretypes/{name}

The resource specified above respresents an individual hardware type request for an update.
Currently, the update of hardware type resource requires complete hardware type object to be 
returned back rather than individual fields.

PUT Parameters
^^^^^^^^^^^^^^^^

Required Parameters

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Parameter
     - Description
   * - name
     - Specifies the name of the hardware type to be updated. 
   * - description
     - New description or old one for the hardware type.
   * - providermap
     - Provider map is map of providers and equivalent flavor type for current hardware type being configured.
       It's currently a map of map.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - If update was successful
   * - 400 (BAD REQUEST)
     - If the resource requested is not found or the fields of the PUT body doesn't specify all the required fields.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X PUT
        -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        -d '{"name":"small.example", "description":"New Example 1 vCPU, 1 GB RAM, 30+ GB Disk", 
             "providermap": {"openstack": {"flavor":"m1.small"},"aws":{"flavor":"aws.small"}}}' 
        http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes/small.example
 $ curl http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes/small.example
 $ {"name":"small.example","description":"New Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
     "providermap":{"openstack":{"flavor":"m1.small"},"aws":{"flavor":"aws.small"}}}

.. _hardware-all-list:
List All Hardware Type
=============================

To list all the hardware types configured within in Loom, make GET HTTP request to URI:
::
 /hardwaretypes

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - Successful
   * - 400 (BAD REQUEST)
     - If the resource uri is specified incorrectly.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes

