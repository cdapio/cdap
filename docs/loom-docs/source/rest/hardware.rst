:orphan:

==================
REST API: Hardware
==================

Loom REST API allow you to manage the mapping of hardware capabilities to "flavors" supported by configured hardwares. Loom hardware type maps to multiple flavors as specified by different hardwares. Using hardware Loom REST APIs you can manage the hardware specifications.

Each hardware configured in the system will have a unique name, a short description and list of key-value pairs that are required by the backend hardware provisioner.

.. _hardware-create:
**Create a Hardware**
==================

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
     - Specifies the name to be assigned to the hardware type that is being created. Should have only
       alphanumeric, dash(-), dot(.) & underscore(_)
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
        -d '{"name":"small.example", "description":"Example 1 vCPU, 1 GB RAM, 30+ GB Disk", "providermap": {"openstack": {"flavor":"m1.small"}}}' 
        http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes

.. _hardware-retrieve:
**Retrieve a Hardware**
===================

To retrieve details about a hardware type, make a GET HTTP request to URI:
::
 /hardwaretypes/{name}

This resource represents an individual hardware requested to be viewed.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - Successfull
   * - 404 (NOT FOUND)
     - If the resource requested is not configured and available in system.

Example
^^^^^^^^
.. code-block:: bash

 $ curl http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes/small.example
 $ {"name":"small.example","description":"Example 1 vCPU, 1 GB RAM, 30+ GB Disk","providermap":{"openstack":{"flavor":"m1.small"}}}


.. _hardware-delete:
**Delete a Hardware**
=================

To delete a hardware type, make a DELETE HTTP request to URI:
::
 /hardwaretypes/{name}

This resource represents an individual hardware type requested to be deleted.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - If delete was successfull
   * - 404 (NOT FOUND)
     - If the resource requested is not found.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X DELETE http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes/example

.. _hardware-modify:
**Update a Hardware**
==================

To update a hardware type, make a PUT HTTP request to URI:
::
 /hardwaretypes/{name}

Resource specified above respresents a individual hardware type which is being updated.
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
     - If update was successfull
   * - 400 (BAD REQUEST)
     - If the resource requested is not found or the fields of the PUT body doesn't specify all the required fields.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -v -X PUT 
    -d '{"name":"small.example", "description":"New Example 1 vCPU, 1 GB RAM, 30+ GB Disk", 
          "providermap": {"openstack": {"flavor":"m1.small"},"aws":{"flavor":"aws.small"}}}' 
    http://<loom-server>:<loom-port>/v1/loom/hardwaretypes/small.example
 $ curl http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes/small.example
 $ {"name":"small.example","description":"New Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
     "providermap":{"openstack":{"flavor":"m1.small"},"aws":{"flavor":"aws.small"}}}

.. hardware-all-list:
**List All Hardwares**
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

 $ curl http://<loom-server>:<loom-port>/<version>/loom/hardwaretypes

