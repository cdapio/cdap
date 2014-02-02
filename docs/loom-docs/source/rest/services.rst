:orphan:
.. include:: /toplevel-links.rst

==================
REST API: Services
==================

Loom REST API allow you to manage the mapping of services capabilities to "flavors" supported by configured servicess. Loom services maps to multiple flavors as specified by different servicess. Using services Loom REST APIs you can manage the services specifications.

Each services configured in the system will have a unique name, a short description and list of key-value pairs that are required by the backend services provisioner.

.. contents::
        :local:
        :class: faq
        :backlinks: none

.. _services-create:
**Add a Service**
==================

To create a new services, make a HTTP POST request to URI:
::
 /services

POST Parameters
^^^^^^^^^^^^^^^^

Required Parameters

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Parameter
     - Description
   * - name
     - Specifies the name to be assigned to the services that is being created. Should have only
       alphanumeric, dash(-), dot(.) & underscore(_)
   * - description
     - Provides a description for the services.
   * - providermap
     - Provider map is map of providers and equivalent flavor type for current services being configured.
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
     - Bad request, server is unable to process the request or a services with the name already exists 
       in the system.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X POST 
        -d '{"name":"small.example", "description":"Example 1 vCPU, 1 GB RAM, 30+ GB Disk", "dependson": ["hosts"], "provisioner":{"actions":{}}}'
        http://<loom-server>:<loom-port>/<version>/loom/services

.. _services-retrieve:
**Retrieve a Service**
===================

To retrieve details about a services, make a GET HTTP request to URI:
::
 /services/{name}

This resource represents an individual services requested to be viewed.

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

 $ curl http://<loom-server>:<loom-port>/<version>/loom/services/small.example
 $ {"name":"small.example","description":"Example 1 vCPU, 1 GB RAM, 30+ GB Disk","dependson":["hosts"],"provisioner":{"actions":{}}}


.. _services-delete:
**Delete a Service**
=================

To delete a services, make a DELETE HTTP request to URI:
::
 /services/{name}

This resource represents an individual services requested to be deleted.

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

 $ curl -X DELETE http://<loom-server>:<loom-port>/<version>/loom/services/example

.. _services-modify:
**Update a Service**
==================

To update a service, make a PUT HTTP request to URI:
::
 /services/{name}

Resource specified above respresents a individual services which is being updated.
Currently, the update of services resource requires complete services object to be
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
     - Specifies the name of the service to be updated.
   * - description
     - New description or old one for the service.
   * - providermap
     - Provider map is map of providers and equivalent flavor type for current services being configured.
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

 $ curl -v -X PUT -d '{"name":"small.example", "description":"New Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
      "dependson": ["hosts"], "provisioner":{"actions":{}}}'
      http://<loom-server>:<loom-port>/<version>/loom/services/small.example
 $ curl http://<loom-server>:<loom-port>/<version>/loom/services/small.example
 $ {"name":"small.example","description":"New Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
      "dependson":["hosts"],"provisioner":{"actions":{}}}

.. _services-all-list:
**List all Services**
=============================

To list all the services configured within in Loom, make GET HTTP request to URI:
::
 /services

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

 $ curl http://<loom-server>:<loom-port>/<version>/loom/services

