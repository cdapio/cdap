:orphan:

.. index::
   single: REST API: Services
==================
REST API: Services
==================

.. include:: /rest/rest-links.rst

Loom REST APIs allow you to manage the mapping of services capabilities to "flavors" supported by configured services. Loom services maps to multiple flavors as specified by 
different services. Using services Loom REST APIs, you can manage the services' specifications.

Each services configured in the system has a unique name, a short description, and a list of key-value pairs that are required by the backend services provisioner.

.. _service-create:
Add a Service
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
     - Specifies the name for the services. The assigned name must have only
       alphanumeric, dash(-), dot(.), and underscore(_) characters.
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
     - Bad request, server is unable to process the request, or a services' name already exists 
       in the system.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X POST 
        -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        -d '{
                "name": "small.example",
                "description": "Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
                "dependson": ["hosts"],
                "provisioner": {
                    "actions": {
                        "configure": {
                            "script": "recipe[apt::default]", "type": "chef"
                        }
                    }
                }
           }'
        http://<loom-server>:<loom-port>/<version>/loom/services

.. _service-retrieve:
Retrieve a Service
===================

To retrieve details about a services, make a GET HTTP request to URI:
::
 /services/{name}

This resource request represents an individual services for viewing.

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
        http://<loom-server>:<loom-port>/<version>/loom/services/small.example
 $ {
       "dependson": [ "hosts" ],
       "description": "Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
       "name": "small.example",
       "provisioner": {
           "actions": {}
       }
   }

.. _service-delete:
Delete a Service
=================

To delete services, make a DELETE HTTP request to URI:
::
 /services/{name}

This resource request represents an individual services for deletion.

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
        http://<loom-server>:<loom-port>/<version>/loom/services/example

.. _service-modify:
Update a Service
==================

To update a service, make a PUT HTTP request to URI:
::
 /services/{name}

Resource specified above respresents an individual services request for an update operation.
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
     - If update was successful
   * - 400 (BAD REQUEST)
     - If the resource requested is not found or the fields of the PUT body do not specify all the required fields.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X PUT 
        -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        -d '{
                 "name": "small.example",
                 "description": "New Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
                 "dependson": ["hosts"],
                 "provisioner": {
                     "actions": {
                         "configure": {
                             "script": "recipe[apt::default]","type": "chef"
                         },
                         "install": {
                             "script": "recipe[apt::default]", "type": "chef"
                         }
                     }
                 }
           }'
        http://<loom-server>:<loom-port>/<version>/loom/services/small.example
 $ curl -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/services/small.example
 $ {
       "name":"small.example",
       "description":"New Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
       "dependson":["hosts"],
       "provisioner":{
           "actions":{
               "install":{
                   "type":"chef",
                   "script":"recipe[apt::default]"
               },
               "configure":{
                   "type":"chef",
                   "script":"recipe[apt::default]"
               }
           }
       }
   }

.. _service-all-list:
List all Services
=============================

To list all the services configured within Loom, make a GET HTTP request to URI:
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

 $ curl -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/services

