:orphan:
.. include:: /toplevel-links.rst

==================
REST API: Cluster Templates
==================

Loom REST API allow you to create templates describing how clusters should be created.  This is done by first describing the set of services, hardware types, and image types that a cluster is compatible with.  Next, default values for provider, services, and configuration are given, with optional defaults for cluster-wide hardware and image type.  Finally, a set of constraints are defined which describe how services, hardware, and images should be placed on a cluster.

Each cluster template configured in the system will have a unique name, a short description and the above mentioned compatibilities, defaults, and constraints.

.. _template-create:
**Add a Cluster Template**
==================

To create a new cluster template, make a HTTP POST request to URI:
::
 /clustertemplates

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
        -d '{
                "name": "small.example",
                "description": "Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
                "dependson": ["hosts"],
                "provisioner": {
                    "actions": {
                        "configure": {
                            "script": "recipe[apt::default]", "type": "chef"
                        }}}}'
        http://<loom-server>:<loom-port>/<version>/loom/clustertemplates

.. _template-retrieve:
**Retrieve a Cluster Template**
===================

To retrieve details about a cluster template, make a GET HTTP request to URI:
::
 /clustertemplates/{name}

This resource represents an individual cluster template requested to be viewed.

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

 $ curl http://<loom-server>:<loom-port>/<version>/loom/clustertemplates/small.example
 $ {
    "dependson": [
        "hosts"
    ],
    "description": "Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
    "name": "small.example",
    "provisioner": {
        "actions": {}
    }
   }

.. _template-delete:
**Delete a Cluster Template**
=================

To delete a cluster template, make a DELETE HTTP request to URI:
::
 /clustertemplates/{name}

This resource represents an individual cluster template requested to be deleted.

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

 $ curl -X DELETE http://<loom-server>:<loom-port>/<version>/loom/clustertemplates/example

.. _template-modify:
**Update a Cluster Template**
==================

To update a service, make a PUT HTTP request to URI:
::
 /clustertemplates/{name}

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

 $ curl -X PUT -d '{
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
                                   }}}}'
      http://<loom-server>:<loom-port>/<version>/loom/clustertemplates/small.example
 $ curl http://<loom-server>:<loom-port>/<version>/loom/clustertemplates/small.example
 $ {"name":"small.example","description":"New Example 1 vCPU, 1 GB RAM, 30+ GB Disk",
      "dependson":["hosts"],"provisioner":{"actions":{"install":{"type":"chef","script":"recipe[apt::default]"},"configure":{"type":"chef","script":"recipe[apt::default]"}}}}

.. _template-all-list:
**List all Cluster Templates**
=============================

To list all the services configured within in Loom, make GET HTTP request to URI:
::
 /clustertemplates

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

 $ curl http://<loom-server>:<loom-port>/<version>/loom/clustertemplates

