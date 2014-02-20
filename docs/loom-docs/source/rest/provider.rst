:orphan:

.. index::
   single: REST API: Provider
==================
REST API: Provider
==================

.. include:: /rest/rest-links.rst

Using the Loom REST API, you can manage providers as well as query available flavors of hardware or instance sizes.
(This API call is also used during the provisioning of instances of machines.) Even though new providers are automatically 
registered, the APIs are available if administrators desire to configure them manually. By default, Loom system supports 
Openstack out of the box.

Each provider configured in the system has a unique name, a short description, and a list of key-value pairs that are required by the backend hardware provisioner.

.. _provider-create:
Create a Provider
==================

To create a new provider, make a HTTP POST request to URI:
::
 /providers

POST Parameters
^^^^^^^^^^^^^^^^

Required Parameters

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Parameter
     - Description
   * - name
     - Specifies the name for the provider. The assigned name must have only
       alphanumeric, dash(-), dot(.), or underscore(_) characters.
   * - description
     - Provides a description for the provider type.
   * - providertype
     - Specifies the type of provider from the configured and available types.
   * - provisioner
     - Specifies the configuration that will be used by the provisioners. Currently, it's been specified
       as map of map of strings.

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
     - Bad request, server is unable to process the request or a provider with the name already exists 
       in the system.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X POST 
        -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        -d '{"name":"example", "providertype":"openstack", "description": "Example"}' 
        http://<loom-server>:<loom-port>/<version>/loom/providers

.. _provider-retrieve:
Retrieve a Provider
===================

To retrieve details about a provider type, make a GET HTTP request to URI:
::
 /providers/{name}

This resource request represents an individual provider for viewing.

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
     - If the resource requested is not configured or available in system.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/providers/example
 $ {"name":"example","description":"Example","providertype":"openstack","provisioner":{}}


.. _provider-delete:
Delete a Provider
=================

To delete a provider type, make a DELETE HTTP request to URI:
::
 /providers/{name}

This resource request represents an individual provider for deletion.

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
        http://<loom-server>:<loom-port>/<version>/loom/providers/example

.. _provider-modify:
Update a Provider
==================

To update a provider type, make a PUT HTTP request to URI:
::
 /providers/{name}

Resource specified above respresents an individual provider that is being updated.
Currently, the update of provider resource requires complete provider object to be 
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
     - Name of the resource to be updated. The name should match. 
   * - description
     - New description to be updated or old if not specified.
   * - providertype
     - New provider type to be updated or old if not specified.
   * - provisioner
     - New provisioner configurations or else retain the previous configuration.

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
        -d '{"name": "example", "description": "Updated example", "providertype":"openstack"}'  
        http://<loom-server>:<loom-port>/<version>/loom/providers/example
 $ curl -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/providers/example
 $ curl -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/providers/example
 $ {"name":"example","description":"Updated example","providertype":"openstack","provisioner":{}}

.. _provider-all-list:
List All Providers
=============================

A configured provider represents a resource used for querying resource types as well as for provisioning the 
resources. The list of all configured providers are available for you to retrieve. The provider list resource represents 
the comprehensive set of providers configured within the Loom system.

To list all the providers configured within in Loom, make GET HTTP request to URI:
::
 /providers

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
        http://<loom-server>:<loom-port>/<version>/loom/providers

