:orphan:

.. index::
   single: REST API: Importing & Exporting Cluster Templates
==================
REST API: Importing & Exporting Cluster Templates
==================

.. include:: /rest/rest-links.rst

Loom REST APIs allow you to export all Providers, Hardware Types, Image Types, Services, and Cluster Templates created in a Loom server into a JSON Object that can then be imported into another Loom server.  

.. _entity-export:
Export Template Metadata
========================

To export all entities from a Loom server, make a HTTP GET request to URI:
::
 /export

The response is a JSON Object with keys for providers, hardwaretypes, imagetypes, services, and clustertemplates.  Each key has a JSON array as its value, with each element in the array as the json representation as described in the corresponding sections for providers, hardware types, image types, services, and cluster templates.

HTTP Responses
^^^^^^^^^^^^^^

.. list-table:: 
   :widths: 15 10 
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - Successfully created
   * - 401 (UNAUTHORIZED)
     - The user is unauthorized and cannot perform an export.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        http://<loom-server>:<loom-port>/<version>/loom/export
   {
     "providers":[ ... ],
     "hardwaretypes":[ ... ],
     "imagetypes":[ ... ],
     "services":[ ... ],
     "clustertemplates":[ ... ]
   }

.. _entity-import:
Import Template Metadata
========================

To import entities into a Loom server, make a POST HTTP request to URI:
::
 /import

The post body must be a JSON object of the same format as the export result.  It has a key for providers, hardwaretypes, imagetypes, services, and clustertemplates.  The value for each key is a JSON array, with each element in the array as a JSON object representation of the corresponding entity.  

.. note:: Imports will wipe out all existing entities from a Loom server, replacing everything with the entities given in the post body. 

HTTP Responses
^^^^^^^^^^^^^^

.. list-table::
   :widths: 15 10
   :header-rows: 1

   * - Status Code
     - Description
   * - 200 (OK)
     - Successful
   * - 403 (FORBIDDEN)
     - If a non-admin user tries to import entities into the server.

Example
^^^^^^^^
.. code-block:: bash

 $ curl -X POST
        -H 'X-Loom-UserID:admin' 
        -H 'X-Loom-ApiKey:<apikey>'
        -d '{ 
              "providers":[...],
              "imagetypes":[...],
              "hardwaretypes":[...],
              "services":[...],
              "clustertemplates":[...]
            }' http://<loom-server>:<loom-port>/<version>/loom/import

