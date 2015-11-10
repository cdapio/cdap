.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-metadata:
.. _http-restful-api-v3-metadata:

=========================
Metadata HTTP RESTful API
=========================

.. highlight:: console

Use the CDAP Metadata HTTP RESTful API to set, retrieve, and delete the metadata annotations
of applications, datasets, streams, and other entities in CDAP.

Metadata consists of **properties** (a list of key-value pairs) or **tags** (a list of keys).
Metadata and their use are described in the :ref:`Developers' Manual: Metadata and Lineage
<metadata-lineage>`.

The HTTP RESTful API is divided into these sections:

- :ref:`metadata properties <http-restful-api-metadata-properties>`
- :ref:`metadata tags <http-restful-api-metadata-tags>`
- :ref:`searching properties <http-restful-api-metadata-searching>`
- :ref:`viewing lineage <http-restful-api-metadata-lineage>`
- :ref:`metadata for a run of a program <http-restful-api-metadata-run>`

Metadata keys, values, and tags must conform to the CDAP :ref:`alphanumeric extra extended
character set <supported-characters>`, and are limited to 50 characters in length. The entire
metadata object associated with a single entity is limited to 10K bytes in size.

There is one reserved word for property keys and values: *tags*, either as ``tags`` or
``TAGS``. Tags themselves have no reserved words.

In this API, ``<base-url>`` is as described under :ref:`Conventions
<http-restful-api-conventions>`. 


.. _http-restful-api-metadata-properties:

Metadata Properties
===================

Annotating Properties
---------------------
To annotate user metadata properties for an application, dataset, or stream, submit an HTTP POST request::

  POST <base-url>/namespaces/<namespace>/<entity-type>/<entity-id>/metadata/properties
  
or, for a particular program of a specific application::

  POST <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/properties

with the metadata properties as a JSON string map of string-string pairs, passed in the
request body::

  {
    "key1" : "value1",
    "key2" : "value2",
    // ...
  }
  
If the entity requested is found, new keys will be added and existing keys will be
updated. Existing keys not in the properties map will not be deleted.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<entity-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<entity-id>``
     - Name of the entity
   * - ``<app-id>``
     - Name of the application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services``, or ``workers``
   * - ``<program-id>``
     - Name of the program

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The properties were set
   * - ``404 NOT FOUND``
     - The entity or program for which properties are being set was not found
     

Retrieving Properties
---------------------
To retrieve user metadata properties for an application, dataset, or stream, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/<entity-type>/<entity-id>/metadata/properties
  
or, for a particular program of a specific application::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/properties

with the metadata properties returned as a JSON string map of string-string pairs, passed
in the response body (pretty-printed)::

  {
    "key1" : "value1",
    "key2" : "value2",
    // ...
  }

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<entity-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<entity-id>``
     - Name of the entity
   * - ``<app-id>``
     - Name of the application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services``, or ``workers``
   * - ``<program-id>``
     - Name of the program

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The properties requested were returned as a JSON string in the body of the response
   * - ``404 NOT FOUND``
     - The entity or program for which properties are being retrieved was not found


Deleting Properties
-------------------
To delete **all** user metadata properties for an application, dataset, or stream, submit an
HTTP DELETE request::

  DELETE <base-url>/namespaces/<namespace>/<entity-type>/<entity-id>/metadata/properties
  
or, for all user metadata properties of a particular program of a specific application::

  DELETE <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/properties

To delete **a specific property** for an application, dataset, or stream, submit
an HTTP DELETE request with the property key::

  DELETE <base-url>/namespaces/<namespace>/<entity-type>/<entity-id>/metadata/properties/<key>
  
or, for a particular property of a program of a specific application::

  DELETE <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/properties/<key>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<entity-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<entity-id>``
     - Name of the entity
   * - ``<app-id>``
     - Name of the application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services``, or ``workers``
   * - ``<program-id>``
     - Name of the program
   * - ``<key>``
     - Metadata property key

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The method was successfully called, and the properties were deleted, or in the case of a
       specific key, were either deleted or the key was not present
   * - ``404 NOT FOUND``
     - The entity or program for which properties are being deleted was not found


.. _http-restful-api-metadata-tags:

Metadata Tags
=============

Adding Tags
-----------
To add user metadata tags for an application, dataset, or stream, submit an HTTP POST request::

  POST <base-url>/namespaces/<namespace>/<entity-type>/<entity-id>/metadata/tags
  
or, for a particular program of a specific application::

  POST <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/tags

with the metadata tags, as a list of strings, passed in the JSON request body::

  ["tag1", "tag2"]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<entity-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<entity-id>``
     - Name of the entity
   * - ``<app-id>``
     - Name of the application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services``, or ``workers``
   * - ``<program-id>``
     - Name of the program

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The tags were set
   * - ``404 NOT FOUND``
     - The entity or program for which tags are being set was not found


Retrieving Tags
---------------
To retrieve user metadata tags for an application, dataset, or stream, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/<entity-type>/<entity-id>/metadata/tags
  
or, for a particular program of a specific application::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/tags

with the metadata tags returned as a JSON string in the return body::

  ["tag1", "tag2"]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<entity-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<entity-id>``
     - Name of the entity
   * - ``<app-id>``
     - Name of the application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services``, or ``workers``
   * - ``<program-id>``
     - Name of the program

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The properties requested were returned as a JSON string in the body of the response
   * - ``404 NOT FOUND``
     - The entity or program for which properties are being retrieved was not found
     
     
Removing Tags
-------------
To delete all user metadata tags for an application, dataset, or stream, submit an
HTTP DELETE request::

  DELETE <base-url>/namespaces/<namespace>/<entity-type>/<entity-id>/metadata/tags
  
or, for all user metadata tags of a particular program of a specific application::

  DELETE <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/tags

To delete a specific user metadata tag for an application, dataset, or stream, submit
an HTTP DELETE request with the tag::

  DELETE <base-url>/namespaces/<namespace>/<entity-type>/<entity-id>/metadata/tags/<tag>
  
or, for a particular user metadata tag of a program of a specific application::

  DELETE <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/tags/<tag>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<entity-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<entity-id>``
     - Name of the entity
   * - ``<app-id>``
     - Name of the application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services``, or ``workers``
   * - ``<program-id>``
     - Name of the program
   * - ``<tag>``
     - Metadata tag

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The method was successfully called, and the tags were deleted, or in the case of a
       specific tag, was either deleted or the tag was not present
   * - ``404 NOT FOUND``
     - The entity or program for which tags are being deleted was not found


.. _http-restful-api-metadata-searching:

Searching for Metadata
======================
To find which applications, datasets, or streams have a particular user metadata property or
user metadata tag, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/metadata/search?query=<term>&target=<entity-type>

Entities with the specified terms are returned as list of entity IDs::

  [
      {
          "entityId": {
              "id": {
                  "applicationId": "PurchaseHistory",
                  "namespace": {
                      "id": "default"
                  }
              },
              "type": "application"
          }
      },
      {
          "entityId": {
              "id": {
                  "application": {
                      "applicationId": "PurchaseHistory",
                      "namespace": {
                          "id": "default"
                      }
                  },
                  "id": "PurchaseFlow",
                  "type": "Flow"
              },
              "type": "program"
          }
      }
  ]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<entity-type>``
     - One of ``app``, ``dataset``, ``program``, or ``stream``
   * - ``<term>``
     - Query term, as described below

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Entity IDs of entities with the metadata properties specified were returned as a
       list of strings in the body of the response

.. rubric:: Query Terms

CDAP supports prefix-based search of metadata properties and tags. Search for specific tags by using
either a complete or partial name with an asterisk ``*``.

Search for properties and tags by specifying one of:

- a complete property key-value pair, separated by a colon, such as ``type:production``

- a complete property key with a partial value, such as ``type:prod*``

- a complete ``tags`` key with a complete or partial value, such as ``tags:prod*`` to search for tags only

- a complete or partial value, such as ``prod*``; this will return both properties and tags

Searches are case-insensitive.

.. _http-restful-api-metadata-lineage:

Viewing Lineages
================
To view the lineage of a dataset or stream, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/<entity-type>/<entity-id>/lineage?start=<start-ts>&end=<end-ts>&maxLevels=<max-levels>

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<entity-type>``
     - One of ``dataset`` or ``stream``
   * - ``<entity-id>``
     - Name of the ``dataset`` or ``stream``
   * - ``<start-ts>``
     - Starting time-stamp of lineage (inclusive), in seconds. Supports ``now``, ``now-1h``, etc. syntax.
   * - ``<end-ts>``
     - Ending time-stamp of lineage (exclusive), in seconds. Supports ``now``, ``now-1h``, etc. syntax.
   * - ``<max-levels>``
     - Maximum number of levels

See in the Metrics HTTP RESTful API :ref:`Querying by a Time Range <http-restful-api-metrics-time-range>`
for examples of the "now" time syntax.

The lineage will be returned as a JSON string in the body of the response. The number of
levels of the request (``<max-levels>``) determines how far back the provenance of the
data in the lineage chain is calculated, as described in the :ref:`Metadata and Lineage <metadata-lineage-lineage>`.

Lineage JSON consists of three main sections:

- **Relations:** contains information on data accessed by programs.
  Access type can be *read*, *write*, *both*, or *unknown*.
  It also contains the *runid* of the program that accessed the data, 
  and the specifics of any *component* of a program
  that also accessed the data. For example, a flowlet is a *component* of a flow.
- **Data:** contains Datasets or Streams that were accessed by programs.
- **Programs:** contains information on programs (flows, MapReduce, Spark, workers, etc.) 
  that accessed the data.

Here is an example, pretty-printed::

  {
      "start": 1442863938,
      "end": 1442881938,
      "relations": [
          {
              "data": "stream.default.purchaseStream",
              "program": "flows.default.PurchaseHistory.PurchaseFlow",
              "access": "read",
              "runs": [
                  "4b5d7891-60a7-11e5-a9b0-42010af01c4d"
              ],
              "components": [
                  "reader"
              ]
          },
          {
              "data": "dataset.default.purchases",
              "program": "flows.default.PurchaseHistory.PurchaseFlow",
              "access": "unknown",
              "runs": [
                  "4b5d7891-60a7-11e5-a9b0-42010af01c4d"
              ],
              "components": [
                  "collector"
              ]
          }
      ],
      "data": {
          "dataset.default.purchases": {
              "entityId": {
                  "id": {
                      "instanceId": "purchases",
                      "namespace": {
                          "id": "default"
                      }
                  },
                  "type": "datasetinstance"
              }
          },
          "stream.default.purchaseStream": {
              "entityId": {
                  "id": {
                      "namespace": {
                          "id": "default"
                      },
                      "streamName": "purchaseStream"
                  },
                  "type": "stream"
              }
          }
      },
      "programs": {
          "flows.default.PurchaseHistory.PurchaseFlow": {
              "entityId": {
                  "id": {
                      "application": {
                          "applicationId": "PurchaseHistory",
                          "namespace": {
                              "id": "default"
                          }
                      },
                      "id": "PurchaseFlow",
                      "type": "Flow"
                  },
                  "type": "program"
              }
          }
      }
  }

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Entities IDs of entities with the metadata properties specified were returned as a
       list of strings in the body of the response
   * - ``404 NOT FOUND``
     - No entities matching the specified query were found

.. _http-restful-api-metadata-run:

Retrieving Metadata for a Program Run
=====================================
At every run of a program, the metadata associated with the program, the application it is part of, and any datasets
and streams used by the program run are recorded. To retrieve the metadata for a program run, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/runs/<run-id>/metadata/tags

with the metadata returned as a JSON string in the return body::

  [
      {
          "entityId": {
              "id": {
                  "namespace": {
                      "id": "default"
                  },
                  "streamName": "purchaseStream"
              },
              "type": "stream"
          },
          "properties": {},
          "scope": "USER",
          "tags": []
      },
      {
          "entityId": {
              "id": {
                  "application": {
                      "applicationId": "PurchaseHistory",
                      "namespace": {
                          "id": "default"
                      }
                  },
                  "id": "PurchaseFlow",
                  "type": "Flow"
              },
              "type": "program"
          },
          "properties": {},
          "scope": "USER",
          "tags": [
              "flow-tag1"
          ]
      },
      {
          "entityId": {
              "id": {
                  "instanceId": "purchases",
                  "namespace": {
                      "id": "default"
                  }
              },
              "type": "datasetinstance"
          },
          "properties": {},
          "scope": "USER",
          "tags": []
      },
      {
          "entityId": {
              "id": {
                  "applicationId": "PurchaseHistory",
                  "namespace": {
                      "id": "default"
                  }
              },
              "type": "application"
          },
          "properties": {},
          "scope": "USER",
          "tags": [
              "app-tag1"
          ]
      }
  ]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the application
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``spark``, ``workflows``, ``services``, or ``workers``
   * - ``<program-id>``
     - Name of the program
   * - ``<run-id>``
     - Program run id

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The properties requested were returned as a JSON string in the body of the response
   * - ``404 NOT FOUND``
     - The entity, program, or run for which properties are being requested was not found
