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
of applications and datasets in CDAP.

Metadata consists of properties (a list of key-value pairs) and tags (a list of keys).
Metadata and their use are described in the :ref:`Developers' Manual: Metadata <metadata>`.

The HTTP RESTful API is divided into two sections,
:ref:`metadata properties <http-restful-api-metadata-properties>` and
:ref:`metadata tags <http-restful-api-metadata-tags>`.

In this API, ``<base-url>`` is as described under :ref:`Conventions
<http-restful-api-conventions>`. Metadata keys and tags must conform to the CDAP
:ref:`supported characters <supported-characters>`, and are limited to 50 characters in
length. Metadata values are limited to 10K bytes in length.


.. _http-restful-api-metadata-properties:

Metadata Properties
===================

Annotating Properties
---------------------
To annotate user metadata properties for an application, dataset, or stream, submit an HTTP POST request::

  POST /v3/namespaces/<namespace>/<element-type>/<element-id>/metadata/properties
  
or, for a particular program of a specific application::

  POST /v3/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/properties

with the metadata properties, as a JSON string map of string-string pairs, passed in the
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
   * - ``<element-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<element-id>``
     - Name of the element
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
     - The entity (element or program) for which properties are being set was not found
     

Retrieving Properties
---------------------
To retrieve user metadata properties for an application, dataset, or stream, submit an HTTP GET request::

  GET /v3/namespaces/<namespace>/<element-type>/<element-id>/metadata/properties
  
or, for a particular program of a specific application::

  GET /v3/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/properties

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
   * - ``<element-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<element-id>``
     - Name of the element
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
     - The entity (element or program) for which properties are being retrieved was not found


Deleting Properties
-------------------
To delete all user metadata properties for an application, dataset, or stream, submit an
HTTP DELETE request::

  DELETE /v3/namespaces/<namespace>/<element-type>/<element-id>/metadata/properties
  
or, for all user metadata properties of a particular program of a specific application::

  DELETE /v3/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/properties

To delete a specific user metadata property for an application, dataset, or stream, submit
an HTTP DELETE request with the property key::

  DELETE /v3/namespaces/<namespace>/<element-type>/<element-id>/metadata/properties/<key>
  
or, for a particular user metadata property of a program of a specific application::

  DELETE /v3/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/properties/<key>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<element-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<element-id>``
     - Name of the element
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
     - The entity (element or program) for which properties are being deleted was not found


.. _http-restful-api-metadata-tags:

Metadata Tags
=============

Adding Tags
-----------
To add user metadata tags for an application, dataset, or stream, submit an HTTP POST request::

  POST /v3/namespaces/<namespace>/<element-type>/<element-id>/metadata/tags
  
or, for a particular program of a specific application::

  POST /v3/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/tags

with the metadata tags, as a list of strings, passed in the JSON request body::

  ["tag1", "tag2"]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<element-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<element-id>``
     - Name of the element
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
     - The entity (element or program) for which tags are being set was not found


Retrieving Tags
---------------
To retrieve user metadata tags for an application, dataset, or stream, submit an HTTP GET request::

  GET /v3/namespaces/<namespace>/<element-type>/<element-id>/metadata/tags
  
or, for a particular program of a specific application::

  GET /v3/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/tags

with the metadata tags returned as a JSON string in the return body::

  ["tag1", "tag2"]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<element-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<element-id>``
     - Name of the element
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
     - The entity (element or program) for which properties are being retreived was not found
     
     
Removing Tags
-------------
To delete all user metadata tags for an application, dataset, or stream, submit an
HTTP DELETE request::

  DELETE /v3/namespaces/<namespace>/<element-type>/<element-id>/metadata/tags
  
or, for all user metadata tags of a particular program of a specific application::

  DELETE /v3/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/tags

To delete a specific user metadata tag for an application, dataset, or stream, submit
an HTTP DELETE request with the tag::

  DELETE /v3/namespaces/<namespace>/<element-type>/<element-id>/metadata/properties/<tag>
  
or, for a particular user metadata tag of a program of a specific application::

  DELETE /v3/namespaces/<namespace>/apps/<app-id>/<program-type>/<program-id>/metadata/properties/<tag>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<element-type>``
     - One of ``apps``, ``datasets``, or ``streams``
   * - ``<element-id>``
     - Name of the element
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
     - The entity (element or program) for which tags are being deleted was not found


.. _http-restful-api-metadata-searching-viewing:

Searching and Viewing
=====================

Searching for Properties and Tags
---------------------------------
To find which applications, datasets, or streams have a particular metadata property or
metadata tag, submit an HTTP GET request::

  GET /v3/namespaces/<namespace>/metadata/search?query=<term>&target=<element-type>

Entities with the specified terms are returned as list of entity IDs::

  ["entity1", "entity2"]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<element-type>``
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
     - Entities IDs of entities with the metadata properties specified were returned as a
       list of strings in the body of the response
   * - ``404 NOT FOUND``
     - No entities matching the specified query were found

.. rubric:: Query Terms

CDAP supports prefix-based search of metadata properties and tags. Search for specific tags by using
either a complete or partial tag name (with the remainder specified by an asterisk ``*``). 

Search for properties by specifying one of:

- a complete property key-value pair, separated by a colon, such as ``type:production``
- a complete key with a partial value, such as ``type:prod*`` or ``type:*``
- a complete or partial value, such as ``prod*``


Viewing Lineages
----------------
To view the lineage of a dataset or stream, submit an HTTP GET request::

  GET /v3/namespaces/<namespace>/<element-type>/<element-id>/lineage?start=<start-ts>&end=<end-ts>&maxLevels=<max-levels>

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<element-type>``
     - One of ``dataset`` or ``stream``
   * - ``<element-id>``
     - Name of the ``dataset`` or ``stream``
   * - ``<start-ts>``
     - Starting time-stamp of lineage, in seconds
   * - ``<end-ts>``
     - Ending time-stamp of lineage, in seconds
   * - ``<max-levels>``
     - Maximum number of levels
     
The lineage will be returned as a JSON string in the body of the response. Here is an example, pretty-printed::

  {
    "start": "1441310434000",
    "end": "1441320599000",
   
    "relations":
    [
      {
        "data": "stream.default.purchaseStream",
        "program": "flow.default.PurchaseHistory.PurchaseFlow",
        "access": "read",
        "runs": ["283-afsd032-adsf90", "283-0rwedfk-09wrff"],
        "component": ["reader"]
      },
      ...,
      {
        "data": "dataset.default.history",
        "program": "service.default.PurchaseHistory.PurchaseHistoryService",
        "runs": ["283-zsed032-adsf90"]
      }
    ],
     
    "programs":
    {
      "flow.default.PurchaseHistory.PurchaseFlow":
      {
        "id":
        {
          "namespace": "default",
          "application": "PurchaseHistory",
          "type": "flow",
          "id": "PurchaseFlow"
        }
      },
      ...,
      "service.default.PurchaseHistory.PurchaseHistoryService":
      {
        "id":
        {
          "namespace": "default",
          "application": "PurchaseHistory",
          "type": "flow",
          "id": "PurchaseHistoryService"
        }
      }
    },
   
    "data":
    {
      "dataset.default.frequentCustomers":
      {
        "id":
        {
          "namespace": "default",
          "type": "dataset",
          "id": "frequentCustomers"
        }
      },
      ...,
      "stream.default.purchaseStream":
      {
        "id":
        {
          "namespace": "default",
          "type": "stream",
          "id": "purchaseStream"
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
