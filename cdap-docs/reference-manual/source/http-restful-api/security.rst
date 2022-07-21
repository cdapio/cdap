.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2017 Cask Data, Inc.

.. _http-restful-api-security:

=========================
Security HTTP RESTful API
=========================

.. highlight:: console

Use the Security HTTP RESTful API to manage privileges (authorization) of users on CDAP
entities as well as manage secure storage.

The HTTP RESTful API is divided into:

- :ref:`Authorization <http-restful-api-authorization>`
- :ref:`Secure Storage <http-restful-api-secure-storage>`

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


.. _http-restful-api-authorization:

Authorization
=============

Use the CDAP Authorization HTTP RESTful API to grant, revoke, and list privileges on CDAP
entities. Details about authorization in CDAP can be found at :ref:`Admin' Manual:
Authorization <admin-authorization>`.

.. highlight:: json

In this API, a JSON-formatted body is used that contains the principal, the CDAP authorizable, and the privileges to
be granted::

  {
    "authorizable": {
      "entityType": "APPLICATION",
      "entityParts": {"NAMESPACE": "default", "APPLICATION": "application"}
    },
    "principal": {
      "name": "admin",
      "type": "ROLE"
    },
    "permissions": [
      {
        "name": "GET",
        "type": "STANDARD"
      },
      {
        "name": "EXECUTE",
        "type": "APPLICATION"
      }
    ]
  }

.. highlight:: console

In the above JSON body, the ``authorizable`` object is the JSON-serialized form of the CDAP
:cdap-java-source-github:`Authorizable <cdap-proto/src/main/java/io/cdap/cdap/proto/id/Authorizable.java>` class.
|---| for example, for applications, its entity type is APPLICATION and it can be constructed by the namespace and application name.
More info can be found at the :cdap-java-source-github:`DatasetId <cdap-proto/src/main/java/io/cdap/cdap/proto/id/Authorizable.java>`
class. In entity parts, the name of the entity can be represented using wildcard by including * and ? in the name.
For example, ``ns*`` represents all namespaces that starts with ``ns``.
``ns?`` represents all namespaces that starts with ``ns`` and follows by a single character.

The ``principal`` object refers to the principal that you want to grant the privileges to.
Principals have a ``name`` and a ``type``. The supported types are ``USER``, ``GROUP`` and ``ROLE``.

**Please note that** the REST endpoints have only been created for supporting :ref:`Apache Sentry <integrations:apache-sentry>`.

The ``permissions`` list contains the permissions you want to grant the ``principal`` on the
``entity``. The supported permission names are ``CREATE``, ``LIST``, ``GET``, ``UPDATE``, ``DELETE`` for
the type ``STANDARD``; ``PREVIEW``, ``EXECUTE`` for type ``APPLICATION`` and ``SET_OWNER``, ``IMPERSONATE``
for the type ``ACCESS``.

.. _http-restful-api-security-auth-grant:

Grant Privileges
----------------
You can grant privileges to a principal on a CDAP Entity by making an HTTP POST request to
the URL::

  POST /v3/security/authorization/privileges/grant

.. highlight:: json

with JSON-formatted body that contains the principal, the CDAP entity, and the permissions to
be granted, such as::

  {
    "authorizable": {
      "entityType": "APPLICATION",
      "entityParts": {"NAMESPACE": "default", "APPLICATION": "application"}
    },
    "principal": {
      "name": "admin",
      "type": "ROLE"
    },
    "permissions": [
      {
        "name": "GET",
        "type": "STANDARD"
      },
      {
        "name": "EXECUTE",
        "type": "APPLICATION"
      }
    ]
  }

.. highlight:: console

- Granting privileges is only supported for ``ROLE`` type.

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Privileges were successfully granted for the specified principal


.. _http-restful-api-security-auth-revoke:

Revoke Privileges
-----------------
You can revoke privileges for a principal on a CDAP Entity by making an HTTP POST request to the URL::

  POST /v3/security/authorization/privileges/revoke

.. highlight:: json

with JSON-formatted body that contains the principal, the CDAP entity and the permissions to be revoked::

  {
    "authorizable": {
      "entityType": "APPLICATION",
      "entityParts": {"NAMESPACE": "default", "APPLICATION": "application"}
    },
    "principal": {
      "name": "admin",
      "type": "ROLE"
    },
    "permissions": [
      {
        "name": "GET",
        "type": "STANDARD"
      },
      {
        "name": "EXECUTE",
        "type": "APPLICATION"
      }
    ]
  }

.. highlight:: console

The ``authorizable`` object is mandatory in a revoke request.

- If both ``principal`` and ``permissions`` are not provided, then the API revokes all
  privileges on the specified entity for all principals.
- If ``authorizable`` and ``principal`` are provided, but ``permissions`` is not, the API revokes
  all permissions on the specified entity for the specified principal.
- Revoking privileges is only supported for ``ROLE`` type.

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Privileges were successfully revoked


.. _http-restful-api-security-auth-list:

List Privileges
---------------
You can list all privileges for a principal on all CDAP entities by making an HTTP GET request to the URL::

  GET /v3/security/authorization/<principal-type>/<principal-name>/privileges


.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``principal-type``
     - The principal type, one of ``USER``, ``GROUP``, or ``ROLE``
   * - ``principal-name``
     - Name of the principal

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Privileges were successfully listed for the specified principal

.. highlight:: json

This will return a JSON array that lists each privilege for the principal with its ``authorizable`` and ``permission``.
Example output (pretty-printed)::

  [
    {
      "authorizable": {
        "entityType": "DATASET",
        "entityParts": {"NAMESPACE": "default", "DATASET": "dataset"}
      },
      "permission": {"type": "STANDARD", "name": "UPDATE"}
    },
    {
      "authorizable": {
        "entityType": "NAMESPACE",
        "entityParts": {"NAMESPACE": "default"}
      },
      "permission": {"type": "STANDARD", "name": "GET"}
    },
    {
      "authorizable": {
        "entityType": "PROGRAM",
        "entityParts":{"NAMESPACE": "default", "APPLICATION": "SportResults", "PROGRAM": "service.UploadService"}
      },
      "permission": {"type": "APPLICATION", "name": "EXECUTE"}
    }
  ]

.. highlight:: console

- Listing privileges are supported for ``USER``, ``GROUP`` and ``ROLE`` type.


.. _http-restful-api-secure-storage:

Secure Storage
==============
Use the Secure Storage HTTP RESTful API to create, retrieve, and delete secure keys.
Details about secure storage and secure keys in CDAP can be found in :ref:`Administration
Manual: Secure Storage <admin-secure-storage>`.

**Note:** In CDAP 3.5.0, encryption and decryption of the contents only happens at the
secure store, not while the data is transitting to the secure store. In a later version of
CDAP, all transport involving secure keys will be secured using SSL.

.. _http-restful-api-security-secure-storage-add:

Add a Secure Key
----------------
You can add a secure key to secure storage by making an HTTP PUT request to the URL::

  PUT /v3/namespaces/<namespace-id>/securekeys/<secure-key-id>

.. highlight:: json

with a JSON-formatted body that contains the description of the key, the data to be stored
under the key, and a map of properties associated with the key::

  {
    "description": "Example Secure Key",
    "data": "<secure-contents>",
    "properties": {
      "<property-key>": "<property-value>"
    }
  }

.. highlight:: console

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     -  Namespace ID
   * - ``secure-key-id``
     - Name of the key to add to secure storage
   * - ``secure-contents``
     - String data to be added under the key
   * - ``property-key``
     - Name of a property key to associate with the secure key
   * - ``property-value``
     - Value associated with the property key


.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The secure key was successfully added to secure storage
   * - ``400 BAD REQUEST``
     - An incorrectly-formatted body was sent with the request or the ``data`` field in
       the request was empty or not present
   * - ``404 NOT FOUND``
     - The namespace specified in the request does not exist


.. _http-restful-api-security-secure-storage-retrieve:

Retrieve a Secure Key
---------------------

You can retrieve a secure key from secure storage by making an HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/securekeys/<secure-key-id>

with the data of the secure key returned as text, passed in the response body.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     -  Namespace ID
   * - ``secure-key-id``
     - Name of the key to retrieve from secure storage

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The secure key was successfully retrieved
   * - ``404 NOT FOUND``
     - The namespace specified in the request does not exist or the secure key with that
       name does not exist in that namespace

.. _http-restful-api-security-secure-storage-retrieve-metadata:

Retrieve the Metadata for a Secure Key
--------------------------------------
You can retrieve just the metadata for a secure key from secure storage by making an HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/securekeys/<secure-key-id>/metadata

.. highlight:: json

with the metadata of the secure key returned as a JSON object |---| name (the
``secure-key-id``), description, created timestamp, and the map of properties
|---| passed in the response body, shown here pretty-printed::

  {
    "name": "<secure-key-id>",
    "description": "Example Secure Key",
    "createdEpochMs": 1471718010326,
    "properties": {
      "property-key": "property-value"
    }
  }

.. highlight:: console

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     -  Namespace ID
   * - ``secure-key-id``
     - Name of the key to retrieve from secure storage

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Metadata for the secure key was successfully retrieved
   * - ``404 NOT FOUND``
     - The namespace specified in the request does not exist or a secure key by the
       specified name does not exist in the specified namespace


.. _http-restful-api-security-secure-storage-list:

List all Secure Keys
--------------------
You can retrieve all the keys in a namespace from secure storage by making an HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/securekeys

with the secure keys in the namespace returned as a JSON string map of string-string pairs, passed
in the response body (shown here pretty-printed)::

  {
    secure-key-id-1: secure key description,
    secure-key-id-2: secure key description,
    ...
  }

.. highlight:: json-ellipsis

such as (depending on what was stored)::

  {
    "securekey": "secure key description",
    "password": "password description",
    ...
  }

.. highlight:: console

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     -  Namespace ID

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The keys were successfully retrieved
   * - ``404 NOT FOUND``
     - The namespace specified in the request does not exist


.. _http-restful-api-security-secure-storage-remove:

Remove a Secure Key
-------------------
You can remove a secure key from secure storage by making an HTTP DELETE request to the URL::

  DELETE /v3/namespaces/<namespace-id>/securekeys/<secure-key-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     -  Namespace ID
   * - ``secure-key-id``
     - Name of the key to remove from secure storage

.. rubric:: HTTP Responses

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - The key was successfully removed
   * - ``404 NOT FOUND``
     - The namespace specified in the request does not exist or a secure key by the
       specified name does not exist in the specified namespace
