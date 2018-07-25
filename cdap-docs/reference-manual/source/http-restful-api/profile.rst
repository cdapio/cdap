.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2018 Cask Data, Inc.

.. _http-restful-api-profile:

========================
Profile HTTP RESTful API
========================

.. highlight:: console

Use the CDAP Profile HTTP RESTful API to create profiles, list available profiles, and
retrieve information about profiles


.. Base URL explanation
.. --------------------
.. include:: base-url.txt


.. _http-restful-api-profile-write:

Write a Profile
===============
A system profile can be created or updated with an HTTP PUT method to the URL::

  PUT /v3/profiles/<profile-name>

A user profile can be created or updated with an HTTP PUT method to the URL::

  PUT /v3/namespaces/<namespace-id>/profiles/<profile-name>

The request body must be a JSON object that specifies the profile details.

.. container:: highlight

  .. parsed-literal::
    |$| PUT /v3/namespace/default/profiles/dataproc -d
    {
      "label": "Dataproc",
      "description": "My Dataproc profile",
      "provisioner": {
        "name": "gcp-dataproc",
        "properties": [
          {
            "name": "projectId",
            "value": "my project id",
            "isEditable": false
          },
          ...
        ]
      }
    }

.. _http-restful-api-profile-list:

List Profiles
=============
To list all system profiles, submit an HTTP GET request::

  GET /v3/profiles

To list all user profiles, submit an HTTP GET request::

  GET /v3/namespaces/<namespace-id>/profiles[?includeSystem=true]

This will return a JSON array that lists each profile. For the user profiles endpoint, if
the `includeSystem` query param is set to `true`, all system profiles will also be returned
in the response.

.. container:: highlight

  .. parsed-literal::
    |$| GET /v3/namespaces/default/profiles
    [
      {
        "name": "dataproc",
        "label": "Dataproc",
        "description": "My Dataproc Profile",
        "scope": "SYSTEM",
        "status": "ENABLED",
        "created": 1234567890,
        "provisioner": {
          "name": "gcp-dataproc",
          "properties": [
            {
              "name": "projectId",
              "value": "my project id",
              "isEditable": false
            },
            ...
          ]          
        }
      },
      ...
    ]

.. _http-restful-api-profile-detail:

Retrieve Profile Details
========================
To retrieve details about a system profile, submit an HTTP GET request::

  GET /v3/profiles/<profile-name>

To retrieve details about a user profile, submit an HTTP GET request::

  GET /v3/namespaces/<namespace-id>/profiles/<profile-name>

This will return a JSON object that contains details about the profile.

.. container:: highlight

  .. parsed-literal::
    |$| GET /v3/namespaces/default/profiles/dataproc
    {
      "name": "dataproc",
      "label": "Dataproc",
      "description": "My Dataproc Profile",
      "scope": "SYSTEM",
      "status": "ENABLED",
      "created": 1234567890,
      "provisioner": {
        "name": "gcp-dataproc",
        "properties": [
          {
            "name": "projectId",
            "value": "my project id",
            "isEditable": false
          },
          ...
        ]          
      }
    }

.. _http-restful-api-profile-disable:

Disable Profile
===============
To disable a system profile, submit an HTTP POST request::

  POST /v3/profiles/<profile-name>/disable

To disable a user profile, submit an HTTP POST request::

  POST /v3/namespaces/<namespace-id>/profiles/<profile-name>/disable

.. _http-restful-api-profile-enable:

Enable Profile
==============
To enable a system profile, submit an HTTP POST request::

  POST /v3/profiles/<profile-name>/enable

To enable a user profile, submit an HTTP POST request::

  POST /v3/namespaces/<namespace-id>/profiles/<profile-name>/enable

.. _http-restful-api-profile-delete:

Delete Profile
==============
To delete a system profile, submit an HTTP DELETE request::

  DELETE /v3/profiles/<profile-name>

To delete a user profile, submit an HTTP POST request::

  DELETE /v3/namespaces/<namespace-id>/profiles/<profile-name>

In order to delete a profile, the profile must be disabled, it must
not be assigned to any entity, and it must not be in use by any active
program runs. If any of these conditions is not met, a 409 will be returned.

