.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _users-etl-operations:

========================
Operating An ETL Adapter
========================

Introduction
============

Lifecycle
=========

These lifecycle operations can be performed on Adapters (including the ETL Adapters) once they are created
and deployed:

Status
------
You can fetch the status of an adapter::

  /v3/namespaces/{namespace-id}/adapters/{adapter-id}/status

Possible responses::

  200 ⇒	STOPPED, STARTED
  404 ⇒ Adapter not found

Start an Adapter
----------------
Once you have created an adapter or stopped an already running adapter, you can start it using::

  /v3/namespaces/{namespace-id}/adapters/{adapter-id}/start -X POST

Stop an Adapter
---------------

Once you have started an adapter, you can stop the adapter using::

  /v3/namespaces/{namespace-id}/adapters/{adapter-id}/stop -X POST

Possible responses for both start and stop::

  200 ⇒ STARTED
  404 ⇒ Adapter not found
  409, 500 ⇒ Internal error
 
Deleting an Adapter
-------------------
A stopped adapter can be deleted using::

  /v3/namespaces/{namespace-id}/adapters/{adapter-id} -X DELETE

Possible responses::

  200 ⇒ Deleted successfully
  403 ⇒ Forbidden. Running adapter cannot be deleted
  404 ⇒ Adapter not found
  500 ⇒ Internal error


Adapter Configuration
---------------------
You can retrieve the configuration of an Adaptsr with::

  GET /v3/namespaces/{namespace-id}/adapters/{adapter-id}

Possible responses::

  200 OK ⇒ Sample Response???
  404 ⇒ Adapter Not found

Runs of an Adapter
------------------
You can retrieve the runs of an Adapter with::

  GET /v3/namespaces/{namespace-id}/adapters/{adapter-id}/runs
  
Query Params can be used to filter the responses.

Sample Response::

  [
      {
          "adapter": "streamAdapter",
          "runid": "92c41dd4-eca6-11e4-8ded-ba2d86f39a8e",
          "start": 1430116089,
          "status": "RUNNING"
      },
      {
          "adapter": "streamAdapter",
          "end": 1430116088,
          "runid": "8fd0a763-eca6-11e4-bd9f-ba2d86f39a8e",
          "start": 1430116084,
          "status": "KILLED"
      },
      {
          "adapter": "streamAdapter",
          "end": 1430116082,
          "runid": "879e6af2-eca6-11e4-8ae1-ba2d86f39a8e",
          "start": 1430116071,
          "status": "KILLED"
      }
  ]


Run Record of an Adapter
------------------------
You can retrieve the run record of an Adapter with::

  GET /v3/namespaces/{namespace-id}/adapter/{adapter-id}/runs/{run-id}

Possible Responses::

  200 OK ⇒ Info displayed

Sample Response::

  {
      "adapter": "streamAdapter",
      "runid": "92c41dd4-eca6-11e4-8ded-ba2d86f39a8e",
      "start": 1430116089,
      "status": "RUNNING"
  }


Logs of an Adapter
------------------
You can retrieve the logs of an Adapter with::

  /v3/namespaces/{namespace-id}/adapter/{adapter-id}/logs
  
Query Params can be used to filter the responses.

Metrics of an Adapter
---------------------
You can retrieve the metrics of an Adapter with::

  [TBD]
