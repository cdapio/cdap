.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _http-restful-api-dataset:

========================
Dataset HTTP RESTful API
========================

.. highlight:: console

The CDAP Dataset HTTP RESTful API allows you to interact with datasets through HTTP. You
can list, create, delete, and truncate datasets. For details on datasets, see the
:ref:`CDAP Components, datasets section <datasets-index>`

.. Base URL explanation
.. --------------------
.. include:: base-url.txt


Listing all Datasets
====================

You can list all datasets in CDAP by issuing an HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/data/datasets

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID

.. highlight:: json-ellipsis

The response body will contain a JSON-formatted list of the existing datasets::

  {
     "name":"cdap.user.purchases",
     "type":"co.cask.cdap.api.dataset.lib.ObjectStore",
     "description" : "Purchases Dataset",
     "properties":{
        "schema":"...",
        "type":"..."
     },
     "datasetSpecs":{
        ...
     }
   }

.. highlight:: console

.. _http-restful-api-dataset-creating:

Creating a Dataset
==================

You can create a dataset by issuing an HTTP PUT request to the URL::

  PUT /v3/namespaces/<namespace-id>/data/datasets/<dataset-name>

.. highlight:: json-ellipsis

with JSON-formatted name of the dataset type, properties, and description in a body::

  {
     "typeName":"<type-name>",
     "properties":{
        "<properties>"
      },
     "description":"Dataset Description"
     "principal":"user/example.net@examplekdc.net"
  }

.. highlight:: console

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``dataset-name``
     - Name of the new dataset
   * - ``type-name``
     - Type of the new dataset
   * - ``properties``
     - Dataset properties, map of String to String
   * - ``description``
     - Dataset description
   * - ``principal``
     - Kerberos principal with which the dataset should be created

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Requested dataset was successfully created
   * - ``403 Forbidden``
     - The dataset already exist with a different Kerberos principal
   * - ``404 Not Found``
     - Requested dataset type was not found
   * - ``409 Conflict``
     - Dataset with the same name already exists

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Request
     - ``PUT /v3/namespaces/default/data/datasets/mydataset``
   * - Body
     - ``{"typeName":"co.cask.cdap.api.dataset.table.Table",`` ``"properties":{"dataset.table.ttl":"3600"},``
       ``"description":"My Dataset Description",`` ``"principal":"user/somehost.net@somekdc.net"}``
   * - Description
     - Creates a dataset named *mydataset* of the type ``Table`` in the namespace *default*
       with the time-to-live property set to 1 hour and a description of ``My Dataset Description``

.. _http-restful-api-dataset-properties:

Properties of an Existing Dataset
=================================

You can retrieve the properties with which a dataset was created or last updated by issuing an HTTP GET request to
the URL::

	GET /v3/namespaces/<namespace-id>/data/datasets/<dataset-name>/properties

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``dataset-name``
     - Name of the existing dataset

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Requested dataset was successfully updated
   * - ``404 Not Found``
     - Requested dataset instance was not found

.. highlight:: json-ellipsis

The response |---| if successful |---| will contain the JSON-formatted properties::

  {
     "key1":"value1",
     "key2":"value2",
     ...
  }

.. highlight:: console

Note that this will return the original properties that were submitted when the dataset was created or updated.
You can use these properties to create a clone of the dataset, or as a basis for updating some properties of this
dataset without modifying the remaining properties.

.. _http-restful-api-dataset-meta:

Metadata of an Existing Dataset
===============================

You can retrieve the metadata with which a dataset was created by issuing an HTTP GET
request to the URL::

	GET /v3/namespaces/<namespace-id>/data/datasets/<dataset-name>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``dataset-name``
     - Name of the existing dataset

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Metadata for the requested dataset instance was successfully returned
   * - ``404 Not Found``
     - Requested dataset instance was not found

.. highlight:: json-ellipsis

The response body will contain JSON-formatted metadata of the existing dataset::

   {
     "spec": {
       "name": "ownedDataset",
       "type": "datasetType1",
       "originalProperties": {},
       "properties": {},
       "datasetSpecs": {}
     },
     "type": {
       "name": "datasetType1",
       "modules": [
         {
           "name": "module1",
           "className": "co.cask.cdap.data2.datafabric.dataset.service.TestModule1",
           "jarLocationPath": "/path/data/module1/archive/module1.jar",
           "types": [
             "datasetType1"
           ],
           "usesModules": [],
           "usedByModules": []
         }
       ]
     },
     "principal": "user/somehost.net@somekdc.net"
   }

.. highlight:: console

.. _http-restful-api-dataset-updating:

Updating an Existing Dataset
============================

You can update an existing dataset's table and properties by issuing an HTTP PUT request to the URL::

	PUT /v3/namespaces/<namespace-id>/data/datasets/<dataset-name>/properties

.. highlight:: json-ellipsis

with JSON-formatted properties in the body::

  {
     "key1":"value1",
     "key2":"value2",
     ...
  }

.. highlight:: console

**Notes:** 

- The dataset must already exist.
- The properties given in this request replace all existing properties; that is, if you
  have set other properties for this table, such as time-to-live (``dataset.table.ttl``),
  you must also include those properties in the update request.
- You can retrieve the existing properties using the :ref:`http-restful-api-dataset-properties`
  and use that as the basis for constructing your request. 

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``dataset-name``
     - Name of the existing dataset

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Requested dataset was successfully updated
   * - ``404 Not Found``
     - Requested dataset instance was not found

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Request
     - ``PUT /v3/namespaces/default/data/datasets/mydataset/properties``
   * - Body
     - ``{"dataset.table.ttl":"7200"}``
   * - Description
     - For the *mydataset* of type ``Table`` of the namespace *default*, update the dataset
       and its time-to-live property to 2 hours


Deleting a Dataset
==================

You can delete a dataset by issuing an HTTP DELETE request to the URL::

  DELETE /v3/namespaces/<namespace-id>/data/datasets/<dataset-name>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``dataset-name``
     - Dataset name

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Dataset was successfully deleted
   * - ``404 Not Found``
     - Dataset named *dataset-name* could not be found

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Request
     - ``DELETE /v3/namespaces/default/data/datasets/mydataset``
   * - Description
     - Deletes the dataset *mydataset* in the namespace *default*

.. _http-restful-api-dataset-deleting-all:

Deleting all Datasets
=====================

If the property ``enable.unrecoverable.reset`` in ``cdap-site.xml`` is set to ``true``, 
you can delete all Datasets (in a namespace) by issuing an HTTP DELETE request to the URL::

  DELETE /v3/unrecoverable/namespaces/<namespace-id>/datasets

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - All Datasets were successfully deleted
   * - ``403 Forbidden``
     - Property to enable unrecoverable methods is not enabled
   * - ``409 Conflict``
     - Programs are currently running in the namespace

This command will only work if all programs in the namespace are not running.

If the property ``enable.unrecoverable.reset`` in ``cdap-site.xml`` is not set to
``true``, this operation will return a Status Code ``403 Forbidden``. Note that this
operation can only be performed if all programs are stopped. If there's at least one
program that is running, this operation will return a Status Code ``409 Conflict``.

This method must be exercised with extreme caution, as there is no recovery from it.

Truncating a Dataset
====================

You can truncate a dataset by issuing an HTTP POST request to the URL::

  POST /v3/namespaces/<namespace-id>/data/datasets/<dataset-name>/admin/truncate

This will clear the existing data from the dataset. This cannot be undone.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``dataset-name``
     - Dataset name

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Dataset was successfully truncated

Datasets used by an Application
===============================

You can retrieve a list of datasets used by an application by issuing a HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/datasets

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Application ID

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Request was successful

Datasets used by a Program
==========================

You can retrieve a list of datasets used by a program by issuing a HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/datasets

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Application ID
   * - ``program-type``
     - Program type, one of ``flows``, ``mapreduce``, ``services``, ``spark``, or ``workflows``
   * - ``program-id``
     - Program ID

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Request was successful

Programs using a Dataset
========================

You can retrieve a list of programs that are using a dataset by issuing a HTTP GET request to the URL::

  GET /v3/namespaces/<namespace-id>/data/datasets/<dataset-id>/programs

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``dataset-id``
     - Dataset ID

.. rubric:: HTTP Responses
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Status Codes
     - Description
   * - ``200 OK``
     - Request was successful
