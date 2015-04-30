.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-adapter:

===========================================================
Application Template and Adapter HTTP RESTful API
===========================================================

Use the CDAP Application Template and Adapter HTTP API to obtain a list of available
Application Templates and Plugins, and create, delete, and manage the lifecycle of
Adapters.

Note that the ETL Templates are a type of Application Template, specifically designed for
creating ETL Adapters. See the Developers’ Manual Building Blocks section on ETL Adapters
for additional information.[link]


.. highlight:: console

Application Templates
=====================

Available Application Templates 
-------------------------------
To retrieve a list of available Application Templates, submit an HTTP GET request::

  GET <base-url>/templates/

This will return a JSON String map that lists each Application Template with its name,
description and type of program that it creates. Example output (pretty-printed)::

  [
    {
      "name": "etlBatch",
      "description": "Batch Extract-Transform-Load (ETL) Adapter",
      "programType": "Workflow"
    },
    {
      "name": "etlRealtime",
      "description": "Realtime Extract-Transform-Load (ETL) Adapter",
      "programType": "Worker"
    }
  ]


Template Details
-----------------
To retrieve the details of a particular Application Templates, submit an HTTP GET request::

  GET <base-url>/templates/<template-id>
  
where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<template-id>``
     - Name of the Application Template, such as ``etlBatch`` or ``etlRealtime``
  
This will return a JSON String map that lists the details of the Application Template.
Example output of the ``etlBatch`` Application Template (pretty-printed)::

  $ GET <base-url>/templates/etlBatch

  {
    "extensions": [
        "sink",
        "source",
        "transform"
    ],
    "name": "etlBatch",
    "description": "Batch Extract-Transform-Load (ETL) Adapter",
    "programType": "Workflow"
  }

``extensions`` is an array of the types of Plugins that are available for use by the
Application Template.


Template Extensions 
-------------------
To retrieve a list of all the extensions of a particular type for an Application
Template, submit an HTTP GET request::

  GET <base-url>/templates/<template-id>/extensions/<extension-type>
  
where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<template-id>``
     - Name of the Application Template, such as ``etlBatch`` or ``etlRealtime``
   * - ``<extension-type>``
     - Extension type, such as (for ETL Templates) ``source``, ``sink``, or ``transform``

This will return a JSON String map that lists all the extensions of particular type for
that Application Template, including their name, description, and the source files that
contain the extension.

Example output for the ``source`` extensions of the ``etlBatch`` Application Template
(pretty-printed and reformatted to fit)::

  $ GET <base-url>/templates/etlBatch/extensions/source

  [
    {
      "template": {
        "name": "etlBatch",
        "description": "Batch Extract-Transform-Load (ETL) Adapter",
        "programType": "Workflow"
      },
      "source": {
        "fileName": "cdap-etl-lib-3.0.0-SNAPSHOT-batch.jar",
        "name": "cdap-etl-lib",
        "version": {
          "version": "3.0.0-SNAPSHOT-batch",
          "major": 3,
          "minor": 0,
          "fix": 0,
          "suffix": "SNAPSHOT-batch"
        }
      },
      "type": "source",
      "name": "Database",
      "description": "Batch source for a database."
    },
    {
      "template": {
        "name": "etlBatch",
        "description": "Batch Extract-Transform-Load (ETL) Adapter",
        "programType": "Workflow"
      },
      "source": {
        "fileName": "cdap-etl-lib-3.0.0-SNAPSHOT-batch.jar",
        "name": "cdap-etl-lib",
        "version": {
          "version": "3.0.0-SNAPSHOT-batch",
          "major": 3,
          "minor": 0,
          "fix": 0,
          "suffix": "SNAPSHOT-batch"
        }
      },
      "type": "source",
      "name": "KVTable",
      "description": "CDAP KeyValue Table Dataset Batch Source. Outputs records with a 
        'key' field and a 'value' field. Both fields are of type bytes."
    },
    {
      "template": {
        "name": "etlBatch",
        "description": "Batch Extract-Transform-Load (ETL) Adapter",
        "programType": "Workflow"
      },
      "source": {
        "fileName": "cdap-etl-lib-3.0.0-SNAPSHOT-batch.jar",
        "name": "cdap-etl-lib",
        "version": {
          "version": "3.0.0-SNAPSHOT-batch",
          "major": 3,
          "minor": 0,
          "fix": 0,
          "suffix": "SNAPSHOT-batch"
        }
      },
      "type": "source",
      "name": "Stream",
      "description": "Batch source for a stream. If a format is given, any property 
        prefixed with 'format.setting.' will be passed to the format. For example, if a 
        property with key 'format.setting.delimiter' and value '|' is given, the setting 
        'delimiter' with value '|' will be passed to the format."
    },
    {
      "template": {
        "name": "etlBatch",
        "description": "Batch Extract-Transform-Load (ETL) Adapter",
        "programType": "Workflow"
      },
      "source": {
        "fileName": "cdap-etl-lib-3.0.0-SNAPSHOT-batch.jar",
        "name": "cdap-etl-lib",
        "version": {
          "version": "3.0.0-SNAPSHOT-batch",
          "major": 3,
          "minor": 0,
          "fix": 0,
          "suffix": "SNAPSHOT-batch"
        }
      },
      "type": "source",
      "name": "Table",
      "description": "CDAP Table Dataset Batch Source"
    }
  ]


Details of an Extension (Plugin)
--------------------------------
To retrieve the details of an extension (plugin) used in an Application Template, submit
an HTTP GET request::

  GET <base-url>/templates/<template-id>/extensions/<extension-type>/plugins/<plugin-id>
  
where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<template-id>``
     - Name of the Application Template, such as ``etlBatch`` or ``etlRealtime``
   * - ``<extension-type>``
     - Extension type, such as (for ETL Templates) ``source``, ``sink``, or ``transform``
   * - ``<plugin-id>``
     - Plugin name

This will return a JSON String map that lists the details of the Plugin. This is the
information needed to configure the plugin when used in creating an Adapter.

Example output for the ``Database`` plugin of type ``source`` of the ``etlBatch``
Application Template (pretty-printed and reformatted to fit)::

  $ GET <base-url>/templates/etlBatch/extensions/source/plugins/Database

  [
    {
      "className": "co.cask.cdap.templates.etl.batch.sources.DBSource",
      "properties": {
        "jdbcPluginType": {
          "name": "jdbcPluginType",
          "description": "Type of the JDBC plugin to use. This is the value of the 'type' 
          key defined in the json file for the JDBC plugin. Defaults to 'jdbc'.",
          "type": "string",
          "required": false
        },
        "tableName": {
          "name": "tableName",
          "description": "Table name to export to.",
          "type": "string",
          "required": true
        },
        "jdbcPluginName": {
          "name": "jdbcPluginName",
          "description": "Name of the JDBC plugin to use. This is the value of the 'name' 
          key defined in the json file for the JDBC plugin. Defaults to 'jdbc'.",
          "type": "string",
          "required": false
        },
        "driverClass": {
          "name": "driverClass",
          "description": "Driver class to connect to the database.",
          "type": "string",
          "required": true
        },
        "importQuery": {
          "name": "importQuery",
          "description": "The SELECT query to use to import data from the specified table. 
          You can specify an arbitrary number of columns to import, or import all columns 
          using *. You can also specify a number of WHERE clauses or ORDER BY clauses. 
          However, LIMIT and OFFSET clauses should not be used in this query.",
          "type": "string",
          "required": true
        },
        "connectionString": {
          "name": "connectionString",
          "description": "JDBC connection string including database name.",
          "type": "string",
          "required": true
        },
        "password": {
          "name": "password",
          "description": "Password to use to connect to the specified database. Required 
          for databases that need authentication. Optional for databases that do not 
          require authentication.",
          "type": "string",
          "required": false
        },
        "user": {
          "name": "user",
          "description": "User to use to connect to the specified database. Required for 
          databases that need authentication. Optional for databases that do not require 
          authentication.",
          "type": "string",
          "required": false
        },
        "countQuery": {
          "name": "countQuery",
          "description": "The SELECT query to use to get the count of records to import 
          from the specified table. Examples: SELECT COUNT(*) from <my_table> where 
          <my_column> 1, SELECT COUNT(my_column) from my_table). NOTE: Please include the 
          same WHERE clauses in this query as the ones used in the import query to reflect 
          an accurate number of records to import.",
          "type": "string",
          "required": true
        }
      },
      "template": {
        "name": "etlBatch",
        "description": "Batch Extract-Transform-Load (ETL) Adapter",
        "programType": "Workflow"
      },
      "source": {
        "fileName": "cdap-etl-lib-3.0.0-SNAPSHOT-batch.jar",
        "name": "cdap-etl-lib",
        "version": {
          "version": "3.0.0-SNAPSHOT-batch",
          "major": 3,
          "minor": 0,
          "fix": 0,
          "suffix": "SNAPSHOT-batch"
        }
      },
      "type": "source",
      "name": "Database",
      "description": "Batch source for a database."
    }
  ]

Adapters
=====================

Creating an Adapter 
-------------------
To create an Adapter, submit an HTTP PUT request::

  PUT <base-url>/namespaces/<namespace-id>/adapters/<adapter-id>

with the path to the Adaptor configuration file [link] as the body of the request::

  <config-path>

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<adapter-id>``
     - Name of the Adapter
   * - ``<config-path>``
     - Path to the configuration file

The format of the configuration file is described in the Developers’ Manual section
on Adapters. [link]

.. rubric::  Example

.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``PUT <base-url>/namespaces/default/adapters/streamAdapter -d @config.json``
   * - Description
     - Creates an Adaptor *streamAdapter* in the namespace *default* using the configuration
       file ``config.json``

Listing Existing Adapters
-------------------------
To retrieve a list of the existing Adapters, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace-id>/adapters

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID

List Details of an Adapter
--------------------------
To retrieve the details of a particular Adapter, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace-id>/adapters/<adapter-id>

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<adapter-id>``
     - Name of the Adapter


Status of an Adapter
--------------------
To retrieve the status of an Adapter, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace-id>/adapters/<adapter-id>/status

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<adapter-id>``
     - Name of the Adapter


Starting an Adapter
-------------------
Starting a Batch Adapter schedules a Workflow to be run periodically based on the cron
schedule that is configured in the Adapter. Starting a Realtime Adapter starts a CDAP
Worker.

To start an Adapter, submit an HTTP POST request::

  POST <base-url>/namespaces/<namespace-id>/adapters/<adapter-id>/start

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<adapter-id>``
     - Name of the Adapter

Stopping an Adapter
-------------------
To stop an Adapter, submit an HTTP POST request::

  POST <base-url>/namespaces/<namespace-id>/adapters/<adapter-id>/stop

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<adapter-id>``
     - Name of the Adapter

Deleting an Adapter
-------------------
To delete an Adapter, submit an HTTP DELETE request::

  DELETE <base-url>/namespaces/<namespace-id>/adapters/<adapter-id>

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<adapter-id>``
     - Name of the Adapter

Getting Adapter runs
--------------------
To retrieve a list of runs of an Adapter, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace-id>/adapters/<adapter-id>/runs

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<adapter-id>``
     - Name of the Adapter

Getting Adapter logs
--------------------
To retrieve the logs of an Adapter, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace-id>/adapters/<adapter-id>/logs

where

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<adapter-id>``
     - Name of the Adapter
