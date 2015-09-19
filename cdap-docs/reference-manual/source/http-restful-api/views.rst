.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _http-restful-api-views:

======================
Views HTTP RESTful API 
======================

.. highlight:: console

Views
=====

Views are a source where data can be read from, similar to a :ref:`stream <streams>` or
:ref:`dataset <datasets-index>`. They are readable and usable anywhere that a stream or
dataset are used, such as a flow, MapReduce and Spark programs, or :ref:`ETL
<included-apps-etl-index>`.

A view is a read-only view of a stream or dataset, with a specific read format. Read
formats consist of a :ref:`schema <stream-exploration-stream-schema>` and a :ref:`format
<stream-exploration-stream-format>` (such as CSV, TSV, or Avro).

Currently, views are only supported for streams. Support for datasets will be added in a
later version of CDAP.





If :ref:`CDAP Explore <data-exploration>` is :ref:`enabled
<install-configuring-explore-service>`, then a Hive table will be created for each view
that is created.



PUT /v3/namespaces/<namespace>/streams/<stream>/views/<view>	
ViewSpecification
{
  "format": <same as before>
}
created new stream view -> 201 Created
modified existing stream view -> 200 OK 	Creates or modifies a view.

GET /v3/namespaces/<namespace>/streams/<stream>/views/<view>	 	
ViewDetail (ViewSpecification with an "id" field)
{"id":"view1", "format": ..}
Get details of an individual view.

DELETE /v3/namespace/<namespace>/streams/<stream>/view/<view>	 	 	Deletes a view.

GET /v3/namespaces/<namespace>/stream/<stream>/views	 	
[
  {"id":"someview", "stream": "stream1", "format": ..},
  {"id":"otherview", "stream": "stream2", "format": ..}
]
Lists all views associated with a stream.




.. _http-restful-api-view-add-view-stream:

Adding a View to a Stream
-------------------------
A view can be added to an existing stream with an HTTP POST method to the URL::

  PUT <base-url>/namespaces/<namespace>/streams/<stream-id>/views/<view-id>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<stream-id>``
     - Name of the stream (must be already existing)
   * - ``<view-id>``
     - Name of the view to be created, or, if already existing, updated

The request body is a JSON object specifying the read format to be used. For example:
 
.. container:: highlight

  .. parsed-literal::
    |$| PUT <base-url>/namespaces/default/streams/purchaseStream/views/purchaseStreamView -H "Content-Type: application/json" -d
    {
      "artifact": {
        "name": "WordCount",
        "version": "|release|",
        "scope": "user"
      },
      "config": {
        "stream": "purchaseStream"
      }
    } 







.. _http-restful-api-artifact-available:

List Available Artifacts 
------------------------
To retrieve a list of available artifacts, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/artifacts[?scope=<scope>]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<scope>``
     - Optional scope filter. If not specified, artifacts in the ``user`` and
       ``system`` scopes are returned. Otherwise, only artifacts in the specified scope are returned.

This will return a JSON array that lists each artifact with its name, version, and scope.
Example output (pretty-printed):

.. container:: highlight

  .. parsed-literal::
    |$| GET <base-url>/namespaces/default/artifacts
    [
      {
        "name": "cdap-etl-batch",
        "scope": "SYSTEM",
        "version": "|release|"
      },
      {
        "name": "cdap-etl-realtime",
        "scope": "SYSTEM",
        "version": "|release|"
      },
      {
        "name": "Purchase",
        "scope": "USER",
        "version": "|release|"
      }
    ]

.. _http-restful-api-artifact-versions:

List Artifact Versions
----------------------
To list all versions of a specific artifact, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/artifact/<artifact-name>[?scope=<scope>]
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<artifact-name>``
     - Name of the artifact
   * - ``<scope>``
     - Optional scope filter. If not specified, defaults to ``user``.

This will return a JSON array that lists each version of the specified artifact with
its name, version, and scope. Example output for the ``cdap-etl-batch`` artifact (pretty-printed):

.. container:: highlight

  .. parsed-literal::
    |$| GET <base-url>/namespaces/default/artifacts/cdap-etl-batch?scope=system
    [
      {
        "name": "cdap-etl-batch",
        "scope": "SYSTEM",
        "version": "|release|"
      }
    ]

.. _http-restful-api-artifact-detail:

Retrieve Artifact Detail
------------------------
To retrieve detail about a specific version of an artifact, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/artifacts/<artifact-name>/versions/<artifact-version>[?scope=<scope>]
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<artifact-name>``
     - Name of the artifact.
   * - ``<artifact-version>``
     - Version of the artifact.
   * - ``<scope>``
     - Optional scope filter. If not specified, defaults to 'user'.

This will return a JSON object that contains information about: classes in the artifact;
the schema of the config object supported by the ``Application`` class; and the artifact name,
version, and scope. Example output for version |release| of the ``WordCount``
artifact (pretty-printed and reformatted to fit):

.. container:: highlight

  .. parsed-literal::
    |$| GET <base-url>/namespaces/default/artifact/WordCount/versions/|release|?scope=system
    {
      "classes": {
        "apps": [
          {
            "className": "co.cask.cdap.examples.wordcount.WordCount",
            "configSchema": {
              "fields": [
                { "name": "stream", "type": [ "string, "null" ] },
                { "name": "uniqueCountTable", "type": [ "string, "null" ] },
                { "name": "wordAssocTable", "type": [ "string, "null" ] },
                { "name": "wordCountTable", "type": [ "string, "null" ] },
                { "name": "wordStatsTable", "type": [ "string, "null" ] }
              ],
              "name": "co.cask.cdap.examples.wordcount.WordCount$WordCountConfig",
              "type": "record"
            },
            "description": ""
          }
        ],
        "plugins": []
      },
      "name": "WordCount",
      "scope": "USER",
      "version": "|release|"
    }

.. _http-restful-api-artifact-extensions:

List Extensions (Plugin Types) Available to an Artifact
-------------------------------------------------------
To list the extensions (plugin types) available to an artifact, submit
an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/artifacts/<artifact-name>/versions/<artifact-version>/extensions[?scope=<scope>]
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<artifact-name>``
     - Name of the artifact
   * - ``<artifact-version>``
     - Version of the artifact
   * - ``<scope>``
     - Optional scope filter. If not specified, defaults to 'user'.
  
This will return a JSON array that lists the extensions (plugin types) available to the artifact.
Example output for version |release| of the ``cdap-etl-batch``
artifact (pretty-printed and reformatted to fit):

.. container:: highlight

  .. parsed-literal::
    |$| GET <base-url>/namespaces/default/artifact/WordCount/versions/|release|/extensions?scope=system
    [ "transform", "validator", "batchsource", "batchsink" ]{

.. _http-restful-api-artifact-available-plugins:

Listing Plugins Available to an Artifact
----------------------------------------
To list plugins of a specific type available to an artifact, submit
an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/artifacts/<artifact-name>/versions/<artifact-version>/extensions/<plugin-type>[?scope=<scope>]
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<artifact-name>``
     - Name of the artifact
   * - ``<artifact-version>``
     - Version of the artifact
   * - ``<plugin-type>``
     - Type of plugins to list
   * - ``<scope>``
     - Optional scope filter. If not specified, defaults to 'user'.

This will return a JSON array that lists the plugins of the specified type
available to the artifact. Each element in the array is a JSON object containing
the artifact that the plugin originated from, and the plugin's class name, description, 
name, and type. Example output for plugins of type ``transform`` available to version |release|
of the ``cdap-etl-batch`` artifact (pretty-printed and reformatted to fit):

.. container:: highlight

  .. parsed-literal::
    |$| GET <base-url>/namespaces/default/artifacts/cdap-etl-batch/versions/|release|/extensions/transform?scope=system

    [
      {
        "artifact": {
          "name": "cdap-etl-lib",
          "scope": "SYSTEM",
          "version": "|release|-batch"
        },
        "className": "co.cask.cdap.etl.transform.LogParserTransform",
        "description": "Parses logs from any input source for relevant information such as URI, IP, Browser, Device, HTTP status code, and timestamp.",
        "name": "LogParser",
        "type": "transform"
      },
      {
        "artifact": {
            "name": "cdap-etl-lib",
            "scope": "SYSTEM",
            "version": "|release|-batch"
        },
        "className": "co.cask.cdap.etl.transform.ProjectionTransform",
        "description": "Projection transform that lets you drop, rename, and cast fields to a different type.",
        "name": "Projection",
        "type": "transform"
      },
      ...
    ]

.. _http-restful-api-artifact-plugin-detail:

Retrieving Plugin Details
-------------------------
To retrieve details about a specific plugin available to an artifact, submit
an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/artifacts/<artifact-name>/versions/<artifact-version>/extensions/<plugin-type>/plugins/<plugin-name>[?scope=<scope>]
  
.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<artifact-name>``
     - Name of the artifact.
   * - ``<artifact-version>``
     - Version of the artifact
   * - ``<plugin-type>``
     - Type of the plugin
   * - ``<plugin-name>``
     - Name of the plugin
   * - ``<scope>``
     - Optional scope filter. If not specified, defaults to 'user'.

This will return a JSON array that lists the plugins of the specified type and name
available to the artifact. Each element in the array is a JSON object containing
the artifact that the plugin originated from, and the plugin's class name, description, name, type, and properties.
Example output for the ``ScriptFilter`` plugin available to version |release|
of the ``cdap-etl-batch`` artifact (pretty-printed and reformatted to fit):

.. container:: highlight

  .. parsed-literal::
    |$| GET <base-url>/namespaces/default/artifacts/cdap-etl-batch/versions/|release|/extensions/transform/plugins/ScriptFilter?scope=system

    [
      {
        "artifact": {
            "name": "cdap-etl-lib",
            "scope": "SYSTEM",
            "version": "|release|-batch"
        },
        "className": "co.cask.cdap.etl.transform.ScriptFilterTransform",
        "description": "A transform plugin that filters records using a custom Javascript provided in the plugin's config.",
        "name": "ScriptFilter",
        "properties": {
            "script": {
                "description": "Javascript that must implement a function 'shouldFilter' that takes a JSON object representation of the input record, and returns true if the input record should be filtered and false if not. For example: 'function shouldFilter(input) { return input.count > 100; }' will filter out any records whose 'count' field is greater than 100.",
                "name": "script",
                "required": true,
                "type": "string"
            }
        },
        "type": "transform"
      }
    ]

.. _http-restful-api-artifact-delete:

Deleting an Artifact
--------------------
To delete an artifact, submit an HTTP DELETE request::

  DELETE <base-url>/namespaces/<namespace>/artifacts/<artifact-name>/versions/<artifact-version>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<artifact-name>``
     - Name of the artifact.
   * - ``<artifact-version>``
     - Version of the artifact.

Deleting an artifact is an advanced feature. If there are programs that use the artifact, those
programs will not be able to start unless the artifact is added again, or the program application
is updated to use a different artifact. 

.. _http-restful-api-artifact-system-load:

Load System Artifacts
---------------------
To load all system artifacts on the CDAP Master node(s), submit an HTTP POST request::

  POST <base-url>/namespaces/system/artifacts

This call will make the CDAP master scan the artifacts directly and add any new artifacts
that it finds. Any snapshot artifacts will be re-loaded.

.. _http-restful-api-artifact-system-delete:

Delete System Artifact
----------------------
To delete a system artifact, submit an HTTP DELETE request::

  DELETE <base-url>/namespaces/system/artifacts/<artifact-name>/versions/<artifact-version>

Deleting an artifact is an advanced feature. If there are programs that use the artifact, those
programs will not be able to start unless the artifact is added again, or the program application
is updated to use a different artifact. 

.. _http-restful-api-artifact-app-classes:

Listing Application Classes
---------------------------
To list application classes, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/classes/apps[scope=<scope>]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<adapter-id>``
     - Name of the adapter
   * - ``<config-path>``
     - Path to the configuration file
   * - ``<scope>``
     - Optional scope filter. If not specified, classes from artifacts in the ``user`` and
       ``system`` scopes are returned. Otherwise, only classes from artifacts in the specified scope are returned.

This will return a JSON array that lists all application classes contained in artifacts.
Each element in the array is a JSON object that describes the artifact the class originates in
as well as the class name. Example output for the ``ScriptFilter`` (pretty-printed and reformatted to fit):

.. container:: highlight

  .. parsed-literal::
    |$| GET <base-url>/namespaces/default/classes/apps

    [
      {
        "artifact": {
          "name": "cdap-etl-batch",
          "scope": "SYSTEM",
          "version": "|release|"
        },
        "className": "co.cask.cdap.etl.batch.ETLBatchApplication"
      },
      {
        "artifact": {
          "name": "cdap-etl-realtime",
          "scope": "SYSTEM",
          "version": "|release|"
        },
        "className": "co.cask.cdap.etl.realtime.ETLRealtimeApplication"
      },
      {
        "artifact": {
          "name": "Purchase",
          "scope": "USER",
          "version": "|release|"
        },
        "className": "co.cask.cdap.examples.purchase.PurchaseApp"
      },
    ]

.. _http-restful-api-artifact-appclass-detail:

Retrivies Application Class Detail
----------------------------------
To retrieve detail about a specific application class, submit an HTTP GET request::

  GET <base-url>/namespaces/<namespace>/classes/apps/<class-name>[?scope=<scope>]

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace>``
     - Namespace ID
   * - ``<class-name>``
     - Application class name
   * - ``<scope>``
     - Optional scope filter. If not specified, defaults to ``user``.

This will return a JSON array that lists each application class with that class name.
Each element in the array is a JSON object that contains details about the application
class, including the artifact the class is from, the class name, and the schema of
the config supported by the application class.
Example output for the ``WordCount`` application (pretty-printed and reformatted to fit):

.. container:: highlight

  .. parsed-literal::
    |$| GET <base-url>/namespaces/default/classes/apps/co.cask.cdap.examples.wordcount.WordCount
    [
      {
        "artifact": {
          "name": "WordCount",
          "scope": "USER",
          "version": "3.2.0-SNAPSHOT"
        },
        "className": "co.cask.cdap.examples.wordcount.WordCount",
        "configSchema": {
          "fields": [
            { "name": "stream", "type": [ "string", "null" ] },
            { "name": "uniqueCountTable", "type": [ "string", "null" ] },
            { "name": "wordAssocTable", "type": [ "string", "null" ] },
            { "name": "wordCountTable", "type": [ "string", "null" ] },
            { "name": "wordStatsTable", "type": [ "string", "null" ] },
          ],
          "name": "co.cask.cdap.examples.wordcount.WordCount$WordCountConfig",
          "type": "record"
        }
      }
    ]
