.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _cli:

============================================
Command Line Interface API
============================================

Introduction
============

The Command Line Interface (CLI) provides methods to interact with the CDAP server from within a shell,
similar to the HBase or ``bash`` shells. It is located within the SDK, at ``bin/cdap-cli`` as either a bash
script or a Windows ``.bat`` file.

The CLI may be used in two ways: interactive mode and non-interactive mode.

Interactive Mode
----------------

.. highlight:: console

To run the CLI in interactive mode, run the ``cdap-cli.sh`` executable with no arguments from the terminal::

  $ ./bin/cdap-cli.sh

or, on Windows::

  > bin\cdap-cli.bat

The executable should bring you into a shell, with this prompt::

  cdap (http://localhost:10000)>

This indicates that the CLI is currently set to interact with the CDAP server at ``localhost``.

- To interact with a different CDAP server by default, set the environment variable ``CDAP_HOST`` to a hostname.
- To change the current CDAP server, run the CLI command ``connect example.com``.
- To connect to an SSL-enabled CDAP server, run the CLI command ``connect https://example.com``.

For example, with ``CDAP_HOST`` set to ``example.com``, the CLI would be interacting with
a CDAP instance at ``example.com``, port ``10000``::

  cdap (http://example.com:10000)>

To list all of the available commands, enter the CLI command ``help``::

  cdap (http://localhost:10000)> help

Non-Interactive Mode
--------------------

To run the CLI in non-interactive mode, run the ``cdap-cli.sh`` executable, passing the command you want executed
as the argument. For example, to list all applications currently deployed to CDAP, execute::

  $ cdap-cli.sh list apps

Connecting to Secure CDAP Instances
-----------------------------------

When connecting to secure CDAP instances, the CLI will look for an access token located at
``~/.cdap.accesstoken.<hostname>`` and use it if it exists and is valid. If not, the CLI will prompt
you for the required credentials to acquire an access token from the CDAP instance. Once acquired,
the CLI will save it to ``~/.cdap.accesstoken.<hostname>"`` for later use and use it for the rest of
the current CLI session.

Options
-------

The CLI can be started with command-line options, as detailed below::

  usage: cdap-cli.sh [--autoconnect <true|false>] [--debug] [--help]
                     [--verify-ssl <true|false>] [--uri <uri>][--script
                     <script-file>]
   -a,--autoconnect <arg>   If "true", try provided connection (from uri)
                            upon launch or try default connection if none
                            provided. Defaults to "true".
   -d,--debug               Print exception stack traces.
   -h,--help                Print the usage message.
   -s,--script <arg>        Execute a file containing a series of CLI
                            commands, line-by-line.
   -u,--uri <arg>           CDAP instance URI to interact with in the format
                            "[http[s]://]<hostname>[:<port>[/<namespace>]]".
                            Defaults to
                            "http://<hostname>.local:10000".
   -v,--verify-ssl <arg>    If "true", verify SSL certificate when making
                            requests. Defaults to "true".

Settings
--------

Certain commands (``connect`` and ``cli render as``) affect how the CLI works for the duration of a session.

The command ``"cli render as <table-renderer>"`` sets how table data is rendered. Valid options are
either ``"alt"`` (the default) and ``"csv"``. As the ``"alt"`` option may split a cell into multiple
lines, you may need to use ``"csv"`` if you want to copy and paste the results into another
application or include in a message.

- With ``"cli render as alt"`` (the default), a command such as ``"list apps"`` will be output as::

    +================================+
    | app id      | description      |
    +=============+==================+
    | PurchaseApp | Some description |
    +=============+==================+

- With ``"cli render as csv"``, the same ``"list apps"`` would be output as::

    app id,description
    PurchaseApp,Some description

.. _cli-available-commands:

Available Commands
==================

These are the available commands:

.. csv-table::
   :header: Command,Description
   :widths: 50, 50

   **General**
   ``cli render as <table-renderer>``,"Modifies how table data is rendered. Valid options are ""alt"" (default) and ""csv""."
   ``cli version``,"Prints the CLI version."
   ``connect <cdap-instance-uri> [<verify-ssl-cert>]``,"Connects to a CDAP instance."
   ``exit``,"Exits the CLI."
   ``quit``,"Exits the CLI."
   **Namespace**
   ``create namespace <namespace-name> [<namespace-description>]``,"Creates a namespace in CDAP."
   ``delete namespace <namespace-name>``,"Deletes a namespace."
   ``describe namespace <namespace-name>``,"Describes a namespace."
   ``list namespaces``,"Lists all namespaces."
   ``use namespace <namespace-name>``,"Changes the current namespace to <namespace-name>."
   **Artifact**
   ``delete artifact <artifact-name> <artifact-version>``,"Deletes an artifact"
   ``describe artifact <artifact-name> <artifact-version> [<scope>]``,"Shows information about an artifact. If no scope is given, the user scope will be used. Includes information about application and plugin classes contained in the artifact."
   ``describe artifact-plugin <artifact-name> <artifact-version> <plugin-type> <plugin-name> [<scope>]``,"Describes all plugins of a specific type and name available to a specific artifact. Can return multiple details if the plugin exists in multiple artifacts. If no scope is given, the user scope will be used."
   ``get artifact properties <artifact-name> <artifact-version> [<scope>]``,"Gets properties of an artifact. If no scope is given, the user scope will be used. "
   ``list artifact plugin-types <artifact-name> <artifact-version> [<scope>]``,"Lists all plugin types usable by the specified artifact. If no scope is given, the user scope will be used."
   ``list artifact plugins <artifact-name> <artifact-version> <plugin-type> [<scope>]``,"Lists all plugins of a specific type available to a specific artifact. Includes the type, name, classname, and description of the plugin, as well as the artifact the plugin came from. If no scope is given, the user scope will be used."
   ``list artifact versions <artifact-name> [<scope>]``,"Lists all versions of a specific artifact. If no scope is given, the user scope will be used."
   ``list artifacts [<scope>]``,"Lists all artifacts. If no scope is given, artifacts in all scopes are returned. Otherwise, only artifacts in the specified scope are returned."
   ``load artifact <local-file-path> [config-file <artifact-config>] [name <artifact-name>] [version <artifact-version>]``,"Loads an artifact into CDAP. If the artifact name and version are not both given, they will be derived from the filename of the artifact. File names are expected to be of the form <name>-<version>.jar. If the artifact contains plugins that extend another artifact, or if it contains third-party plugins, a config file must be given. The config file must contain a JSON object that specifies the parent artifacts and any third-party plugins in the jar. For example, if there is a config file with these contents:
    | ``{``
    |   ``""parents"":[ ""app1[1.0.0,2.0.0)"", ""app2[1.2.0,1.3.0] ],``
    |   ``""plugins"":[``
    |     ``{ ""type"": ""jdbc"",``
    |       ``""name"": ""mysql"",``
    |       ``""className"": ""com.mysql.jdbc.Driver""``
    |     ``}``
    |   ``],``
    |   ``""plugins"":{``
    |     ``""prop1"": ""val1"",``
    |   ``},``
    | ``}``

   This config specifies that the artifact contains one JDBC third-party plugin that should be available to the app1 artifact (versions 1.0.0 inclusive to 2.0.0 exclusive) and app2 artifact (versions 1.2.0 inclusive to 1.3.0 inclusive). The config may also include a 'properties' field specifying properties for the artifact."
   ``set artifact properties <artifact-name> <artifact-version> <scope> <local-file-path>``,"Sets properties of an artifact. The properties file must contain a JSON Object with a 'properties' key whose value is a JSON Object of the properties for the artifact."
   **Metadata And Lineage**
   ``add metadata-properties <entity-id> <properties>``,"Adds metadata properties for an entity"
   ``add metadata-tags <entity-id> <tags>``,"Adds metadata tags for an entity"
   ``get lineage dataset <dataset-name> [start <start>] [end <end>] [levels <levels>]``,"Gets the lineage of a dataset"
   ``get lineage stream <stream-id> [start <start>] [end <end>] [levels <levels>]``,"Gets the lineage of a stream"
   ``get metadata <entity-id> [scope <scope>]``,"Gets the metadata of an entity"
   ``get metadata-properties <entity-id> [scope <scope>]``,"Gets the metadata properties of an entity"
   ``get metadata-tags <entity-id> [scope <scope>]``,"Gets the metadata tags of an entity"
   ``remove metadata <entity-id>``,"Removes metadata for an entity"
   ``remove metadata-properties <entity-id>``,"Removes all metadata properties for an entity"
   ``remove metadata-property <entity-id> <property>``,"Removes a metadata property for an entity"
   ``remove metadata-tag <entity-id> <tag>``,"Removes a metadata tag for an entity"
   ``remove metadata-tags <entity-id>``,"Removes all metadata tags for an entity"
   ``search metadata <search-query> [filtered by target-type <target-type>]``,"Allows users to search CDAP entities based on the metadata annotated on them."
   **Application Lifecycle**
   ``create app <app-id> <artifact-name> <artifact-version> <scope> [<app-config-file>]``,"Creates an application from an artifact with optional configuration. If configuration is needed, it must be given as a file whose contents are a JSON object containing the application config. For example, the file contents could contain: '{ ""config"": { ""stream"": ""purchases"" } }'. In this case, the application would receive '{ ""stream"": ""purchases"" }' as its config object."
   ``delete app <app-id>``,"Deletes an application."
   ``delete preferences app [<app-id>]``,"Deletes the preferences of an application."
   ``delete preferences flow [<app-id.flow-id>]``,"Deletes the preferences of a flow."
   ``delete preferences instance [<instance-id>]``,"Deletes the preferences of an instance."
   ``delete preferences mapreduce [<app-id.mapreduce-id>]``,"Deletes the preferences of a MapReduce program."
   ``delete preferences namespace [<namespace-name>]``,"Deletes the preferences of a namespace."
   ``delete preferences service [<app-id.service-id>]``,"Deletes the preferences of a service."
   ``delete preferences spark [<app-id.spark-id>]``,"Deletes the preferences of a Spark program."
   ``delete preferences worker [<app-id.worker-id>]``,"Deletes the preferences of a worker."
   ``delete preferences workflow [<app-id.workflow-id>]``,"Deletes the preferences of a workflow."
   ``deploy app <app-jar-file> [<app-config>]``,"Deploys an application optionally with a serialized configuration string."
   ``describe app <app-id>``,"Shows information about an application."
   ``get app <app-id> programs status [of type <program-types>]``,"Command to get status of one or more programs of an application. By default, get status of all flows, services, and workers. A comma separated list of program types can be specified, which will start all programs of those types. For example, specifying ""flow,workflow"" will get status of all flows and workflows in the application."
   ``get endpoints service <app-id.service-id>``,"Lists the endpoints that a service exposes."
   ``get flow live <app-id.flow-id>``,"Gets the live info of a flow."
   ``get flow logs <app-id.flow-id> [<start-time>] [<end-time>]``,"Gets the logs of a flow."
   ``get flow runs <app-id.flow-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a flow."
   ``get flow runtimeargs <app-id.flow-id>``,"Gets the runtime arguments of a flow."
   ``get flow status <app-id.flow-id>``,"Gets the status of a flow."
   ``get flowlet instances <app-id.flow-id.flowlet-id>``,"Gets the instances of a flowlet."
   ``get mapreduce logs <app-id.mapreduce-id> [<start-time>] [<end-time>]``,"Gets the logs of a MapReduce program."
   ``get mapreduce runs <app-id.mapreduce-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a MapReduce program."
   ``get mapreduce runtimeargs <app-id.mapreduce-id>``,"Gets the runtime arguments of a MapReduce program."
   ``get mapreduce status <app-id.mapreduce-id>``,"Gets the status of a MapReduce program."
   ``get preferences app [<app-id>]``,"Gets the preferences of an application."
   ``get preferences flow [<app-id.flow-id>]``,"Gets the preferences of a flow."
   ``get preferences instance [<instance-id>]``,"Gets the preferences of an instance."
   ``get preferences mapreduce [<app-id.mapreduce-id>]``,"Gets the preferences of a MapReduce program."
   ``get preferences namespace [<namespace-name>]``,"Gets the preferences of a namespace."
   ``get preferences service [<app-id.service-id>]``,"Gets the preferences of a service."
   ``get preferences spark [<app-id.spark-id>]``,"Gets the preferences of a Spark program."
   ``get preferences worker [<app-id.worker-id>]``,"Gets the preferences of a worker."
   ``get preferences workflow [<app-id.workflow-id>]``,"Gets the preferences of a workflow."
   ``get resolved preferences app [<app-id>]``,"Gets the resolved preferences of an application."
   ``get resolved preferences flow [<app-id.flow-id>]``,"Gets the resolved preferences of a flow."
   ``get resolved preferences instance [<instance-id>]``,"Gets the resolved preferences of an instance."
   ``get resolved preferences mapreduce [<app-id.mapreduce-id>]``,"Gets the resolved preferences of a MapReduce program."
   ``get resolved preferences namespace [<namespace-name>]``,"Gets the resolved preferences of a namespace."
   ``get resolved preferences service [<app-id.service-id>]``,"Gets the resolved preferences of a service."
   ``get resolved preferences spark [<app-id.spark-id>]``,"Gets the resolved preferences of a Spark program."
   ``get resolved preferences worker [<app-id.worker-id>]``,"Gets the resolved preferences of a worker."
   ``get resolved preferences workflow [<app-id.workflow-id>]``,"Gets the resolved preferences of a workflow."
   ``get schedule status <app-id.schedule-id>``,"Gets the status of a schedule"
   ``get service instances <app-id.service-id>``,"Gets the instances of a service."
   ``get service runs <app-id.service-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a service."
   ``get service runtimeargs <app-id.service-id>``,"Gets the runtime arguments of a service."
   ``get service status <app-id.service-id>``,"Gets the status of a service."
   ``get spark logs <app-id.spark-id> [<start-time>] [<end-time>]``,"Gets the logs of a Spark program."
   ``get spark runs <app-id.spark-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Spark program."
   ``get spark runtimeargs <app-id.spark-id>``,"Gets the runtime arguments of a Spark program."
   ``get spark status <app-id.spark-id>``,"Gets the status of a Spark program."
   ``get worker instances <app-id.worker-id>``,"Gets the instances of a worker."
   ``get worker live <app-id.worker-id>``,"Gets the live info of a worker."
   ``get worker logs <app-id.worker-id> [<start-time>] [<end-time>]``,"Gets the logs of a worker."
   ``get worker runs <app-id.worker-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a worker."
   ``get worker runtimeargs <app-id.worker-id>``,"Gets the runtime arguments of a worker."
   ``get worker status <app-id.worker-id>``,"Gets the status of a worker."
   ``get workflow current <app-id.workflow-id> <runid>``,"Gets the currently running nodes of a workflow for a given run id."
   ``get workflow logs <app-id.workflow-id> [<start-time>] [<end-time>]``,"Gets the logs of a workflow."
   ``get workflow runs <app-id.workflow-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a workflow."
   ``get workflow runtimeargs <app-id.workflow-id>``,"Gets the runtime arguments of a workflow."
   ``get workflow schedules <app-id.workflow-id>``,"Resumes a schedule"
   ``get workflow status <app-id.workflow-id>``,"Gets the status of a workflow."
   ``get workflow token <app-id.workflow-id> <runid> [at node <workflow-node>] [scope <workflow-token-scope>] [key <workflow-token-key>]``,"Gets the workflow token of a workflow for a given run id."
   ``list apps [<artifact-name>] [<artifact-version>]``,"Lists all applications, optionally filtered by artifact name and version."
   ``list flows``,"Lists all flows."
   ``list mapreduce``,"Lists all MapReduce programs."
   ``list programs``,"Lists all programs."
   ``list services``,"Lists all services."
   ``list spark``,"Lists all Spark programs."
   ``list workers``,"Lists all workers."
   ``list workflows``,"Lists all workflows."
   ``load preferences app <local-file-path> <content-type> <app-id>``,"Sets preferences of an application from a local config file (supported formats = JSON)."
   ``load preferences flow <local-file-path> <content-type> <app-id.flow-id>``,"Sets preferences of a flow from a local config file (supported formats = JSON)."
   ``load preferences instance <local-file-path> <content-type>``,"Sets preferences of an instance from a local config file (supported formats = JSON)."
   ``load preferences mapreduce <local-file-path> <content-type> <app-id.mapreduce-id>``,"Sets preferences of a MapReduce program from a local config file (supported formats = JSON)."
   ``load preferences namespace <local-file-path> <content-type>``,"Sets preferences of a namespace from a local config file (supported formats = JSON)."
   ``load preferences service <local-file-path> <content-type> <app-id.service-id>``,"Sets preferences of a service from a local config file (supported formats = JSON)."
   ``load preferences spark <local-file-path> <content-type> <app-id.spark-id>``,"Sets preferences of a Spark program from a local config file (supported formats = JSON)."
   ``load preferences worker <local-file-path> <content-type> <app-id.worker-id>``,"Sets preferences of a worker from a local config file (supported formats = JSON)."
   ``load preferences workflow <local-file-path> <content-type> <app-id.workflow-id>``,"Sets preferences of a workflow from a local config file (supported formats = JSON)."
   ``restart app <app-id> programs [of type <program-types>]``,"Command to restart one or more programs of an application. By default, restart all flows, services, and workers. A comma separated list of program types can be specified, which will start all programs of those types. For example, specifying ""flow,workflow"" will restart all flows and workflows in the application."
   ``resume schedule <app-id.schedule-id>``,"Resumes a schedule"
   ``set flow runtimeargs <app-id.flow-id> <runtime-args>``,"Sets the runtime arguments of a flow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set flowlet instances <app-id.flow-id.flowlet-id> <num-instances>``,"Sets the instances of a flowlet."
   ``set mapreduce runtimeargs <app-id.mapreduce-id> <runtime-args>``,"Sets the runtime arguments of a MapReduce program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set preferences app <runtime-args> <app-id>``,"Sets the preferences of an application. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences flow <runtime-args> <app-id.flow-id>``,"Sets the preferences of a flow. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences instance <runtime-args>``,"Sets the preferences of an instance. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences mapreduce <runtime-args> <app-id.mapreduce-id>``,"Sets the preferences of a MapReduce program. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences namespace <runtime-args>``,"Sets the preferences of a namespace. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences service <runtime-args> <app-id.service-id>``,"Sets the preferences of a service. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences spark <runtime-args> <app-id.spark-id>``,"Sets the preferences of a Spark program. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences worker <runtime-args> <app-id.worker-id>``,"Sets the preferences of a worker. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences workflow <runtime-args> <app-id.workflow-id>``,"Sets the preferences of a workflow. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set service instances <app-id.service-id> <num-instances>``,"Sets the instances of a service."
   ``set service runtimeargs <app-id.service-id> <runtime-args>``,"Sets the runtime arguments of a service. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set spark runtimeargs <app-id.spark-id> <runtime-args>``,"Sets the runtime arguments of a Spark program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set worker instances <app-id.worker-id> <num-instances>``,"Sets the instances of a worker."
   ``set worker runtimeargs <app-id.worker-id> <runtime-args>``,"Sets the runtime arguments of a worker. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set workflow runtimeargs <app-id.workflow-id> <runtime-args>``,"Sets the runtime arguments of a workflow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start app <app-id> programs [of type <program-types>]``,"Command to start one or more programs of an application. By default, start all flows, services, and workers. A comma separated list of program types can be specified, which will start all programs of those types. For example, specifying ""flow,workflow"" will start all flows and workflows in the application."
   ``start flow <app-id.flow-id> [<runtime-args>]``,"Starts a flow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start mapreduce <app-id.mapreduce-id> [<runtime-args>]``,"Starts a MapReduce program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start service <app-id.service-id> [<runtime-args>]``,"Starts a service. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start spark <app-id.spark-id> [<runtime-args>]``,"Starts a Spark program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start worker <app-id.worker-id> [<runtime-args>]``,"Starts a worker. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start workflow <app-id.workflow-id> [<runtime-args>]``,"Starts a workflow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start-debug flow <app-id.flow-id> [<runtime-args>]``,"Starts a flow in debug mode. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start-debug mapreduce <app-id.mapreduce-id> [<runtime-args>]``,"Starts a MapReduce program in debug mode. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start-debug service <app-id.service-id> [<runtime-args>]``,"Starts a service in debug mode. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start-debug spark <app-id.spark-id> [<runtime-args>]``,"Starts a Spark program in debug mode. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start-debug worker <app-id.worker-id> [<runtime-args>]``,"Starts a worker in debug mode. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start-debug workflow <app-id.workflow-id> [<runtime-args>]``,"Starts a workflow in debug mode. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``stop app <app-id> programs [of type <program-types>]``,"Command to stop one or more programs of an application. By default, stop all flows, services, and workers. A comma separated list of program types can be specified, which will start all programs of those types. For example, specifying ""flow,workflow"" will stop all flows and workflows in the application."
   ``stop flow <app-id.flow-id>``,"Stops a flow."
   ``stop mapreduce <app-id.mapreduce-id>``,"Stops a MapReduce program."
   ``stop service <app-id.service-id>``,"Stops a service."
   ``stop spark <app-id.spark-id>``,"Stops a Spark program."
   ``stop worker <app-id.worker-id>``,"Stops a worker."
   ``stop workflow <app-id.workflow-id>``,"Stops a workflow."
   ``suspend schedule <app-id.schedule-id>``,"Suspends a schedule"
   ``update app <app-id> <artifact-name> <artifact-version> <scope> [<app-config-file>]``,"Updates an application to use another artifact version and/or configuration."
   **Dataset**
   ``create dataset instance <dataset-type> <new-dataset-name> [<dataset-properties>]``,"Creates a dataset. <dataset-properties> is in the format ""key1=val1 key2=val2"""
   ``delete dataset instance <dataset-name>``,"Deletes a dataset."
   ``delete dataset module <dataset-module>``,"Deletes a dataset module."
   ``deploy dataset module <new-dataset-module> <module-jar-file> <module-jar-classname>``,"Deploys a dataset module."
   ``describe dataset instance <dataset-name>``,"Shows information about a dataset."
   ``describe dataset module <dataset-module>``,"Shows information about a dataset module."
   ``describe dataset type <dataset-type>``,"Shows information about a dataset type."
   ``list dataset instances``,"Lists all datasets."
   ``list dataset modules``,"Lists all dataset modules."
   ``list dataset types``,"Lists all dataset types."
   ``set dataset instance properties <dataset-name> <dataset-properties>``,"Sets properties for a dataset."
   ``truncate dataset instance <dataset-name>``,"Truncates a dataset."
   **Explore**
   ``execute <query> [<timeout>]``,"Executes a query with optional <timeout> in minutes (default is no timeout)."
   **Metrics**
   ``get metric value <metric-name> [<tags>] [start <start>] [end <end>]``,"Gets the value of a metric. Provide <tags> as a map in the format 'tag1=value1 tag2=value2'."
   ``search metric names [<tags>]``,"Searches metric names. Provide <tags> as a map in the format 'tag1=value1 tag2=value2'."
   ``search metric tags [<tags>]``,"Searches metric tags. Provide <tags> as a map in the format 'tag1=value1 tag2=value2'."
   **Ingest**
   ``create stream <new-stream-id>``,"Creates a stream."
   ``create stream-view <stream-id> <view-id> format <format> [schema <schema>] [settings <settings>]``,"Creates or updates a stream-view. Valid <format>s are avro, csv, tsv, text, clf, grok, syslog. <schema> is a sql-like schema ""column_name data_type, ..."" or Avro-like JSON schema and <settings> is specified in the format ""key1=v1 key2=v2""."
   ``delete stream <stream-id>``,"Deletes a stream."
   ``delete stream-view <stream-id> <view-id>``,"Deletes a stream-view."
   ``describe stream <stream-id>``,"Shows information about a stream."
   ``describe stream-view <stream-id> <view-id>``,"Describes a stream-view."
   ``get stream <stream-id> [<start-time>] [<end-time>] [<limit>]``,"Gets events from a stream. The time format for <start-time> and <end-time> can be a timestamp in milliseconds or a relative time in the form of [+|-][0-9][d|h|m|s]. <start-time> is relative to current time; <end-time> is relative to <start-time>. Special constants ""min"" and ""max"" can be used to represent ""0"" and ""max timestamp"" respectively."
   ``get stream-stats <stream-id> [limit <limit>] [start <start-time>] [end <end-time>]``,"Gets statistics for a stream. The <limit> limits how many Stream events to analyze; default is 100. The time format for <start-time> and <end-time> can be a timestamp in milliseconds or a relative time in the form of [+|-][0-9][d|h|m|s]. <start-time> is relative to current time; <end-time> is relative to <start-time>. Special constants ""min"" and ""max"" can be used to represent ""0"" and ""max timestamp"" respectively."
   ``list stream-views <stream-id>``,"Lists all stream-views."
   ``list streams``,"Lists all streams."
   ``load stream <stream-id> <local-file-path> [<content-type>]``,"Loads a file to a stream. The contents of the file will become multiple events in the stream, based on the content type (avro/binary, text/csv, text/plain, text/tsv). If <content-type> is not provided, it will be detected by the file extension. Supported file extensions: avro, csv, log, tsv, txt."
   ``send stream <stream-id> <stream-event>``,"Sends an event to a stream."
   ``set stream format <stream-id> <format> [<schema>] [<settings>]``,"Sets the format of a stream. Valid <format>s are avro, csv, tsv, text, clf, grok, syslog. <schema> is a sql-like schema ""column_name data_type, ..."" or Avro-like JSON schema and <settings> is specified in the format ""key1=v1 key2=v2""."
   ``set stream notification-threshold <stream-id> <notification-threshold-mb>``,"Sets the notification threshold of a stream."
   ``set stream properties <stream-id> <local-file-path>``,"Sets the properties of a stream, such as TTL, format, and notification threshold."
   ``set stream ttl <stream-id> <ttl-in-seconds>``,"Sets the time-to-live (TTL) of a stream."
   ``truncate stream <stream-id>``,"Truncates a stream."
   **Egress**
   ``call service <app-id.service-id> <http-method> <endpoint> [headers <headers>] [body <body>] [body:file <local-file-path>]``,"Calls a service endpoint. The <headers> are formatted as ""{'key':'value', ...}"". The request body may be provided either as a string or a file. To provide the body as a string, use ""body <body>"". To provide the body as a file, use ""body:file <local-file-path>""."
   **Security**
   ``security access entity <entity-id> user <user> actions <actions>``,"Checks whether a user has permission to perform certain actions on an entity. <actions> is a comma-separated list."
   ``security grant entity <entity-id> user <user> actions <actions>``,"Grants a user permission to perform certain actions on an entity. <actions> is a comma-separated list."
   ``security revoke entity <entity-id> [user <user>] [actions <actions>]``,"Revokes a user's permission to perform certain actions on an entity. <actions> is a comma-separated list."
