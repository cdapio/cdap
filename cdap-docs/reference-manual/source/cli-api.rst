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
similar to HBase shell or ``bash``. It is located within the SDK, at ``bin/cdap-cli`` as either a bash
script or a Windows ``.bat`` file.

The CLI may be used in two ways: interactive mode and non-interactive mode.

Interactive Mode
----------------

.. highlight:: console

To run the CLI in interactive mode, run the ``cdap-cli.sh`` executable with no arguments from the terminal::

  $ /bin/cdap-cli.sh

or, on Windows::

  ~SDK> bin\cdap-cli.bat

The executable should bring you into a shell, with this prompt::

  cdap (http://localhost:10000)>

This indicates that the CLI is currently set to interact with the CDAP server at ``localhost``.
There are two ways to interact with a different CDAP server:

- To interact with a different CDAP server by default, set the environment variable ``CDAP_HOST`` to a hostname.
- To change the current CDAP server, run the command ``connect example.com``.
- To connect to an SSL-enabled CDAP server, run the command ``connect https://example.com``.

For example, with ``CDAP_HOST`` set to ``example.com``, the CLI would be interacting with
a CDAP instance at ``example.com``, port ``10000``::

  cdap (http://example.com:10000)>

To list all of the available commands, enter ``help``::

  cdap (http://localhost:10000)> help

Non-Interactive Mode
--------------------

To run the CLI in non-interactive mode, run the ``cdap-cli.sh`` executable, passing the command you want executed
as the argument. For example, to list all applications currently deployed to CDAP, execute::

  cdap-cli.sh list apps

Connecting to Secure CDAP Instances
-----------------------------------

When connecting to secure CDAP instances, the CLI will look for an access token located at
``~/.cdap.accesstoken.<hostname>`` and use it if it exists and is valid. If not, the CLI will prompt
you for the required credentials to acquire an access token from the CDAP instance. Once acquired,
the CLI will save it to ``~/.cdap.accesstoken.<hostname>"`` for later use and use it for the rest of
the current CLI session.

Options
-------

The CLI may be started with command-line options, as detailed below::

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

Certain commands (``connect`` and ``cli render as``) affect how CLI works for the duration of a session.

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
   ``connect <cdap-instance-uri>``,"Connects to a CDAP instance."
   ``exit``,"Exits the CLI."
   ``quit``,"Exits the CLI."
   **Namespace**
   ``create namespace <namespace-name> [<namespace-description>]``,"Creates a namespace in CDAP."
   ``delete namespace <namespace-name>``,"Deletes a Namespace."
   ``describe namespace <namespace-name>``,"Describes a Namespace."
   ``list namespaces``,"Lists all Namespaces."
   ``use namespace <namespace-name>``,"Changes the current Namespace to <namespace-name>."
   **Application Lifecycle**
   ``create stream <new-stream-id>``,"Creates a Stream."
   ``delete app <app-id>``,"Deletes an Application."
   ``delete preferences app [<app-id>]``,"Deletes the preferences of an Application."
   ``delete preferences flow [<app-id.flow-id>]``,"Deletes the preferences of a Flow."
   ``delete preferences instance [<instance-id>]``,"Deletes the preferences of an Instance."
   ``delete preferences mapreduce [<app-id.mapreduce-id>]``,"Deletes the preferences of a MapReduce Program."
   ``delete preferences namespace [<namespace-name>]``,"Deletes the preferences of a Namespace."
   ``delete preferences service [<app-id.service-id>]``,"Deletes the preferences of a Service."
   ``delete preferences spark [<app-id.spark-id>]``,"Deletes the preferences of a Spark Program."
   ``delete preferences worker [<app-id.worker-id>]``,"Deletes the preferences of a Worker."
   ``delete preferences workflow [<app-id.workflow-id>]``,"Deletes the preferences of a Workflow."
   ``delete stream <stream-id>``,"Deletes a Stream."
   ``deploy app <app-jar-file>``,"Deploys an Application."
   ``describe app <app-id>``,"Shows information about an Application."
   ``describe app-template <app-template-id>``,"Lists all Application Templates."
   ``describe stream <stream-id>``,"Shows detailed information about a Stream."
   ``get app-template plugins <app-template-id> <plugin-type>``,"Lists plugins for an Application Template."
   ``get endpoints service <app-id.service-id>``,"List the endpoints that a Service exposes."
   ``get flow live <app-id.flow-id>``,"Gets the live info of a Flow."
   ``get flow logs <app-id.flow-id> [<start-time>] [<end-time>]``,"Gets the logs of a Flow."
   ``get flow runs <app-id.flow-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Flow."
   ``get flow runtimeargs <app-id.flow-id>``,"Gets the runtime arguments of a Flow."
   ``get flow status <app-id.flow-id>``,"Gets the status of a Flow."
   ``get flowlet instances <app-id.flow-id.flowlet-id>``,"Gets the instances of a Flowlet."
   ``get mapreduce logs <app-id.mapreduce-id> [<start-time>] [<end-time>]``,"Gets the logs of a MapReduce Program."
   ``get mapreduce runs <app-id.mapreduce-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a MapReduce Program."
   ``get mapreduce runtimeargs <app-id.mapreduce-id>``,"Gets the runtime arguments of a MapReduce Program."
   ``get mapreduce status <app-id.mapreduce-id>``,"Gets the status of a MapReduce Program."
   ``get preferences app [<app-id>]``,"Gets the preferences of an Application."
   ``get preferences flow [<app-id.flow-id>]``,"Gets the preferences of a Flow."
   ``get preferences instance [<instance-id>]``,"Gets the preferences of an Instance."
   ``get preferences mapreduce [<app-id.mapreduce-id>]``,"Gets the preferences of a MapReduce Program."
   ``get preferences namespace [<namespace-name>]``,"Gets the preferences of a Namespace."
   ``get preferences service [<app-id.service-id>]``,"Gets the preferences of a Service."
   ``get preferences spark [<app-id.spark-id>]``,"Gets the preferences of a Spark Program."
   ``get preferences worker [<app-id.worker-id>]``,"Gets the preferences of a Worker."
   ``get preferences workflow [<app-id.workflow-id>]``,"Gets the preferences of a Workflow."
   ``get resolved preferences app [<app-id>]``,"Gets the resolved preferences of an Application."
   ``get resolved preferences flow [<app-id.flow-id>]``,"Gets the resolved preferences of a Flow."
   ``get resolved preferences instance [<instance-id>]``,"Gets the resolved preferences of an Instance."
   ``get resolved preferences mapreduce [<app-id.mapreduce-id>]``,"Gets the resolved preferences of a MapReduce Program."
   ``get resolved preferences namespace [<namespace-name>]``,"Gets the resolved preferences of a Namespace."
   ``get resolved preferences service [<app-id.service-id>]``,"Gets the resolved preferences of a Service."
   ``get resolved preferences spark [<app-id.spark-id>]``,"Gets the resolved preferences of a Spark Program."
   ``get resolved preferences worker [<app-id.worker-id>]``,"Gets the resolved preferences of a Worker."
   ``get resolved preferences workflow [<app-id.workflow-id>]``,"Gets the resolved preferences of a Workflow."
   ``get schedule status <app-id.schedule-id>``,"Gets the status of a schedule"
   ``get service instances <app-id.service-id>``,"Gets the instances of a Service."
   ``get service runs <app-id.service-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Service."
   ``get service runtimeargs <app-id.service-id>``,"Gets the runtime arguments of a Service."
   ``get service status <app-id.service-id>``,"Gets the status of a Service."
   ``get spark logs <app-id.spark-id> [<start-time>] [<end-time>]``,"Gets the logs of a Spark Program."
   ``get spark runs <app-id.spark-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Spark Program."
   ``get spark runtimeargs <app-id.spark-id>``,"Gets the runtime arguments of a Spark Program."
   ``get spark status <app-id.spark-id>``,"Gets the status of a Spark Program."
   ``get stream <stream-id> [<start-time>] [<end-time>] [<limit>]``,"Gets events from a Stream. The time format for <start-time> and <end-time> can be a timestamp in milliseconds or a relative time in the form of [+|-][0-9][d|h|m|s]. <start-time> is relative to current time; <end-time> is relative to <start-time>. Special constants ""min"" and ""max"" can be used to represent ""0"" and ""max timestamp"" respectively."
   ``get stream-stats <stream-id> [limit <limit>] [start <start-time>] [end <end-time>]``,"Gets statistics for a Stream. The <limit> limits how many Stream events to analyze; default is 100. The time format for <start-time> and <end-time> can be a timestamp in milliseconds or a relative time in the form of [+|-][0-9][d|h|m|s]. <start-time> is relative to current time; <end-time> is relative to <start-time>. Special constants ""min"" and ""max"" can be used to represent ""0"" and ""max timestamp"" respectively."
   ``get worker instances <app-id.worker-id>``,"Gets the instances of a Worker."
   ``get worker live <app-id.worker-id>``,"Gets the live info of a Worker."
   ``get worker logs <app-id.worker-id> [<start-time>] [<end-time>]``,"Gets the logs of a Worker."
   ``get worker runs <app-id.worker-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Worker."
   ``get worker runtimeargs <app-id.worker-id>``,"Gets the runtime arguments of a Worker."
   ``get worker status <app-id.worker-id>``,"Gets the status of a Worker."
   ``get workflow current <app-id.workflow-id> <runid>``,"Gets the currently running nodes of a Workflow for a given run id."
   ``get workflow runs <app-id.workflow-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Workflow."
   ``get workflow runtimeargs <app-id.workflow-id>``,"Gets the runtime arguments of a Workflow."
   ``get workflow schedules <app-id.workflow-id>``,"Resumes a schedule"
   ``get workflow status <app-id.workflow-id>``,"Gets the status of a Workflow."
   ``list app-templates``,"Lists all Application Templates."
   ``list apps``,"Lists all Applications."
   ``list flows``,"Lists all Flows."
   ``list mapreduce``,"Lists all MapReduce Programs."
   ``list programs``,"Lists all Programs."
   ``list services``,"Lists all Services."
   ``list spark``,"Lists all Spark Programs."
   ``list streams``,"Lists all Streams."
   ``list workers``,"Lists all Workers."
   ``list workflows``,"Lists all Workflows."
   ``load preferences app <local-file-path> <content-type> [<app-id>]``,"Set Preferences of an Application from a local Config File (supported formats = JSON)."
   ``load preferences flow <local-file-path> <content-type> [<app-id.flow-id>]``,"Set Preferences of a Flow from a local Config File (supported formats = JSON)."
   ``load preferences instance <local-file-path> <content-type> [<instance-id>]``,"Set Preferences of an Instance from a local Config File (supported formats = JSON)."
   ``load preferences mapreduce <local-file-path> <content-type> [<app-id.mapreduce-id>]``,"Set Preferences of a MapReduce Program from a local Config File (supported formats = JSON)."
   ``load preferences namespace <local-file-path> <content-type> [<namespace-name>]``,"Set Preferences of a Namespace from a local Config File (supported formats = JSON)."
   ``load preferences service <local-file-path> <content-type> [<app-id.service-id>]``,"Set Preferences of a Service from a local Config File (supported formats = JSON)."
   ``load preferences spark <local-file-path> <content-type> [<app-id.spark-id>]``,"Set Preferences of a Spark Program from a local Config File (supported formats = JSON)."
   ``load preferences worker <local-file-path> <content-type> [<app-id.worker-id>]``,"Set Preferences of a Worker from a local Config File (supported formats = JSON)."
   ``load preferences workflow <local-file-path> <content-type> [<app-id.workflow-id>]``,"Set Preferences of a Workflow from a local Config File (supported formats = JSON)."
   ``resume schedule <app-id.schedule-id>``,"Resumes a schedule"
   ``set flow runtimeargs <app-id.flow-id> <runtime-args>``,"Sets the runtime arguments of a Flow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set flowlet instances <app-id.flow-id.flowlet-id> <num-instances>``,"Sets the instances of a Flowlet."
   ``set mapreduce runtimeargs <app-id.mapreduce-id> <runtime-args>``,"Sets the runtime arguments of a MapReduce Program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set preferences app <runtime-args> [<app-id>]``,"Sets the preferences of an Application. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences flow <runtime-args> [<app-id.flow-id>]``,"Sets the preferences of a Flow. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences instance <runtime-args> [<instance-id>]``,"Sets the preferences of an Instance. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences mapreduce <runtime-args> [<app-id.mapreduce-id>]``,"Sets the preferences of a MapReduce Program. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences namespace <runtime-args> [<namespace-name>]``,"Sets the preferences of a Namespace. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences service <runtime-args> [<app-id.service-id>]``,"Sets the preferences of a Service. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences spark <runtime-args> [<app-id.spark-id>]``,"Sets the preferences of a Spark Program. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences worker <runtime-args> [<app-id.worker-id>]``,"Sets the preferences of a Worker. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set preferences workflow <runtime-args> [<app-id.workflow-id>]``,"Sets the preferences of a Workflow. <runtime-args> is specified in the format ""key1=v1 key2=v2""."
   ``set service instances <app-id.service-id> <num-instances>``,"Sets the instances of a Service."
   ``set service runtimeargs <app-id.service-id> <runtime-args>``,"Sets the runtime arguments of a Service. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set spark runtimeargs <app-id.spark-id> <runtime-args>``,"Sets the runtime arguments of a Spark Program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set stream format <stream-id> <format> [<schema>] [<settings>]``,"Sets the format of a Stream. <schema> is a sql-like schema ""column_name data_type, ..."" or avro-like json schema and <settings> is specified in the format ""key1=v1 key2=v2""."
   ``set stream notification-threshold <stream-id> <notification-threshold-mb>``,"Sets the Notification Threshold of a Stream."
   ``set stream properties <stream-id> <local-file-path>``,"Sets the properties of a Stream, such as TTL, format, and notification threshold."
   ``set stream ttl <stream-id> <ttl-in-seconds>``,"Sets the Time-to-Live (TTL) of a Stream."
   ``set worker instances <app-id.worker-id> <num-instances>``,"Sets the instances of a Worker."
   ``set worker runtimeargs <app-id.worker-id> <runtime-args>``,"Sets the runtime arguments of a Worker. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set workflow runtimeargs <app-id.workflow-id> <runtime-args>``,"Sets the runtime arguments of a Workflow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start flow <app-id.flow-id> [<runtime-args>]``,"Starts a Flow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start mapreduce <app-id.mapreduce-id> [<runtime-args>]``,"Starts a MapReduce Program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start service <app-id.service-id> [<runtime-args>]``,"Starts a Service. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start spark <app-id.spark-id> [<runtime-args>]``,"Starts a Spark Program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start worker <app-id.worker-id> [<runtime-args>]``,"Starts a Worker. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start workflow <app-id.workflow-id> [<runtime-args>]``,"Starts a Workflow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``stop flow <app-id.flow-id>``,"Stops a Flow."
   ``stop mapreduce <app-id.mapreduce-id>``,"Stops a MapReduce Program."
   ``stop service <app-id.service-id>``,"Stops a Service."
   ``stop spark <app-id.spark-id>``,"Stops a Spark Program."
   ``stop worker <app-id.worker-id>``,"Stops a Worker."
   ``suspend schedule <app-id.schedule-id>``,"Suspends a schedule"
   ``truncate stream <stream-id>``,"Truncates a Stream."
   **Adapter Lifecycle**
   ``create adapter <adapter-name> <adapter-spec-file>``,"Creates an Adapter."
   ``delete adapter <adapter-name>``,"Deletes an Adapter."
   ``describe adapter <adapter-name>``,"Lists all Adapters."
   ``get adapter logs <adapter-name>``,"Gets the logs of an Adapter."
   ``get adapter runs <adapter-name> [status <status>]``,"Gets the runs of an Adapter."
   ``get adapter status <adapter-name>``,"Gets the status of an Adapter."
   ``list adapters``,"Lists all Adapters."
   ``start adapter <adapter-name>``,"Starts an Adapter."
   ``stop adapter <adapter-name>``,"Stops an Adapter."
   **Dataset**
   ``create dataset instance <dataset-type> <new-dataset-name> [<dataset-properties>]``,"Creates a Dataset."
   ``delete dataset instance <dataset-name>``,"Deletes a Dataset."
   ``delete dataset module <dataset-module>``,"Deletes a Dataset module."
   ``deploy dataset module <new-dataset-module> <module-jar-file> <module-jar-classname>``,"Deploys a Dataset module."
   ``describe dataset instance <dataset-name>``,"Shows information about a Dataset."
   ``describe dataset module <dataset-module>``,"Shows information about a Dataset module."
   ``describe dataset type <dataset-type>``,"Shows information about a Dataset type."
   ``list dataset instances``,"Lists all Datasets."
   ``list dataset modules``,"Lists all Dataset modules."
   ``list dataset types``,"Lists all Dataset types."
   ``set dataset instance properties <dataset-name> <dataset-properties>``,"Sets properties for a Dataset."
   ``truncate dataset instance <dataset-name>``,"Truncates a Dataset."
   **Explore**
   ``execute <query> [<timeout>]``,"Executes a Query with optional <timeout> in minutes (default is no timeout)."
   **Metrics**
   ``get metric value <metric-name> [<tags>] [start <start>] [end <end>]``,"Gets the value of a metric. Provide <tags> as a map in the format 'tag1=value1 tag2=value2'."
   ``search metric names [<tags>]``,"Searches metric names. Provide <tags> as a map in the format 'tag1=value1 tag2=value2'."
   ``search metric tags [<tags>]``,"Searches metric tags. Provide <tags> as a map in the format 'tag1=value1 tag2=value2'."
   **Ingest**
   ``load stream <stream-id> <local-file-path> [<content-type>]``,"Loads a file to a Stream. The contents of the file will become multiple events in the Stream, based on the content type (avro/binary, text/csv, text/plain, text/tsv). If <content-type> is not provided, it will be detected by the file extension. Supported file extensions: avro, csv, log, tsv, txt."
   ``send stream <stream-id> <stream-event>``,"Sends an event to a Stream."
   **Egress**
   ``call service <app-id.service-id> <http-method> <endpoint> [headers <headers>] [body <body>] [body:file <local-file-path>]``,"Calls a Service endpoint. The <headers> are formatted as ""{'key':'value', ...}"". The request body may be provided either as a string or a file. To provide the body as a string, use ""body <body>"". To provide the body as a file, use ""body:file <local-file-path>""."
