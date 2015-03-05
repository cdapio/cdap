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
~/.cdap.accesstoken.<hostname> and use it if it exists and is valid. If not, the CLI will prompt
you for the required credentials to acquire an access token from the CDAP instance. Once acquired,
the CLI will save it to ~/.cdap.accesstoken.<hostname> for later use and use it for the rest of
the current CLI session.

Options
-------

The CLI may be started with command-line options, as detailed below::

  usage: cdap-cli.sh [--autoconnect <true|false>] [--debug] [--help]
                     [--verify-ssl <true|false>] [--uri <arg>]
   -a,--autoconnect <arg>   If "true", try provided connection (from uri)
                            upon launch or try default connection if none
                            provided. Defaults to "true".
   -d,--debug               Print exception stack traces.
   -h,--help                Print the usage message.
   -s,--verify-ssl <arg>    If "true", verify SSL certificate when making
                            requests. Defaults to "true".
   -u,--uri <arg>           CDAP instance URI to interact with in the format
                            "[http[s]://]<hostname>[:<port>[/<namespace>]]".
                            Defaults to "http://127.0.0.1:10000/default".

.. _cli-available-commands:

Available Commands
==================

These are the available commands:

.. csv-table::
   :header: Command,Description
   :widths: 50, 50

   **General**
   ``connect <cdap-instance-uri>``,"Connects to a CDAP instance."
   ``exit``,"Exits the CLI."
   ``help [<command-category>]``,"Prints this helper text. Optionally, provide <command-category> to get help with a specific category."
   ``quit``,"Exits the CLI."
   ``search commands <query>``,"Searches available commands using regex."
   ``version``,"Prints the CDAP version."
   **Namespace**
   ``create namespace <namespace-id> [<namespace-display-name>] [<namespace-description>]``,"Creates a namespace in CDAP."
   ``delete namespace <namespace-id>``,"Deletes a Namespace."
   ``describe namespace <namespace-id>``,"Describes a Namespace."
   ``list namespaces``,"Lists all Namespaces."
   ``use namespace <namespace-id>``,"Changes the current Namespace to <namespace-id>."
   **Lifecycle**
   ``create adapter <adapter-name> type <adapter-type> [props <adapter-props>] src <adapter-source> [src-props <adapter-source-config>] sink <adapter-sink> [sink-props <adapter-sink-config>]``,"Creates an Adapter."
   ``create stream <new-stream-id>``,"Creates a Stream."
   ``create stream-conversion adapter <adapter-name> on <stream-id> [frequency <frequency>] [format <format>] [schema <schema>] [headers <headers>] [to <dataset-name>]``,"Creates a Stream conversion Adapter that periodically reads from a Stream and writes to a time-partitioned fileset. <frequency> is a number followed by a 'm', 'h', or 'd' for minute, hour, or day. <format> is the name of the stream format, such as 'text', 'avro', 'csv', or 'tsv'. <schema> is a sql-like schema of comma separated column name followed by column type. <headers> is a comma separated list of Stream headers to include in the output schema. <dataset-name> is the name of the time-partitioned fileset to write to."
   ``delete adapter <adapter-name>``,"Deletes an Adapter."
   ``delete app <app-id>``,"Deletes an Application."
   ``deploy app <app-jar-file>``,"Deploys an Application."
   ``describe app <app-id>``,"Shows information about an Application."
   ``describe stream <stream-id>``,"Shows detailed information about a Stream."
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
   ``get procedure instances <app-id.procedure-id>``,"Gets the instances of a Procedure."
   ``get procedure live <app-id.procedure-id>``,"Gets the live info of a Procedure."
   ``get procedure logs <app-id.procedure-id> [<start-time>] [<end-time>]``,"Gets the logs of a Procedure."
   ``get procedure runs <app-id.procedure-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Procedure."
   ``get procedure runtimeargs <app-id.procedure-id>``,"Gets the runtime arguments of a Procedure."
   ``get procedure status <app-id.procedure-id>``,"Gets the status of a Procedure."
   ``get workers instances <app-id.worker-id>``,"Gets the instances of a Worker."
   ``get worker live <app-id.worker-id>``,"Gets the live info of a Worker."
   ``get worker logs <app-id.worker-id> [<start-time>] [<end-time>]``,"Gets the logs of a Worker."
   ``get worker runs <app-id.worker-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Worker."
   ``get worker runtimeargs <app-id.worker-id>``,"Gets the runtime arguments of a Worker."
   ``get worker status <app-id.worker-id>``,"Gets the status of a Worker."
   ``get runnable instances <app-id.service-id.runnable-id>``,"Gets the instances of a Runnable."
   ``get runnable logs <app-id.service-id.runnable-id> [<start-time>] [<end-time>]``,"Gets the logs of a Runnable."
   ``get runnable runs <app-id.service-id.runnable-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Runnable."
   ``get service runtimeargs <app-id.service-id>``,"Gets the runtime arguments of a Service."
   ``get service status <app-id.service-id>``,"Gets the status of a Service."
   ``get spark logs <app-id.spark-id> [<start-time>] [<end-time>]``,"Gets the logs of a Spark Program."
   ``get spark runs <app-id.spark-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Spark Program."
   ``get spark runtimeargs <app-id.spark-id>``,"Gets the runtime arguments of a Spark Program."
   ``get spark status <app-id.spark-id>``,"Gets the status of a Spark Program."
   ``get stream <stream-id> [<start-time>] [<end-time>] [<limit>]``,"Gets events from a Stream. The time format for <start-time> and <end-time> can be a timestamp in milliseconds or a relative time in the form of [+|-][0-9][d|h|m|s]. <start-time> is relative to current time; <end-time> is relative to <start-time>. Special constants ""min"" and ""max"" can be used to represent ""0"" and ""max timestamp"" respectively."
   ``get stream-stats <stream-id> [limit <limit>] [start <start-time>] [end <end-time>]``,"Gets statistics for a Stream. The <limit> limits how many Stream events to analyze; default is 100. The time format for <start-time> and <end-time> can be a timestamp in milliseconds or a relative time in the form of [+|-][0-9][d|h|m|s]. <start-time> is relative to current time; <end-time> is relative to <start-time>. Special constants ""min"" and ""max"" can be used to represent ""0"" and ""max timestamp"" respectively."
   ``get workflow runs <app-id.workflow-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Workflow."
   ``get workflow runtimeargs <app-id.workflow-id>``,"Gets the runtime arguments of a Workflow."
   ``get workflow status <app-id.workflow-id>``,"Gets the status of a Workflow."
   ``list adapters``,"Lists all Adapters."
   ``list apps``,"Lists all Applications."
   ``list flows``,"Lists all Flows."
   ``list mapreduce``,"Lists all MapReduce Programs."
   ``list procedures``,"Lists all Procedures."
   ``list programs``,"Lists all Programs."
   ``list services``,"Lists all Services."
   ``list spark``,"Lists all Spark Programs."
   ``list streams``,"Lists all Streams."
   ``list workflows``,"Lists all Workflows."
   ``set flow runtimeargs <app-id.flow-id> <runtime-args>``,"Sets the runtime arguments of a Flow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set flowlet instances <app-id.flow-id.flowlet-id> <num-instances>``,"Sets the instances of a Flowlet."
   ``set mapreduce runtimeargs <app-id.mapreduce-id> <runtime-args>``,"Sets the runtime arguments of a MapReduce Program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set procedure instances <app-id.procedure-id> <num-instances>``,"Sets the instances of a Procedure."
   ``set procedure runtimeargs <app-id.procedure-id> <runtime-args>``,"Sets the runtime arguments of a Procedure. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set runnable instances <app-id.service-id.runnable-id> <num-instances>``,"Sets the instances of a Runnable."
   ``set service runtimeargs <app-id.service-id> <runtime-args>``,"Sets the runtime arguments of a Service. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set spark runtimeargs <app-id.spark-id> <runtime-args>``,"Sets the runtime arguments of a Spark Program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``set stream format <stream-id> <format> [<schema>] [<settings>]``,"Sets the format of a Stream. <schema> is a sql-like schema ""column_name data_type, ..."" or avro-like json schema and <settings> is specified in the format ""key1=v1, key2=v2""."
   ``set stream properties <stream-id> <local-file-path>``,"Sets the properties of a Stream, such as TTL, format, and notification threshold."
   ``set stream ttl <stream-id> <ttl-in-seconds>``,"Sets the Time-to-Live (TTL) of a Stream."
   ``set workflow runtimeargs <app-id.workflow-id> <runtime-args>``,"Sets the runtime arguments of a Workflow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start flow <app-id.flow-id> [<runtime-args>]``,"Starts a Flow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start mapreduce <app-id.mapreduce-id> [<runtime-args>]``,"Starts a MapReduce Program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start procedure <app-id.procedure-id> [<runtime-args>]``,"Starts a Procedure. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start service <app-id.service-id> [<runtime-args>]``,"Starts a Service. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start spark <app-id.spark-id> [<runtime-args>]``,"Starts a Spark Program. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``start workflow <app-id.workflow-id> [<runtime-args>]``,"Starts a Workflow. <runtime-args> is specified in the format ""key1=a key2=b""."
   ``stop flow <app-id.flow-id>``,"Stops a Flow."
   ``stop mapreduce <app-id.mapreduce-id>``,"Stops a MapReduce Program."
   ``stop procedure <app-id.procedure-id>``,"Stops a Procedure."
   ``stop service <app-id.service-id>``,"Stops a Service."
   ``stop spark <app-id.spark-id>``,"Stops a Spark Program."
   ``stop workflow <app-id.workflow-id>``,"Stops a Workflow."
   ``truncate stream <stream-id>``,"Truncates a Stream."
   **Dataset**
   ``create dataset instance <dataset-type> <new-dataset-name>``,"Creates a Dataset."
   ``delete dataset instance <dataset-name>``,"Deletes a Dataset."
   ``delete dataset module <dataset-module>``,"Deletes a Dataset module."
   ``deploy dataset module <new-dataset-module> <module-jar-file> <module-jar-classname>``,"Deploys a Dataset module."
   ``describe dataset module <dataset-module>``,"Shows information about a Dataset module."
   ``describe dataset type <dataset-type>``,"Shows information about a Dataset type."
   ``list dataset instances``,"Lists all Datasets."
   ``list dataset modules``,"Lists all Dataset modules."
   ``list dataset types``,"Lists all Dataset types."
   ``truncate dataset instance <dataset-name>``,"Truncates a Dataset."
   **Explore**
   ``execute <query> [<timeout>]``,"Executes a Dataset query with an optional timeout (default 60) in minutes."
   **Ingest**
   ``load stream <stream-id> <local-file-path> [<content-type>]``,"Loads a file to a Stream. The content of the file will become multiple events in the Stream, based on the content type. If <content-type> is not provided, it will be detected by the file extension."
   ``send stream <stream-id> <stream-event>``,"Sends an event to a Stream."
   **Egress**
   ``call procedure <app-id.procedure-id> <app-id.method-id> [<parameter-map>]``,"Calls a Procedure."
   ``call service <app-id.service-id> <http-method> <endpoint> [headers <headers>] [body <body>]``,"Calls a Service endpoint. The <headers> are formatted as ""{'key':'value', ...}"" and the <body> is a String."

