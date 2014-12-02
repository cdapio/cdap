.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _cli:

============================================
Command-Line Interface API
============================================

Introduction
============

The Command-Line Interface (CLI) provides methods to interact with the CDAP server from within a shell,
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

Available Commands
==================

These are the available commands:

.. csv-table::
   :header: Command,Description
   :widths: 50, 50

   **General**
   ``exit``,"Exits the CLI"
   ``help``,"Prints this helper text"
   ``version``,"Prints the version"
   **Calling and Connecting**
   ``call procedure <app-id.procedure-id> <app-id.method-id> [<parameter-map>]``,"Calls a Procedure"
   ``call service <app-id.service-id> <http-method> <endpoint> [headers <headers>] [body <body>]``,"Calls a Service endpoint. The header is formatted as ""{'key':'value', ...}"" and the body is a String."
   ``connect <cdap-instance-uri>``,"Connects to a CDAP instance. <credential(s)> parameter(s) could be used if authentication is enabled in the gateway server."
   **Creating**
   ``create dataset instance <dataset-type> <new-dataset-name>``,"Creates a Dataset"
   ``create stream <new-stream-id>``,"Creates a Stream"
   **Deleting**
   ``delete app <app-id>``,"Deletes an application"
   ``delete dataset instance <dataset-name>``,"Deletes a Dataset"
   ``delete dataset module <dataset-module>``,"Deletes a Dataset module"
   **Deploying**
   ``deploy app <app-jar-file>``,"Deploys an application"
   ``deploy dataset module <new-dataset-module> <module-jar-file> <module-jar-classname>``,"Deploys a Dataset module"
   **Describing**
   ``describe app <app-id>``,"Shows detailed information about an application"
   ``describe dataset module <dataset-module>``,"Shows information about a Dataset module"
   ``describe dataset type <dataset-type>``,"Shows information about a Dataset type"
   ``describe stream <stream-id>``,"Shows detailed information about a Stream"
   **Executing Queries**
   ``execute <query>``,"Executes a Dataset query"
   **Retrieving Information**
   ``get endpoints service <app-id.service-id>``,"List the endpoints that a Service exposes"
   ``get flow live <app-id.flow-id>``,"Gets the live info of a Flow"
   ``get flow logs <app-id.flow-id> [<start-time>] [<end-time>]``,"Gets the logs of a Flow"
   ``get flow runs <app-id.flow-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Flow"
   ``get flow status <app-id.flow-id>``,"Gets the status of a Flow"
   ``get flowlet instances <app-id.flow-id.flowlet-id>``,"Gets the instances of a Flowlet"
   ``get mapreduce logs <app-id.mapreduce-id> [<start-time>] [<end-time>]``,"Gets the logs of a MapReduce job"
   ``get mapreduce runs <app-id.mapreduce-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a MapReduce job"
   ``get mapreduce status <app-id.mapreduce-id>``,"Gets the status of a MapReduce job"
   ``get procedure instances <app-id.procedure-id>``,"Gets the instances of a Procedure"
   ``get procedure live <app-id.procedure-id>``,"Gets the live info of a Procedure"
   ``get procedure logs <app-id.procedure-id> [<start-time>] [<end-time>]``,"Gets the logs of a Procedure"
   ``get procedure runs <app-id.procedure-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Procedure"
   ``get procedure status <app-id.procedure-id>``,"Gets the status of a Procedure"
   ``get runnable instances <app-id.runnable-id>``,"Gets the instances of a Runnable"
   ``get runnable logs <app-id.runnable-id> [<start-time>] [<end-time>]``,"Gets the logs of a Runnable"
   ``get runnable runs <app-id.runnable-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Runnable"
   ``get service status <app-id.service-id>``,"Gets the status of a Service"
   ``get spark logs <app-id.spark-id> [<start-time>] [<end-time>]``,"Gets the logs of a Spark job"
   ``get spark runs <app-id.spark-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Spark job"
   ``get spark status <app-id.spark-id>``,"Gets the status of a Spark job"
   ``get stream <stream-id> [<start-time>] [<end-time>] [<limit>]``,"Gets events from a Stream. The time format for <start-time> and <end-time> can be a timestamp in milliseconds or a relative time in the form of [+\-][0-9]+[hms]. For <start-time>, it is relative to current time; for <end-time>, it is relative to start time. Special constants ""min"" and ""max"" can also be used to represent ""0"" and ""max timestamp"" respectively."
   ``get workflow runs <app-id.workflow-id> [<status>] [<start-time>] [<end-time>] [<limit>]``,"Gets the run history of a Workflow"
   ``get workflow status <app-id.workflow-id>``,"Gets the status of a Workflow"
   **Listing Elements**
   ``list apps``,"Lists all applications"
   ``list dataset instances``,"Lists all Datasets"
   ``list dataset modules``,"Lists Dataset modules"
   ``list dataset types``,"Lists Dataset types"
   ``list flows``,"Lists Flows"
   ``list mapreduce``,"Lists MapReduce jobs"
   ``list procedures``,"Lists Procedures"
   ``list programs``,"Lists all programs"
   ``list services``,"Lists Services"
   ``list spark``,"Lists Spark jobs"
   ``list streams``,"Lists Streams"
   ``list workflows``,"Lists Workflows"
   **Sending Events**
   ``send stream <stream-id> <stream-event>``,"Sends an event to a Stream"
   **Setting**
   ``set flowlet instances <app-id.flow-id.flowlet-id> <num-instances>``,"Sets the instances of a Flowlet"
   ``set procedure instances <app-id.procedure-id> <num-instances>``,"Sets the instances of a Procedure"
   ``set runnable instances <app-id.runnable-id> <num-instances>``,"Sets the instances of a Runnable"
   ``set stream ttl <stream-id> <ttl-in-seconds>``,"Sets the Time-to-Live (TTL) of a Stream"
   **Starting**
   ``start flow <app-id.flow-id>``,"Starts a Flow"
   ``start mapreduce <app-id.mapreduce-id>``,"Starts a MapReduce job"
   ``start procedure <app-id.procedure-id>``,"Starts a Procedure"
   ``start service <app-id.service-id>``,"Starts a Service"
   ``start spark <app-id.spark-id>``,"Starts a Spark job"
   ``start workflow <app-id.workflow-id>``,"Starts a Workflow"
   **Stopping**
   ``stop flow <app-id.flow-id>``,"Stops a Flow"
   ``stop mapreduce <app-id.mapreduce-id>``,"Stops a MapReduce job"
   ``stop procedure <app-id.procedure-id>``,"Stops a Procedure"
   ``stop service <app-id.service-id>``,"Stops a Service"
   ``stop spark <app-id.spark-id>``,"Stops a Spark job"
   ``stop workflow <app-id.workflow-id>``,"Stops a Workflow"
   **Truncating**
   ``truncate dataset instance <dataset-name>``,"Truncates a Dataset"
   ``truncate stream <stream-id>``,"Truncates a Stream"

