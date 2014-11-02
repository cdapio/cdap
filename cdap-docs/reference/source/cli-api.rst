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
   ``help``,Prints this helper text
   ``version``,Prints the version
   ``exit``,Exits the shell
   **Calling and Executing**
   ``call procedure <app-id>.<procedure-id> <method-id> <parameters-map>``,"Calls a Procedure, passing in the parameters as a JSON String map"
   ``execute <query>``,Executes a Dataset query
   **Creating**
   ``create dataset instance <type-name> <new-dataset-name>``,Creates a Dataset
   ``create stream <new-stream-id>``,Creates a Stream
   **Deleting**
   ``delete app <app-id>``,Deletes an Application
   ``delete dataset instance <dataset-name>``,Deletes a Dataset
   ``delete dataset module <module-name>``,Deletes a Dataset module
   **Deploying**
   ``deploy app <app-jar-file>``,Deploys an application
   ``deploy dataset module <module-jar-file> <module-name> <module-jar-classname>``,Deploys a Dataset module
   **Describing**
   ``describe app <app-id>``,Shows detailed information about an application
   ``describe dataset module <module-name>``,Shows information about a Dataset module
   ``describe dataset type <type-name>``,Shows information about a Dataset type
   **Retrieving Information**
   ``get history flow <app-id>.<program-id>``,Gets the run history of a Flow
   ``get history mapreduce <app-id>.<program-id>``,Gets the run history of a MapReduce job
   ``get history procedure <app-id>.<program-id>``,Gets the run history of a Procedure
   ``get history runnable <app-id>.<program-id>``,Gets the run history of a Service Handler/Worker
   ``get history workflow <app-id>.<program-id>``,Gets the run history of a Workflow
   ``get instances flowlet <app-id>.<program-id>``,Gets the instances of a Flowlet
   ``get instances procedure <app-id>.<program-id>``,Gets the instances of a Procedure
   ``get instances runnable <app-id>.<program-id>``,Gets the instances of a Service Handler/Worker
   ``get live flow <app-id>.<program-id>``,Gets the live info of a Flow
   ``get live procedure <app-id>.<program-id>``,Gets the live info of a Procedure
   ``get logs flow <app-id>.<program-id> [<start-time> <end-time>]``,Gets the logs of a Flow
   ``get logs mapreduce <app-id>.<program-id> [<start-time> <end-time>]``,Gets the logs of a MapReduce job
   ``get logs procedure <app-id>.<program-id> [<start-time> <end-time>]``,Gets the logs of a Procedure
   ``get logs runnable <app-id>.<program-id> [<start-time> <end-time>]``,Gets the logs of a Service Handler/Worker
   ``get status flow <app-id>.<program-id>``,Gets the status of a Flow
   ``get status mapreduce <app-id>.<program-id>``,Gets the status of a MapReduce job
   ``get status procedure <app-id>.<program-id>``,Gets the status of a Procedure
   ``get status service <app-id>.<program-id>``,Gets the status of a Service
   ``get status workflow <app-id>.<program-id>``,Gets the status of a Workflow
   **Listing Elements**
   ``list apps``,Lists all applications
   ``list dataset instances``,Lists all Datasets
   ``list dataset modules``,Lists Dataset modules
   ``list dataset types``,Lists Dataset types
   ``list flows``,Lists Flows
   ``list mapreduce``,Lists MapReduce jobs
   ``list procedures``,Lists Procedures
   ``list programs``,Lists all programs
   ``list streams``,Lists Streams
   ``list workflows``,Lists Workflows
   **Sending Events**
   ``send stream <stream-id> <stream-event>``,Sends an event to a Stream
   **Setting**
   ``set instances flowlet <program-id> <num-instances>``,Sets the instances of a Flowlet
   ``set instances procedure <program-id> <num-instances>``,Sets the instances of a Procedure
   ``set instances runnable <program-id> <num-instances>``,Sets the instances of a Service Handler/Worker
   ``set stream ttl <stream-id> <ttl-in-seconds>``,Sets the Time-to-Live (TTL) of a Stream
   **Starting**
   ``start flow <program-id>``,Starts a Flow
   ``start mapreduce <program-id>``,Starts a MapReduce job
   ``start procedure <program-id>``,Starts a Procedure
   ``start service <program-id>``,Starts a Service
   ``start workflow <program-id>``,Starts a Workflow
   **Stopping**
   ``stop flow <program-id>``,Stops a Flow
   ``stop mapreduce <program-id>``,Stops a MapReduce job
   ``stop procedure <program-id>``,Stops a Procedure
   ``stop service <program-id>``,Stops a Service
   ``stop workflow <program-id>``,Stops a Workflow
   **Truncating**
   ``truncate dataset instance``,Truncates a Dataset
   ``truncate stream``,Truncates a Stream

