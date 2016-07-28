.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _cli:

============================================
Command Line Interface API
============================================

Introduction
============

The Command Line Interface (CLI, or CDAP CLI) provides methods to interact with the CDAP server from within a shell,
similar to the HBase or ``bash`` shells. It is located within the SDK, at ``bin/cdap-cli`` as either a bash
script or a Windows ``.bat`` file.

It can be :ref:`installed on Distributed CDAP <packages-cli-package-installation>`, in
which case it will be installed either in ``/opt/cdap/cli`` or, in the case of Cloudera
Manager, ``opt/cloudera/parcels/cdap/cli/``.

The CLI may be used in two ways: interactive mode and non-interactive mode.

Interactive Mode
----------------

.. highlight:: console

To run the CLI in interactive mode, run the ``cdap-cli.sh`` executable with no arguments from a terminal:

.. tabbed-parsed-literal::
  :tabs: Linux,Windows,"Distributed CDAP","Cloudera Manager Clusters"
  :dependent: linux-windows
  :mapping: Linux,Windows,Linux,Linux
  :languages: console,shell-session,console,console

  .. Linux

  $ ./bin/cdap-cli.sh

  .. Windows

  > .\bin\cdap-cli.bat
  
  .. Distributed CDAP

  $ /opt/cdap/cli/bin/cdap-cli.sh
  
  .. Cloudera Manager Clusters
  
  $ /opt/cloudera/parcels/cdap/cli/bin/cdap-cli.sh


The executable should bring you into a shell, with a prompt similar to:

.. container:: highlight

  .. parsed-literal::
  
    cdap (http://localhost:10000/namespace:default)>

This indicates that the CLI is currently set to interact with the CDAP server at ``localhost``.

- To interact with a different CDAP server by default, set the environment variable ``CDAP_HOST`` to a hostname.
- To change the current CDAP server, run the CLI command ``connect example.com``.
- To connect to an SSL-enabled CDAP server, run the CLI command ``connect https://example.com``.

For example, with ``CDAP_HOST`` set to ``example.com``, the CLI would be interacting with
a CDAP instance at ``example.com``, port ``10000``::

  cdap (http://example.com:10000/namespace:default)>

To list all of the available commands, enter the CLI command ``help``::

  cdap (http://localhost:10000/namespace:default)> help
  
In this documentation, to save space, this prompt is abbreviated to:

.. container:: highlight

  .. parsed-literal::
  
    |cdap >|
  

Non-Interactive Mode
--------------------

To run the CLI in non-interactive mode, run the CDAP CLI executable, passing the command you want executed
as the argument. For example, to list all applications currently deployed to CDAP, execute:

.. tabbed-parsed-literal::

  $ cdap-cli.sh list apps

Connecting to Secure CDAP Instances
-----------------------------------

When connecting to secure CDAP instances, the CLI will look for an access token located at
``~/.cdap.accesstoken.<hostname>`` and use it if it exists and is valid. If not, the CLI will prompt
you for the required credentials to acquire an access token from the CDAP instance. Once acquired,
the CLI will save it to ``~/.cdap.accesstoken.<hostname>`` for later use and use it for the rest of
the current CLI session.

Options
-------

The CLI can be started with command-line options:

.. tabbed-parsed-literal::

  .. Linux

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

  .. Windows

  usage: cdap-cli.bat [--autoconnect <true|false>] [--debug] [--help]
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

Certain commands (``cli render as``, ``connect``, and ``use namespace``) affect how the CLI works for the duration of a session.

- The command ``cli render as <table-renderer>`` (from the `General`_ commands) sets how
  table data is rendered. Valid options are either ``alt`` (the default) and ``csv``. As
  the ``alt`` option may split a cell into multiple lines, you may need to use ``csv`` if
  you want to copy and paste the results into another application or include in a message.

  - With ``cli render as alt`` (the default), a command such as ``list apps`` will be output as::

      +================================+
      | app id      | description      |
      +=============+==================+
      | PurchaseApp | Some description |
      +=============+==================+

  - With ``cli render as csv``, the same ``list apps`` would be output as::

      app id,description
      PurchaseApp,Some description
      
- The command ``connect`` (from the `General`_ commands) allows you to connect to a CDAP
  instance.

- The command ``use namespace`` (from the `Namespace`_ commands) allows you to set the
  namespace used with all subsequent commands. By default, all sessions start in the
  ``default`` namespace.

.. _cli-available-commands:

Available Commands
==================

These are the available commands:

.. include:: ../target/_includes/cdap-cli-table.rst
