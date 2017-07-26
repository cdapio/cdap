.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

.. _http-restful-api-logging:

========================
Logging HTTP RESTful API
========================

.. highlight:: console

Use the CDAP Logging HTTP RESTful API to download the logs of applications and the system,
with the option of formating and filtering the logs that are downloaded, and setting the
log levels of programs at runtime when under Distributed CDAP mode.

Additional details on logging can be found in the :ref:`Administration Manual: Logging and Monitoring <logging-monitoring>`.


.. Base URL explanation
.. --------------------
.. include:: base-url.txt


Downloading Application Logs
============================
Logs emitted by a *flow*, *MapReduce*, *service*, *Spark*, *worker*, or *workflow* program running in CDAP can be
downloaded with the Logging HTTP RESTful API. To do that, send an HTTP GET request::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/logs?start=<ts>&stop=<ts>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application being called
   * - ``program-type``
     - One of ``flows``, ``mapreduce``, ``services``, ``spark``, ``workers``, or ``workflows``
   * - ``program-id``
     - Name of the program being called
   * - ``ts``
     - *Start* and *stop* times, given as seconds since the start of the Epoch.

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET /v3/namespaces/default/apps/WordCount/flows/WordCountFlow/``\
       ``logs?start=1382576400&stop=1382576700``
   * - Description
     - Return the logs for all the events from the flow *WordCountFlow* of the *WordCount*
       application in the namespace *default*,
       beginning ``Thu, 24 Oct 2013 01:00:00 GMT`` and
       ending ``Thu, 24 Oct 2013 01:05:00 GMT`` (five minutes later)


Downloading Application Logs for a Run
======================================
To download logs for a program run, send an HTTP GET request::

  GET /v3/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/runs/<run-id>/logs?start=<ts>&stop=<ts>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application being called
   * - ``program-type``
     - One of ``flows``, ``mapreduce``, ``services``, ``spark``, ``workers``, or ``workflows``
   * - ``program-id``
     - Name of the program being called
   * - ``run-id``
     - Run id of the program run
   * - ``ts``
     - *Start* and *stop* times, given as seconds since the start of the Epoch.

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET /v3/namespaces/default/apps/WordCount/flows/WordCountFlow/``\
       ``runs/c826e692-ef8c-11e4-953d-d6686e126da6/logs?start=1382576400&stop=1382576700``
   * - Description
     - Return the logs for all the events from the flow *WordCountFlow* of the *WordCount*
       application in the namespace *default* for run c826e692-ef8c-11e4-953d-d6686e126da6,
       beginning ``Thu, 24 Oct 2013 01:00:00 GMT`` and
       ending ``Thu, 24 Oct 2013 01:05:00 GMT`` (five minutes later)


.. _http-restful-api-logging-downloading-system-logs:

Downloading System Logs
=======================
Logs emitted by a system service running in CDAP can be downloaded with the Logging HTTP
API. To do that, send an HTTP GET request::

  GET /v3/system/services/<service-id>/logs?start=<ts>&stop=<ts>

where:

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``service-id``
     - One of ``appfabric``, ``dataset.executor``, ``explore.service``, ``metrics``, ``metrics.processor``, ``streams``, ``transaction``
   * - ``ts``
     - *Start* and *stop* times, given as seconds since the start of the Epoch.

Note that the start and stop times are **not** optional.

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET /v3/system/services/appfabric/logs?start=1428541200&stop=1428541500``
   * - Description
     - Return the logs for the *AppFabric* service
       beginning ``Thu, 09 Apr 2015 01:00:00 GMT`` and
       ending ``Thu, 09 Apr 2015 01:05:00 GMT`` (five minutes later)

Formatting and Filtering
========================

Formatting
----------

The output is, by default, formatted as HTML-embeddable text; that is, characters that
have a special meaning in HTML will be escaped. A line of a log may look like this::

  2013-10-23 18:03:09,793 - INFO [FlowletProcessDriver-source-0-
        executor:c.c.e.c.StreamSource@-1] – source: Emitting line: this is an &amp; character

**Note:** The context of the log line shows the name of the flowlet (*source*), its instance number (0) as
well as the original line in the application code. The character *&* is escaped as ``&amp;``; if you don’t desire
this escaping, you can turn it off by adding the parameter ``&escape=false`` to the request URL.

The text can also be formatted as JSON. To do so, suffix your URL with the parameter
``&format=json``.

Filtering
---------
A filter string can be supplied as an optional parameter. It will filter the returned log entries
to those that match the supplied string.


.. _http-restful-api-logging-changing-program-log-levels:

Changing Program Log Levels
===========================
Log levels can be set for a particular run of a program. Once changed, they can be reset back to what
they started with by using the :ref:`reset endpoint <http-restful-api-logging-resetting>` shown below.

**Note:** The log levels can only be changed for programs that are running under Distributed CDAP.
Currently, only flows, services, or workers are supported.

Setting Program Log Levels
--------------------------
To set the log levels of a program run at runtime under Distributed CDAP, submit an HTTP PUT request::

  PUT /v3/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/runs/<run-id>/loglevels

To set the log levels of a program run in a particular version of an application::

  PUT /v3/namespaces/<namespace-id>/apps/<app-id>/versions/<version-id>/<program-type>/<program-id>/runs/<run-id>/loglevels

To set the log levels of a flowlet in the run of a flow::

  PUT /v3/namespaces/<namespace-id>/apps/<app-id>/flows/<flow-id>/flowlet/<flowlet-id>/runs/<run-id>/loglevels

To set the log levels of a flowlet in the run of a flow in a particular version of an application::

  PUT /v3/namespaces/<namespace-id>/apps/<app-id>/versions/<version-id>/flows/<flow-id>/flowlet/<flowlet-id>/runs/<run-id>/loglevels

and, in all cases, with a JSON map in the request body consisting of a map of logger names and log level pairs, such as::

  '{ "co.cask.cdap":"ERROR", "ROOT":"TRACE" }'

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application
   * - ``version-id``
     - Version of the application
   * - ``program-type``
     - One of ``flows``, ``services``, or ``workers``
   * - ``program-id``
     - Name of the program
   * - ``flow-id``
     - Name of the flow
   * - ``flowlet-id``
     - Name of the flowlet
   * - ``run-id``
     - UUID of the program run

For example::

  PUT /v3/namespaces/default/apps/HelloWorld/flows/WhoFlow/run-id/cdec1791-c2c0-11e6-ac6b-42010a800022/loglevels \
      -d '{ "co.cask.cdap":"ERROR", "ROOT":"TRACE" }'

will update the log levels of the flow *WhoFlow* in the *HelloWorld* application with two log level arguments.

.. _http-restful-api-logging-resetting:

Resetting Program Log Levels
----------------------------
Resetting the log levels will change the log levels back to what they were when the program was started.

To reset the log levels of a program run at runtime under Distributed CDAP mode, submit an HTTP PUT request::

  POST /v3/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/runs/<run-id>/resetloglevels

To reset the log levels of a program run in a particular version of an application::

  POST /v3/namespaces/<namespace-id>/apps/<app-id>/versions/<version-id>/<program-type>/<program-id>/runs/<run-id>/resetloglevels

To reset the log levels of a flowlet in the run of a flow::

  POST /v3/namespaces/<namespace-id>/apps/<app-id>/flows/<flow-id>/flowlet/<flowlet-id>/runs/<run-id>/resetloglevels

To reset the log levels of a flowlet in the run of a flow in a particular version of an application::

  POST /v3/namespaces/<namespace-id>/apps/<app-id>/versions/<version-id>/flows/<flow-id>/flowlet/<flowlet-id>/runs/<run-id>/resetloglevels

and, in all cases, with an optional JSON array in the request body consisting of the logger names to be reset.
If the body is not provided or is empty, it will reset the log levels of all logger names.

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``namespace-id``
     - Namespace ID
   * - ``app-id``
     - Name of the application
   * - ``version-id``
     - Version of the application
   * - ``program-type``
     - One of ``flows``, ``services``, or ``workers``
   * - ``program-id``
     - Name of the program
   * - ``flow-id``
     - Name of the flow
   * - ``flowlet-id``
     - Name of the flowlet
   * - ``run-id``
     - UUID of the program run

For example::

  POST /v3/namespaces/default/apps/HelloWorld/flows/WhoFlow/run-id/cdec1791-c2c0-11e6-ac6b-42010a800022/resetloglevels \
      -d '[ "co.cask.cdap", "ROOT" ]'

will reset the log level of the *WhoFlow* flow in the *HelloWorld* application for the two logger names.
