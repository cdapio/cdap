.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _http-restful-api-logging:

========================
Logging HTTP RESTful API
========================

.. highlight:: console

Use the CDAP Logging HTTP RESTful API to download the logs of applications and the system,
with the option of formating and filtering the logs that are downloaded.

Additional details on logging can be found in the :ref:`Administration Manual: Logging and Monitoring <logging-monitoring>`.


.. Base URL explanation
.. --------------------
.. include:: base-url.txt


Downloading Application Logs
============================
Logs emitted by a *flow*, *MapReduce*, *service*, or *Spark* program running in CDAP can be
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
     - One of ``flows``, ``mapreduce``, ``services``, ``spark``, or ``workflows``
   * - ``program-id``
     - Name of the program (*flow*, *MapReduce*, *service*, *Spark*, *workflow*) being called
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
     - One of ``flows``, ``mapreduce``, ``services``, ``spark``, or ``workflows``
   * - ``program-id``
     - Name of the program (*flow*, *MapReduce*, *service*, *Spark*, *workflow*) being called
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


.. _http-restful-api-logging_downloading_system_logs:

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
     - ``GET /v3/services/system/appfabric/logs?start=1428541200&stop=1428541500``
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
