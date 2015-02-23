.. meta::
    :author: Cask Data, Inc.
    :description: HTTP RESTful Interface to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _http-restful-api-logging:

===========================================================
Logging HTTP RESTful API
===========================================================

.. highlight:: console

Downloading Application Logs
----------------------------
Logs emitted by a *Flow*, *MapReduce*, *Service*, or *Spark* program running in CDAP can be
downloaded with the Logging HTTP API. To do that, send an HTTP GET request::

  GET <base-url>/namespaces/<namespace-id>/apps/<app-id>/<program-type>/<program-id>/logs?start=<ts>&stop=<ts>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<app-id>``
     - Name of the Application being called
   * - ``<program-type>``
     - One of ``flows``, ``mapreduce``, ``services``, or ``spark``
   * - ``<program-id>``
     - Name of the program (*Flow*, *MapReduce*, *Service*, *Spark*) being called
   * - ``<ts>``
     - *Start* and *stop* times, given as seconds since the start of the Epoch.

.. rubric:: Example
.. list-table::
   :widths: 20 80
   :stub-columns: 1

   * - HTTP Method
     - ``GET <base-url>/namespaces/default/apps/WordCount/flows/WordCountFlow/``
       ``logs?start=1382576400&stop=1382576700``
   * - Description
     - Return the logs for all the events from the Flow *WordCountFlow* of the *WordCount*
       Application in the namespace *default*,
       beginning ``Thu, 24 Oct 2013 01:00:00 GMT`` and
       ending ``Thu, 24 Oct 2013 01:05:00 GMT`` (five minutes later)


Downloading System Logs
-----------------------
Logs emitted by a system service running in CDAP can be downloaded with the Logging HTTP
API. To do that, send an HTTP GET request::

  GET <base-url>/namespaces/<namespace-id>/system/<component-id>/<service-id>/logs?start=<ts>&stop=<ts>

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Parameter
     - Description
   * - ``<namespace-id>``
     - Namespace ID
   * - ``<component-id>``
     - ``appfabric``
   * - ``<service-id>``
     - [to be completed]
   * - ``<ts>``
     - *Start* and *stop* times, given as seconds since the start of the Epoch.
     

Formatting
----------
The output is formatted as HTML-embeddable text; that is, characters that have a special meaning in HTML will be
escaped. A line of a log may look like this::

  2013-10-23 18:03:09,793 - INFO [FlowletProcessDriver-source-0-
        executor:c.c.e.c.StreamSource@-1] – source: Emitting line: this is an &amp; character

**Note:** The context of the log line shows the name of the Flowlet (*source*), its instance number (0) as
well as the original line in the Application code. The character *&* is escaped as ``&amp;``; if you don’t desire
this escaping, you can turn it off by adding the parameter ``&escape=false`` to the request URL.

Filtering
---------
A filter string can be supplied as an optional parameter. It will filter the returned log entries
to those that match the supplied string.




