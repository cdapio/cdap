.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform SparkPageRank Application
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _examples-log-analysis:

====================
Log Analysis Example
====================

A Cask Data Application Platform (CDAP) example demonstrating Spark and MapReduce
running in parallel inside a Workflow through fork.

Overview
========

This example demonstrates Spark and MapReduce performing log analysis, computing
total number of hits for every unique URL, total number of responses for every
unique response code, and total number of requests made by every unique IP address,
based on Apache usage log.

Logs are sent to CDAP and ingested into the *logStream*, which stores the log
information event in its entirety.

After these events are streamed, they are taken up by the *ResponseCounterSpark*, which
goes through the entries, calculates the total number of responses for every unique
response code, and tabulates results in an ``KeyValueTable`` dataset, *responseCount*.
The Spark program also computes the total number of requests made by every unique IP
address and writes it to ``TimePartitionedFileSet``, *reqCount*.

In parallel, these events are also taken up by the *HitCounterProgram*, which goes
through the entries, calculates the total number hits for every unique URL and
tabulates results in a ``KeyValueTable`` dataset, *hitCount*.

The *LogAnalysisWorkflow* ties the Spark and MapReduce programs to run in parallel.

Once the application completes, you can query the *responseCount* dataset by using
the ``rescount`` endpoint of the *ResponseCounterService*. It will send back a
string result with the total number of responses on the ``rescount`` query parameter.
You can also query the *hitCount* dataset by using the ``url`` endpoint of the
*HitCounterService*. It will send the total number of hits for the queried url.
You can query the ``reqCount`` ``TimePartitionedFileSet`` by using the ``reqcount``
endpoint of the *RequestCounterService* which will return a set of all available
partitions. Using one of partitions from the above set, you can query for the total
number of requests made by every unique IP address in last 60 minutes. The ``reqfile``
endpoint of the *RequestCounterService* returns a map of IP addresses to the total
number of requests made by them.

Let's look at some of these components, and then run the application and see the results.

The LogAnalysis Application
---------------------------

As in the other `examples <index.html>`__, the components
of the application are tied together by the class ``LogAnalysisApp``:

.. literalinclude:: /../../../cdap-examples/LogAnalysis/src/main/java/co/cask/cdap/examples/loganalysis/LogAnalysisApp.java
   :language: java
   :lines: 60-94

The *hitCount* and *responseCount* ``KeyValueTables`` and *reqCount* ``TimePartitionedFileSet``
-----------------------------------------------------------------------------------------------

The calculated hit count for every unique URL is stored in a ``KeyValueTable`` dataset,
*hitCount* and the total number of responses for a response code is stored in another
``KeyValueTable`` dataset, *responseCount*. The total number of requests made by every
unique IP address is written to a ``TimePartitionedFileSet``, *ipCount*.

The ``HitCounterService``, ``ResponseCounterService`` and ``RequestCounterService``
-----------------------------------------------------------------------------------

These services provide convenient endpoints:

- ``HitCounterService:`` ``hitcount`` endpoint to obtain the total number of hits for a given URL;
- ``ResponseCounterService:`` ``rescount`` endpoint to obtain the total number of responses for a given response code;
- ``RequestCounterService:`` ``reqcount`` endpoint to obtain a set of all the available partitions in the TimePartitionedFileSet; and
- ``RequestCounterService:`` ``reqfile`` endpoint to retrieve data from a particular partition.


.. |example| replace:: LogAnalysisApp
.. include:: building-starting-running-cdap.txt


Running the Example
===================

Starting the Services
---------------------

Once the application is deployed:

- Go to the *LogAnalysisApp* `application overview page
  <http://localhost:9999/ns/default/apps/LogAnalysisApp/overview/status>`__,
  click *ResponseCounterService* to get to the service detail page, then click the *Start* button,
  and then do the same for the *HitCounterService* and *RequestCounterService* services; or
- From the Standalone CDAP SDK directory, use the CDAP Command Line Interface::

    $ cdap-cli.sh start service LogAnalysisApp.ResponseCounterService
    $ cdap-cli.sh start service LogAnalysisApp.HitCounterService
    $ cdap-cli.sh start service LogAnalysisApp.RequestCounterService
    
    Successfully started service 'ResponseCounterService' of application 'LogAnalysisApp' with stored runtime arguments '{}'
    Successfully started service 'HitCounterService' of application 'LogAnalysisApp' with stored runtime arguments '{}'
    Successfully started service 'RequestCounterService' of application 'LogAnalysisApp' with stored runtime arguments '{}'

Injecting Access Logs
---------------------

Inject a file of Apache access log to the stream *logStream* by running this command from the
Standalone CDAP SDK directory, using the Command Line Interface::
  
  $ cdap-cli.sh load stream logStream cdap-examples/LogAnalysis/resources/apache.accesslog "text/plain"
  Successfully sent stream event to stream 'logStream'

Running the Workflow
--------------------
There are three ways to start the workflow:

1. Go to the *LogAnalysisApp* `application overview page
   <http://localhost:9999/ns/default/apps/LogAnalysisApp/overview/status>`__,
   click ``LogAnalysisWorkflow`` to get to the Workflow detail page, then click the *Start* button; or
   
#. Send a query via an HTTP request using the ``curl`` command::

    $ curl -w'\n' -v \
        http://localhost:10000/v3/namespaces/default/apps/LogAnalysisApp/workflows/LogAnalysisWorkflow/start

#. Use the Command Line Interface::

    $ cdap-cli.sh start workflow LogAnalysisApp.LogAnalysisWorkflow

Querying the Results
--------------------

To query the *hitCount* KeyValueTable through the ``HitCounterService``, send a query via an HTTP
request using the ``curl`` command. For example::

  $ curl -w'\n' -X POST -d'{"url":"/index.html"}' http://localhost:10000/v3/namespaces/default/apps/LogAnalysisApp/services/HitCounterService/methods/hitcount

You can also use the Command Line Interface::

  $ cdap-cli.sh call service LogAnalysisApp.HitCounterService POST 'hitcount' body '{"url":"/index.html"}'

On success, this command will return the hit count for the above URL, such as ``4``.

Similarly, to query the *responseCount* ``KeyValueTable`` through the *ResponseCounterService*, the *reqCount*
``TimePartitionedFileSet`` through the *RequestCounterService*, and to retrieve data from a particular partition of
the ``TimePartitionedFileSet``, use:

curl and Command Line Interface::

  $ curl -w'\n' http://localhost:10000/v3/namespaces/default/apps/LogAnalysisApp/services/ResponseCounterService/methods/rescount/200
  $ cdap-cli.sh call service LogAnalysisApp.ResponseCounterService GET 'rescount/200'

On success, this command will return the total number of responses sent with the queried response code, ``30``.

::

  $ curl -w'\n' http://localhost:10000/v3/namespaces/default/apps/LogAnalysisApp/services/RequestCounterService/methods/reqcount
  $ cdap-cli.sh call service LogAnalysisApp.RequestCounterService GET 'reqcount'

On success, this command will return a set of all the available partitions::

  ["7/29/15 7:47 PM"]

|

::

  $ curl -w'\n' -X POST -d'{"time":"7/27/15 5:58 PM"}' http://localhost:10000/v3/namespaces/default/apps/LogAnalysisApp/services/RequestCounterService/methods/reqfile
  $ cdap-cli.sh call service LogAnalysisApp.RequestCounterService POST 'reqfile' body '{"time":"7/27/15 5:58 PM"}'

On success, this above command will return a map of all the unique IP addresses with number of request made by them::

  {"255.255.255.109":1,255.255.255.121":1,"255.255.255.211":1}

Stopping and Removing the Application
=====================================
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the steps:

**Stopping the Workflow**

- Go to the *LogAnalysisApp* `application overview page
  <http://localhost:9999/ns/default/apps/LogAnalysisApp/overview/status>`__,
  click *LogAnalysisWorkflow* to get to the workflow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop workflow LogAnalysisApp.LogAnalysisWorkflow

**Stopping the Services**

- Go to the *LogAnalysisApp* `application overview page
  <http://localhost:9999/ns/default/apps/LogAnalysisApp/overview/status>`__,
  click ``ResponseCounterService`` to get to the service detail page, then click the *Stop* button,
  doing the same for the ``HitCounterService`` and ``RequestCounterService`` services; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop service LogAnalysisApp.ResponseCounterService
    $ cdap-cli.sh stop service LogAnalysisApp.HitCounterService
    $ cdap-cli.sh stop service LogAnalysisApp.RequestCounterService

**Removing the Application**

You can now remove the application as described above, `Removing an Application <#removing-an-application>`__, or:

- Go to the *LogAnalysisApp* `application overview page
  <http://localhost:9999/ns/default/apps/LogAnalysisApp/overview/status>`__,
  click the *Actions* menu on the right side and select *Manage* to go to the Management pane for the application,
  then click the *Actions* menu on the right side and select *Delete* to delete the application; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh delete app LogAnalysisApp
