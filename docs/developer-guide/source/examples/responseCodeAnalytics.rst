.. :Author: Continuuity, Inc.
   :Description: Continuuity Reactor Apache Log Event Logger

=============================
ResponseCodeAnalytics Example
=============================

**A Continuuity Reactor Application Demonstrating Streams, Flows, Datasets and Procedures**

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

Overview
========
This example demonstrates a simple application for real-time streaming log analysis—computing 
the number of occurrences of each HTTP status code by processing Apache access log data. 

The example introduces the basic constructs of the Continuuity Reactor programming paradigm:
Applications, Streams, Flows, Procedures and Datasets.

In any real-time application, there are four distinct areas:

#. Data Collection: how to get the relevant signals needed by the real-time application for processing
#. Data Processing: the application logic to process the signals into meaningful analysis
#. Data Storage: saving the computed analysis in an appropriate data structure
#. Data Queries: serving the computed data to any down-stream application that wants to use the data

The ``ResponseCodeAnalyticsApp`` Application demonstrates using the abstractions of the Continuuity Reactor to cover these four areas.

Let's look at each one in turn.

The ResponseCodeAnalytics Application
-------------------------------------
All of the components (Streams, Flows, Datasets, and Procedures) of the Application are tied together 
as a deployable entity by the class ``ResponseCodeAnalyticsApp``,
an implementation of ``com.continuuity.api.Application``.

::

  public class ResponseCodeAnalyticsApp extends AbstractApplication {
      
    @Override
    public void configure() {
      setName("ResponseCodeAnalytics");
      setDescription("HTTP response code analytics");
      
      // Ingest data into the app via Streams
      addStream(new Stream("logEventStream"));
      
      // Store processed data in DataSets
      createDataSet("statusCodeTable", Table.class);
      
      // Process log events in real-time using Flows
      addFlow(new LogAnalyticsFlow());
      
      // Query the processed data using Procedures
      addProcedure(new StatusCodeProcedure());
    }
    // ...

Notice that in coding the Application, *Streams* and *Datasets* are defined
using Continuuity classes, and are referenced by names, 
while *Flows*, *Flowlets* and *Procedures* are defined using user-written classes
that implement Continuuity classes and are referenced by passing an object, 
in addition to being assigned a unique name.

Names used for *Streams* and *Datasets* need to be unique across the Reactor instance,
while names used for *Flows*, *Flowlets* and *Procedures* need to be unique only to the Application.

Streams for Data Collection
-------------------------------
Streams are the primary means for bringing data from external systems into the Continuuity Reactor in real-time.

Data can be written to streams using REST. In this example, a Stream named *logEventStream* is used to ingest Apache access logs.

The Stream is configured to ingest data using the Apache Common Log Format. Here is a sample event from a log::

	165.225.156.91 - - [09/Jan/2014:21:28:53 -0400] "GET /index.html HTTP/1.1" 200 225 "http://continuuity.com" "Mozilla/4.08 [en] (Win98; I ;Nav)"

If data is unavailable, a hyphen ("-") is used. Reading from left to right, this format contains:

- the source IP address
- the client's identity
- the remote userid (if using HTTP authentication)
- the date, time, and time zone of the request
- the actual content of the request
- the server's status code to the request
- the size of the data block returned to the client, in bytes

If the log is in Combined Log Format (as is the above example), two additional fields will be present:

- the referrer HTTP request header
- the user-agent HTTP request header

Flows and Flowlets for Real-time Data Processing
------------------------------------------------
Data ingested through Streams can be processed in real-time using Flows, which are user-implemented realtime-stream processors. 

A Flow is comprised of one or more Flowlets that are wired together as a Directed Acyclic Graph (DAG). 
Each Flowlet is able to perform custom logic and execute data operations for each individual data object processed. 

In the example, two Flowlets are used to process the data:

- *parser*: parses the Apache access log entries coming into the *logEventStream* Stream
	- implemented by ``LogEventParseFlowlet``
- *counter*: aggregates the HTTP status codes from the Apache access log entries
	- implemented by ``LogCountFlowlet``

The *parser* and *counter* Flowlets are wired together by the Flow implementation class ``LogAnalyticsFlow``.

Datasets for Data Storage
-------------------------
The processed data is stored in a Table Dataset named *statusCodeTable*. 
The computed analysis—a count of each HTTP status code—is stored on a row named *status*,
with the HTTP status code as the column key and the count as the column value.

Procedures for Real-time Queries
--------------------------------
The data in Datasets can be served using Procedures for real-time querying of the aggregated results.
The ``ResponseCodeAnalyticsApp`` has a Procedure to retrieve all status codes and counts.

Building and Running the Application and Example
================================================
In this remainder of this document, we refer to the Continuuity Reactor runtime as "Reactor", and the
example code that is running on it as an "Application".

We show the Windows prompt as ``~SDK>`` to indicate a command prompt opened in the SDK directory.

In this example, you can either build the Application from source or deploy the already-compiled JAR file.
In either case, you then start a Continuuity Reactor, deploy the Application, and then run the example by
injecting Apache access log entries from an example file into the Application. 

As you do so, you can query the Application to see the results
of its processing the log entries.

When finished, stop the Application as described below.

Building the ResponseCodeAnalyticsApp
-------------------------------------
From the project root, build ``ResponseCodeAnalyticsApp`` with the following `Apache Maven <http://maven.apache.org>`_ command::

	$ mvn clean package

Deploying and Starting the Application
--------------------------------------
Make sure an instance of the Continuuity Reactor is running and available. 
From within the SDK root directory, this command will start Reactor in local mode::

	$ bin/continuuity-reactor start

On Windows::

	~SDK> bin\reactor.bat start

From within the Continuuity Reactor Dashboard (`http://localhost:9999/ <http://localhost:9999/>`_ in local mode):

#. Drag and drop the Application JAR file (``target/ResponseCodeAnalytics-...jar``)
   onto your browser window.
   Alternatively, use the *Load App* button found on the *Overview* of the Reactor Dashboard.
#. Once loaded, select ``ResponseCodeAnalytics`` Application from the list.
   On the Application's detail page, click the *Start* button on **both** the *Process* and *Query* lists.
	
Command line tools are also available to deploy and manage apps. From within the project root:

#. To deploy the Application JAR file, run ``$ bin/app-manager.sh --action deploy [--host <hostname>]``
#. To start the Application, run ``$ bin/app-manager.sh --action start [--host <hostname>]``

:Note:	[--host <hostname>] is not available for a *Local Reactor*.

On Windows:

#. To deploy the App JAR file, run ``~SDK> bin\app-manager.bat deploy`` or drag and drop the
   Application .JAR file (``target/ResponseCodeAnalytics-<version>.jar`` onto your browser window.
   (Currently, the *Load App* button does not work under Windows.)
#. To start the App, run ``~SDK> bin\app-manager.bat start``

Running the Example
-------------------

Injecting Apache Access Log Entries into the Application
........................................................

Running this script will inject Apache access log entries 
from the log file ``/resources/apache.accesslog``
to a Stream named *logEventStream* in the ``ResponseCodeAnalyticsApp``::

	$ bin/inject-data.sh [--host <hostname>]

:Note:	[--host <hostname>] is not available for a *Local Reactor*.

On Windows::

	~SDK> bin\inject-data.bat

Query
.....

If the Procedure has not already been started, you start it either through the 
Continuuity Reactor Dashboard or via an HTTP request using the ``curl`` command::

	curl -v -X POST 'http://localhost:10000/v2/apps/ResponseCodeAnalytics/procedures/StatusCodeProcedure/start'

There are two ways to query the *statusCodeTable* DataSet:

#. Send a query via an HTTP request using the ``curl`` command. For example::

	curl -v -X POST 'http://localhost:10000/v2/apps/ResponseCodeAnalytics/procedures/StatusCodeProcedure/methods/getCounts'

  On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the example::

	libexec\curl...

#. Type a Procedure method name, in this case ``getCounts``, in the *Query* page of the Reactor Dashboard:

   In the Continuuity Reactor Dashboard:

   #. Click the *Query* button.
   #. Click on the *StatusCodeProcedure* Procedure.
   #. Type ``getCounts`` in the *Method* text box.
   #. Click the *Execute* button.
   #. The results of the occurrences for each HTTP status code are displayed in the Dashboard
      in JSON format. For example::

	{"200":21, "301":1,"404":19}

Stopping the Application
------------------------
Either:

- On the Application detail page of the Reactor Dashboard, click the *Stop* button on **both** the *Process* and *Query* lists; or
- Run ``$ bin/app-manager.sh --action stop [--host <hostname>]``

  :Note:	[--host <hostname>] is not available for a *Local Reactor*.

  On Windows, run ``~SDK> bin\app-manager.bat stop``


Downloading the Example
=======================
This example (and more!) is included with our `software development kit <http://continuuity.com/download>`__.
