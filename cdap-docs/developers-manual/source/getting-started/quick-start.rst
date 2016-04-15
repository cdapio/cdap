.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _quick-start:

===========
Quick Start
===========

These instructions will take you from downloading the CDAP SDK through the running of an application.

Download, Install and Start CDAP
================================
If you haven't already, download, install and start CDAP 
:doc:`following these instructions. <standalone/index>`

Your First CDAP Application
===========================
CDAP is a platform for developing and running big data applications. To get started, we
have a collection of pre-built applications available that will allow you to quickly
experience the power of CDAP.

In this example, we will use one of these applications to complete a very common Big Data
use case: web log analytics.

This `web log analytics application <https://github.com/caskdata/cdap-apps/tree/develop/Wise>`__, 
or *WISE*, will show you how CDAP can aggregate logs, perform real-time and batch
analytics of the logs ingested, and expose the results using multiple interfaces. 

Specifically, this application processes web server access logs, counts page-views by IP
in real time, and computes the bounce ratio of each web page encountered in batch. (The
bounce rate is the percentage of views that are not followed by another view on the same
site.)

Conventions
-----------

In the examples and commands that follow, for brevity we will use these conventions:

- ``<CDAP-SDK-HOME>`` is the directory that you have installed the CDAP Standalone SDK, either
  on a UNIX-type system or Windows.
  
- The `CDAP Command Line Interface (CLI) <http://docs.cask.co/cdap/current/en/reference-manual/cli-api.html>`__
  is included in the SDK in the ``bin`` directory, either at ``bin/cdap-cli.sh`` or |---|
  on Windows |---| ``bin\cdap-cli.bat``. The CLI allows you to quickly access CDAP
  facilities from a command line environment.
  
- For brevity in the commands given below, we will simply use ``cdap-cli.sh`` for the CDAP
  Command Line Interface. Substitute the actual path of ``./<CDAP-SDK-HOME>/bin/cdap-cli.sh``,
  or ``<CDAP-SDK-HOME>\bin\cdap-cli.bat`` on Windows, as appropriate. 

- A Windows-version of the application ``curl`` is included in the CDAP SDK as
  ``libexec\bin\curl.exe``; use it as a substitute for ``curl`` in examples.

- If you add the SDK bin directory to your path, you can simplify the commands. From within
  the CDAP-SDK-home directory, enter:
  
  .. tabbed-parsed-literal::
  
    .. Linux

    $ export PATH=${PATH}:\`pwd\`/bin

    .. Windows

    > set path=%PATH%;%CD%\bin;%CD%\libexec\bin
  
  The Windows path has been augmented with a directory where the SDK includes
  Windows-versions of commands such as ``curl``.
  
.. include:: ../_includes/windows-note.txt

.. highlight:: console

Downloading the Artifact
========================
An artifact is a JAR file that contains Application code. You can either download the
application zip archive that we have built for you, or you can pull the source code from
GitHub. If you download the zip file, then the artifact is already built and packaged:

.. tabbed-parsed-literal::

  .. Linux

  $ cd <CDAP-SDK-HOME>
  $ curl -w"\n" -O -X GET "http://repository.cask.co/downloads/co/cask/cdap/apps/|cdap-apps-version|/cdap-wise-|cdap-apps-version|.zip"
  $ unzip cdap-wise-|cdap-apps-version|.zip

  .. Windows
  
  > cd <CDAP-SDK-HOME>
  > curl -O -X GET "http://repository.cask.co/downloads/co/cask/cdap/apps/|cdap-apps-version|/cdap-wise-|cdap-apps-version|.zip"
  > jar xf cdap-wise-|cdap-apps-version|.zip

            
If instead you would like to clone the source code from GitHub, you can build and package
the artifact with these commands:

.. tabbed-parsed-literal::

  $ git clone https://github.com/caskdata/cdap-apps --branch |cdap-apps-compatibile-version|
  $ cd cdap-apps/Wise
  $ mvn clean package -DskipTests
  $ cd <CDAP-SDK-HOME>


In both cases (downloading the artifact or building from source), the artifact is in a
``target/`` directory with the file name:

.. container:: highlight

  .. parsed-literal::
    cdap-wise-|cdap-apps-version|.jar

**Learn More:** A detailed description of the application and its implementation is
available in the :ref:`Web Analytics Application <examples-web-analytics>` documentation.


Deploying the Application
=========================
You can load the artifact into your running instance of CDAP either by using the
:ref:`CDAP Command Line Interface <reference:cli>`:

.. tabbed-parsed-literal::

    $ cdap-cli.sh load artifact cdap-wise-|cdap-apps-version|/target/cdap-wise-|cdap-apps-version|.jar 

    Successfully added artifact with name 'cdap-wise'

or using ``curl`` to directly make an HTTP request:

.. tabbed-parsed-literal::

    $ curl -w"\n" localhost:10000/v3/namespaces/default/artifacts/cdap-wise \
    --data-binary @cdap-wise-|cdap-apps-version|/target/cdap-wise-|cdap-apps-version|.jar
    
    Artifact added successfully

If you cloned the source code and built the app, you'll need to adjust the above paths to
include the ``cdap-apps/Wise`` directory, such as:

.. tabbed-parsed-literal::

    $ cdap-cli.sh load artifact cdap-apps/Wise/target/cdap-wise-|cdap-apps-version|.jar 

    or

    $ curl -w"\n" localhost:10000/v3/namespaces/default/artifacts/cdap-wise \
    --data-binary @cdap-apps/Wise/target/cdap-wise-|cdap-apps-version|.jar


Once the artifact has been added to CDAP, you can create an application using that artifact.
You can create the application by using the CLI:

.. tabbed-parsed-literal::

    $ cdap-cli.sh create app Wise cdap-wise |cdap-apps-version| user

    Successfully created application

or by using ``curl``:

.. tabbed-parsed-literal::

    $ curl -w"\n" -X PUT -H "Content-Type: application/json" localhost:10000/v3/namespaces/default/apps/Wise \
    -d '{ "artifact":{ "name": "cdap-wise", "version": "|cdap-apps-version|", "scope": "user" } }'

    Deploy Complete
    
**Learn More:** *You can also deploy artifacts and apps in one step by dragging and dropping their JARs on*
:ref:`the CDAP UI <cdap-ui>`.

Starting Real-time Processing
=============================
Now that the application is deployed, we can start the real-time processing:

.. tabbed-parsed-literal::

  $ cdap-cli.sh start flow Wise.WiseFlow
  
  Successfully started Flow 'WiseFlow' of application 'Wise' with stored runtime arguments '{}'

This starts the flow named *WiseFlow,* which listens for log events from web servers to
analyze them in real time. Another way to start the flow is using ``curl``:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "localhost:10000/v3/namespaces/default/apps/Wise/flows/WiseFlow/start"

At any time, you can find out whether the flow is running:

.. tabbed-parsed-literal::

  $ cdap-cli.sh get flow status Wise.WiseFlow
  
  RUNNING
  
  $ curl -w"\n" "localhost:10000/v3/namespaces/default/apps/Wise/flows/WiseFlow/status"
  
  {"status":"RUNNING"}


Injecting Data 
==============
The *WiseFlow* uses a stream to receive log events from Web servers. The stream has a REST
endpoint used to ingest data with HTTP requests, and you can do that using the
Command Line Interface:

.. tabbed-parsed-literal::

  .. Linux

  $ cdap-cli.sh send stream logEventStream \''255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] '\
  '"GET /cdap.html HTTP/1.0" 401 2969 " " "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)"'\'
  
  .. Windows

  > cdap-cli.bat send stream logEventStream ^
  \"255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] 'GET /cdap.html HTTP/1.0' 401 2969 ' ' 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'\" 

Or, you can use ``curl`` and an HTTP request:

.. tabbed-parsed-literal::

  .. Linux

  $ curl -w"\n" -X POST "localhost:10000/v3/namespaces/default/streams/logEventStream" \
    -d "255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] 'GET /cdap.html HTTP/1.0' \ 
    401 2969 ' ' 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'"
    
  .. Windows
    
  > curl -X POST "localhost:10000/v3/namespaces/default/streams/logEventStream" ^
  -d "255.255.255.200 - - [23\Sep\2014:11:45:38 -0400] 'GET \cdap.html HTTP\1.0' 2969 ' ' 'Mozilla\4.0 (compatible; MSIE 7.0; Windows NT 5.1)'"


Because it is tedious to send events manually (not to mention difficult to correctly quote
a multi-line command), a file with sample web log events is included in the WISE
application source. The CDAP CLI can read it line-by-line and submit them as events
to the stream. Use the CLI to send the events to the stream:

.. tabbed-parsed-literal::

    $ cdap-cli.sh load stream logEventStream cdap-wise-|cdap-apps-version|/resources/apache.accesslog text/plain
    
If you have downloaded the source code instead of the artifact, the file will be a different location:

.. tabbed-parsed-literal::

    $ cdap-cli.sh load stream logEventStream cdap-apps/Wise/resources/apache.accesslog text/plain


This will run for a number of seconds until all events are inserted.


Inspecting the Injected Data 
============================
Now that you have data in the stream, you can verify it by reading the events back. Each
event is tagged with a timestamp of when it was received by CDAP. (Note: this is not the
same time as the date included in each event—that is the time when the event actually
occurred on the web server.) 

You can retrieve events from a stream by specifying a time range and a limit on the number
of events you want to see. For example, using the Command Line Interface, this shows up to 5 events
in a time range of 3 minutes duration, starting 5 minutes ago:

.. tabbed-parsed-literal::

  $ cdap-cli.sh get stream logEventStream -5m +3m 5
  
Results::

  +========================================================================================================+
  | timestamp     | headers | body size | body                                                             |
  +========================================================================================================+
  | 1412386081819 |         | 140       | 255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] "GET /cdap.html |
  |               |         |           |  HTTP/1.0" 401 2969 " " "Mozilla/4.0 (compatible; MSIE 7.0; Wind |
  |               |         |           | ows NT 5.1)"                                                     |
  |--------------------------------------------------------------------------------------------------------|
  | 1412386081830 |         | 146       | 255.255.255.250 - - [23/Sep/2014:11:45:39 -0400] "POST /quicksta |
  |               |         |           | rt.html HTTP/1.1" 401 837 " " "Googlebot/2.1 ( http://www.google |
  |               |         |           | bot.com/bot.html)"                                               |
  |--------------------------------------------------------------------------------------------------------|
  | 1412386081841 |         | 141       | 255.255.255.158 - - [23/Sep/2014:11:45:40 -0400] "GET /index.htm |
  |               |         |           | l HTTP/1.0" 200 2565 " " "Googlebot/2.1 ( http://www.googlebot.c |
  |               |         |           | om/bot.html)"                                                    |
  |--------------------------------------------------------------------------------------------------------|
  | 1412386081851 |         | 139       | 255.255.255.211 - - [23/Sep/2014:11:45:41 -0400] "GET /cdap.html |
  |               |         |           |  HTTP/1.0" 200 135 " " "Googlebot/2.1 ( http://www.googlebot.com |
  |               |         |           | /bot.html)"                                                      |
  |--------------------------------------------------------------------------------------------------------|
  | 1412386081862 |         | 141       | 255.255.255.135 - - [23/Sep/2014:11:45:44 -0400] "POST /cdap.htm |
  |               |         |           | l HTTP/1.0" 401 3939 " " "Googlebot/2.1 ( http://www.googlebot.c |
  |               |         |           | om/bot.html)"                                                    |
  +========================================================================================================+
  Fetched 5 events from stream logEventStream
  
Note: you may have to adjust the time range according to when you injected the
events into the stream. The longer after you inject the events, the farther back in time
you will need to go to find the events:

.. tabbed-parsed-literal::

  $ cdap-cli.sh get stream logEventStream -60m +3m 5

The same query can be made using curl with an HTTP request. However, you'll need to adjust the
start and end of the time range to milliseconds since the start of the Epoch:

.. tabbed-parsed-literal::

  $ curl -w"\n" "localhost:10000/v3/namespaces/default/streams/logEventStream/events?start=1412386081819\&end=1412386081870\&limit=5"
  
The current time in seconds since the start of the Epoch can be found with:

.. tabbed-parsed-literal::

  .. Linux

  $ date +%s
  
  .. Windows
  
  > echo.WScript.Echo(DateDiff("s", "01/01/1970 00:00:00", Now())) > t.vbs & cscript /nologo t.vbs & del t.vbs

Note that it is important to escape the ampersands in the URL of the ``curl`` command to
prevent the shell from interpreting it as a special character. The RESTful API will return
the events in a JSON format; there are a `variety of tools available
<https://www.google.com/search?q=json+pretty+print>`__ to pretty-print it on the
Command Line.


Monitoring with the CDAP UI
===========================
You may recall that before we started injecting data into the stream, we started the
*WiseFlow* to process these events in real time. You can observe the flow while it is
processing events by retrieving metrics about how many events it has processed. For that,
we need to know the name of the flowlet inside the *WiseFlow* that performs the actual
processing. 

In this case, it is a flowlet named *parser*. Here is a ``curl`` command to retreive the
number of events it has processed (the endTime and the value returned will vary, depending 
on when and how many events you have sent):

.. tabbed-parsed-literal::

  .. Linux

  $ curl -w"\n" -X POST "localhost:10000/v3/metrics/query?"\
  "context=namespace.default.app.Wise.flow.WiseFlow.flowlet.parser"\
  "&metric=system.process.events.processed&aggregate=true"
  
  .. Windows
  
  > curl -X POST "localhost:10000/v3/metrics/query?context=namespace.default.app.Wise.flow.WiseFlow.flowlet.parser&metric=system.process.events.processed&aggregate=true"
  
  {"startTime":0,"endTime":1431467057,"series":[{"metricName":"system.process.events.processed","grouping":{},"data":[{"time":0,"value":3007}]}]}

A much easier way to observe the flow is in the `CDAP UI: <http://localhost:9999>`__
it shows a `visualization of the flow, <http://localhost:9999/ns/default/apps/Wise/programs/flows/WiseFlow/runs>`__
annotated with its real-time metrics:

.. image:: ../_images/quickstart/wise-flow1.png
   :width: 600px

In this screenshot, we see that the stream has about thirty thousand events and all of them
have been processed by both flowlets. You can watch these metrics update in real time by
repeating the injection of events into the stream:

.. tabbed-parsed-literal::

    $ cdap-cli.sh load stream logEventStream cdap-wise-|cdap-apps-version|/resources/apache.accesslog text/plain

    or

    $ cdap-cli.sh load stream logEventStream cdap-apps/Wise/resources/apache.accesslog text/plain
  
If you click on the right-most flowlet (*pageViewCount*) you see the current number of
events being processed by each flowlet, in this case up to about 60 events per second:

.. image:: ../_images/quickstart/wise-flow2.png
   :width: 600px


Retrieving the Results of Processing 
====================================
The flow counts URL requests by the origin IP address, using a dataset called
*pageViewStore*. To make these counts available, the application implements a service called
*WiseService*. Before we can use this service, we need to make sure that it is running. We
can start the service using the Command Line Interface:

.. tabbed-parsed-literal::

  $ cdap-cli.sh start service Wise.WiseService
  
  Successfully started Service 'WiseService' of application 'Wise' with stored runtime arguments '{}'
  
Or, using RESTful calls:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "localhost:10000/v3/namespaces/default/apps/Wise/services/WiseService/start"
  
  $ curl -w"\n" -X GET "localhost:10000/v3/namespaces/default/apps/Wise/services/WiseService/status"
  
  {"status":"RUNNING"}

Now that the service is running, we can query it to find out the current count for a
particular IP address. For example, the data injected by our script contains this line
(reformatted to fit)::

  255.255.255.239 - - [23/Sep/2014:11:46:05 -0400] "POST /home.html HTTP/1.1" 
    401 2620 " " "Opera/9.20 (Windows NT 6.0; U; en)"

To find out the total number of page views from this IP address, we can query the service
using a REST call:

.. tabbed-parsed-literal::

  $ curl -w"\n" "localhost:10000/v3/namespaces/default/apps/Wise/services/WiseService/methods/ip/255.255.255.249/count"
  42

Or, we can find out how many times the URL ``/home.html`` was accessed from the same IP address:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST -d "/home.html" \
  "localhost:10000/v3/namespaces/default/apps/Wise/services/WiseService/methods/ip/255.255.255.249/count"  
  6
  
  $ cdap-cli.sh call service Wise.WiseService POST ip/255.255.255.249/count body "/home.html"
  
  < 200 OK
  < Content-Length: 2
  < Connection: keep-alive
  < Content-Type: application/json
  6  

Note that this is a POST request, because we need to send over the URL of interest.
Because an URL can contain characters that have special meaning within URLs, it is most
convenient to send the URL as the body of a POST request.

We can also use SQL to bypass the service and query the raw contents of the underlying
table (reformatted to fit):

.. tabbed-parsed-literal::

  $ cdap-cli.sh execute "\"SELECT * FROM dataset_pageviewstore WHERE key = '255.255.255.249'\""

Results::

  +============================================================================================+
  | dataset_pageviewstore.key: STRING | dataset_pageviewstore.value: map<string,bigint>        |
  +============================================================================================+
  | 255.255.255.249                   | {"/about.html":2,"/world.html":4,"/index.html":14,     |
  |                                   | "/news.html":4,"/team.html":2,"/cdap.html":4,          |
  |                                   | "/contact.html":2,"/home.html":6,"/developers.html":4} |
  +============================================================================================+

Here we can see that the storage format is one table row per IP address, with a column for
each URL that was requested from that IP address. This is an implementation detail that
the service hides from external clients. However, there are situations where inspecting
the underlying table is useful; for example, when debugging a problem.


Processing in Batch
===================
The WISE application also processes the web log to compute the “bounce count” of each URL.
For this purpose, we consider it a “bounce” if a user views a page but does not view
another page within a time threshold: essentially, that means the user has left the web site. 

Bounces are difficult to detect with a flow. This is because processing in a flow is
triggered by incoming events; a bounce, however, is indicated by the absence of an event:
the same user’s next page view. 

It is much easier to detect bounces with a MapReduce. The WISE application includes a
MapReduce that computes the total number of bounces for each URL. It is part of a workflow
that is scheduled to run every 10 minutes; we can also start the job immediately using the
CLI:

.. tabbed-parsed-literal::

  $ cdap-cli.sh start mapreduce Wise.BounceCountsMapReduce
  
  Successfully started MapReduce program 'BounceCountsMapReduce' of application 'Wise' with stored runtime arguments '{}'
  
or using a RESTful call:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST "localhost:10000/v3/namespaces/default/apps/Wise/mapreduce/BounceCountsMapReduce/start"

Note that this MapReduce program processes the exact same data that is consumed by the
*WiseFlow*, namely, the log event stream, and both programs can run at the same time without
getting in each other’s way. 

We can inquire as to the status of the MapReduce:

.. tabbed-parsed-literal::

  $ curl -w"\n" "localhost:10000/v3/namespaces/default/apps/Wise/mapreduce/BounceCountsMapReduce/status"
  
  {"status":"RUNNING"}

When the job has finished, the returned status will be *STOPPED*. Now we can query the
bounce counts with SQL. Let's take a look at the schema first:

.. tabbed-parsed-literal::

  $ cdap-cli.sh execute "\"DESCRIBE dataset_bouncecountstore\""

Results::

  +==========================================================+
  | col_name: STRING | data_type: STRING | comment: STRING   |
  +==========================================================+
  | uri              | string            | from deserializer |
  | totalvisits      | bigint            | from deserializer |
  | bounces          | bigint            | from deserializer |
  +==========================================================+

For example, to get the five URLs with the highest bounce-to-visit ratio (or bounce rate):

.. tabbed-parsed-literal::

  $ cdap-cli.sh execute "\"SELECT uri, bounces/totalvisits AS ratio \
  FROM dataset_bouncecountstore ORDER BY ratio DESC LIMIT 5\""
  
Results::

  +===================================+
  | uri: STRING | ratio: DOUBLE       |
  +===================================+
  | /cdap.html  | 0.18867924528301888 |
  | /world.html | 0.1875              |
  | /news.html  | 0.18545454545454546 |
  | /team.html  | 0.18181818181818182 |
  | /intro.html | 0.18072289156626506 |
  +===================================+

Apparently, the ``/cdap.html`` has the highest bounce rate of all the URLs. 

We can also use the full power of the `Hive query language
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual>`__ in formulating our
queries. For example, Hive allows us to explode the page view counts into a table with
fixed columns:

.. tabbed-parsed-literal::

  $ cdap-cli.sh execute "\"SELECT key AS ip, uri, count FROM dataset_pageviewstore \
  LATERAL VIEW explode(value) t AS uri,count ORDER BY count DESC LIMIT 10\""

Results::
  
  +====================================================+
  | ip: STRING      | uri: STRING      | count: BIGINT |
  +====================================================+
  | 255.255.255.113 | /home.html       | 9             |
  | 255.255.255.131 | /home.html       | 9             |
  | 255.255.255.246 | /quickstart.html | 8             |
  | 255.255.255.153 | /quickstart.html | 8             |
  | 255.255.255.236 | /quickstart.html | 8             |
  | 255.255.255.181 | /index.html      | 8             |
  | 255.255.255.198 | /index.html      | 7             |
  | 255.255.255.249 | /index.html      | 7             |
  | 255.255.255.194 | /cdap.html       | 7             |
  | 255.255.255.180 | /index.html      | 7             |
  +====================================================+

We can even join two datasets: the one produced by the real-time flow; and the other one
produced by the MapReduce. The query below returns, for each of the three URLs with the
highest bounce ratio, the IP addresses that have made more than three requests for that
URL. In other words: who are the users who are most interested in the least interesting
pages?

.. tabbed-parsed-literal::

  .. Linux

  $ cdap-cli.sh execute "\"SELECT views.uri, ratio, ip, count FROM \
       (SELECT uri, totalvisits/bounces AS ratio \
          FROM dataset_bouncecountstore ORDER BY ratio DESC LIMIT 3) bounce, \
       (SELECT key AS ip, uri, count \
          FROM dataset_pageviewstore LATERAL VIEW explode(value) t AS uri,count) views \
    WHERE views.uri = bounce.uri AND views.count >= 3\""
    
  .. Windows
  
  > cdap-cli.bat execute \"SELECT views.uri, ratio, ip, count FROM (SELECT uri, totalvisits/bounces AS ratio FROM dataset_bouncecountstore ORDER BY ratio DESC LIMIT 3) bounce, (SELECT key AS ip, uri, count FROM dataset_pageviewstore LATERAL VIEW explode(value) t AS uri,count) views WHERE views.uri = bounce.uri AND views.count >= 3\"
    
Results::

  +=========================================================================+
  | views.uri: STRING | ratio: DOUBLE     | ip: STRING      | count: BIGINT |
  +=========================================================================+
  | /contact.html     | 8.666666666666666 | 255.255.255.166 | 3             |
  | /contact.html     | 8.666666666666666 | 255.255.255.199 | 3             |
  | /contact.html     | 8.666666666666666 | 255.255.255.216 | 3             |
  | /about.html       | 7.333333333333333 | 255.255.255.227 | 3             |
  | /home.html        | 6.551724137931035 | 255.255.255.105 | 3             |
  | /home.html        | 6.551724137931035 | 255.255.255.106 | 6             |
  | /home.html        | 6.551724137931035 | 255.255.255.107 | 4             |
  | /home.html        | 6.551724137931035 | 255.255.255.111 | 5             |
  | /home.html        | 6.551724137931035 | 255.255.255.112 | 5             |
  | /home.html        | 6.551724137931035 | 255.255.255.113 | 9             |
  | /home.html        | 6.551724137931035 | 255.255.255.114 | 5             |
  | /home.html        | 6.551724137931035 | 255.255.255.115 | 4             |
  | /home.html        | 6.551724137931035 | 255.255.255.117 | 4             |
  | /home.html        | 6.551724137931035 | 255.255.255.118 | 3             |
  | /home.html        | 6.551724137931035 | 255.255.255.120 | 3             |
  | /home.html        | 6.551724137931035 | 255.255.255.123 | 5             |
  | /home.html        | 6.551724137931035 | 255.255.255.124 | 5             |
  | /home.html        | 6.551724137931035 | 255.255.255.126 | 5             |
  | /home.html        | 6.551724137931035 | 255.255.255.127 | 4             |
  | /home.html        | 6.551724137931035 | 255.255.255.129 | 4             |
  +=========================================================================+

Conclusion
==========
Congratulations! You've just successfully run your first Big Data log analytics application on CDAP. 

You can deploy the same application on a real cluster and experience the power of CDAP.

Additional :ref:`examples <examples-index>`, :ref:`guides <guides-index>`, and
:ref:`tutorials <tutorials>` on building CDAP applications :ref:`are available <examples-introduction-index>`. 

As a next step, we recommend reviewing all of these :ref:`training materials <examples-introduction-index>`
as being the easiest way to become familiar and proficient with CDAP.

If you want to begin writing your own application, continue with the instructions on the 
:ref:`Getting Started <getting-started-index>` page.
