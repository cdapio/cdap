.. :author: Cask Data, Inc.
   :description: Index document
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Quick Start
============================================

Your First CDAP Application
===========================
CDAP is a platform for developing and running big data applications. To get started, we
have a collection of pre-built applications available that will allow you to quickly
experience the power of CDAP.

In this example, we will be using a very common big data use case: log analytics.

This log analytics application will show you how CDAP can aggregate logs, perform
real-time and batch analytics of the logs ingested, and expose the results using multiple
interfaces. Specifically, this application processes web server access logs, counts
page-views by IP in real-time, and computes the bounce ratio of each web page encountered
in batch.

Downloading the Application
===========================
You can either choose to download the application archive that we have built for you, or
you can choose to pull it from github. If you download the zip file, then the application
is already built and packaged. 

::

  $ wget http://repository.cask.co/downloads/co/cask/cdap/apps/0.2.0/cdap-wise-0.2.0.zip
  $ unzip cdap-wise-0.2.0.zip
  $ cd cdap-wise-0.2.0

If you clone from GitHub, you will first need to build and package the application with
these commands::

  $ git clone https://github.com/caskdata/cdap-apps
  $ cd cdap-apps/Wise
  $ mvn package -DskipTests

In both cases, the packaged application is in the ``target/`` directory and the file name is
``Wise-0.2.0.jar``.

Learn more about the details and implementation of this application here.


Deploying the Application
=========================
You can deploy the application into a running instance of CDAP either using the
command-line tool::

  $ cdap-cli.sh deploy app target/Wise-0.2.0.jar

or using curl to directly make an HTTP request::

  $ curl -v -H "X-Archive-Name: Wise-0.2.0.jar" -X POST localhost:10000/v2/apps \
       --data-binary @target/Wise-0.2.0.jar

Learn More: You can also deploy apps using the CDAP Console.

Starting Realtime Processing
============================
Now that the application is deployed, we can start the real-time processing::

  $ curl -v -XPOST localhost:10000/v2/apps/Wise/flows/WiseFlow/start

This starts the flow named WiseFlow, which listens for log events from Web servers to
analyze them in realtime. Another way to start the flow is using the command line::

  $ cdap-cli.sh start flow Wise.WiseFlow
  Successfully started Flow 'WiseFlow' of application 'Wise'

At any time, you can find out whether the flow is running::

  $ cdap-cli.sh get status flow Wise.WiseFlow
  RUNNING
  $ curl localhost:10000/v2/apps/Wise/flows/WiseFlow/status
  {"status":"RUNNING"}

Injecting Data 
==============
The WiseFlow uses a Stream to receive log events from Web servers. The Stream has a REST
endpoint to send events to it, and you can do that using an HTTP request::

  $ curl localhost:10000/v2/streams/logEventStream -X POST \
  > -d '255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] \ 
  > "GET /cdap.html HTTP/1.0" 401 2969 " " "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)"'

Or, you can use the command line interface to achieve the same::

  $ cdap-cli.sh send stream logEventStream 
  > '255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] \
  > "GET /cdap.html HTTP/1.0" 401 2969 " " "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)"'

Because it is tedious to send events manually, a file with sample web log events is
included in the Wise application source, along with a script that reads it line-by-line
and submits the events to the stream using REST. Use this script to send events to the
stream::

  $ bin/inject-data.sh

This will run for several seconds until all events are inserted.

Inspecting Injected Data 
========================
Now that you have data in the stream, you can verify it by reading the events back. Each
event is tagged with the timestamp of the time when it was received (Note: this is not the
same time as the date included in each event - that is the time when the event actually
occurred on the web server). You can retrieve events from a stream by specifying a time
range and a limit on the number of events you want to see. For example, using the command
line, this shows up to 5 events in the time range of 3 minutes duration, starting 5
minutes ago::

  $ cdap-cli.sh get stream logEventStream -5m +3m 5
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

Note: you may have to adjust the time range according to how long ago you injected the
events into the stream. 

The same can be done using curl to make an HTTP request. However, you need to specify the
start and end of the time range as milliseconds since the Epoch::

  $ curl localhost:10000/v2/streams/logEventStream/events?start=1412385622228\&end=1412386222228\&limit=5

Note that it is important to escape the ampersands in the URL to prevent the shell from
interpreting it as a special character. Note also that the REST API will return the events
in a JSON format that is not suited for display here; you are welcome to try it out and
parse the output—it is intended for consumption by machines, not humans.


Monitoring with the CDAP Console
================================
Remember that before we started injected data into the stream, we started the WiseFlow to
process these events in real-time. You can observe the flow while it is processing events,
for example, by retrieving metrics about many events it has processed. For that we need to
know the name of the flowlet inside the WiseFlow that performs the actual processing, in
this case it is a flowlet named “pageViewCount”. Here is a curl command to get the number
of events it has processed::

  $ curl localhost:10000/v2/metrics/system/apps/Wise/flows/WiseFlow/flowlets/parser/\
  > process.events.processed\?aggregate=true
  {"data":3000}

A much easier way to observe the flow is in the CDAP Console: it shows a visualization of
the flow annotated with its realtime metrics:

.. image:: ../_images/quickstart/wise-flow1.png
   :width: 600px

In this screenshot, we see that the stream has about three thousand events and all of them
have been processed by both flowlets. You can see how these metrics update in realtime, by
repeating the injection of events into the stream::

  $ bin/inject-data.sh
  
You can change the type of metrics being displayed using the dropdown on the right. If you
change it to “Flowlet Rate”, you see the current number of events being processed by each
flowlet, in this case about 63 events per second:

.. image:: ../_images/quickstart/wise-flow2.png
   :width: 600px

Refer to [section of CDAP Console docs] for more details on the flow status page. 

Retrieving the Results of Processing 
====================================
The flow counts URL requests by the origin IP address, using a dataset called
pageViewStore. To make these counts available, the application implements a service called
WiseService. Before we can use this service, we need to make sure that it is running. We
can do that using a REST call::

  $ curl -XPOST localhost:10000/v2/apps/Wise/services/WiseService/start

Or, using the command line interface::

  $ cdap-cli.sh start service Wise.WiseService

Now that the service is running, we can query it to find out the current count for a
particular IP address. For example, the data injected by our script contains this line
(reformatted to fit)::

  255.255.255.239 - - [23/Sep/2014:11:46:05 -0400] "POST /home.html HTTP/1.1" 
    401 2620 " " "Opera/9.20 (Windows NT 6.0; U; en)"

To find out the total number of page views from this IP address, we can query the service
using a REST call::

  $ curl localhost:10000/v2/apps/Wise/services/WiseService/methods/ip/255.255.255.249/count
  42

Or, we can find out how many times the URL /home.html was accessed from this IP address::

  $ curl -d “/home.html” localhost:10000/v2/apps/Wise/services/WiseService/methods/ip/255.255.255.249/count
  6

Note that this is a POST request, because we need to send over the URL of interest.
Because that URL contains characters that have special meaning within URLs, it is most
convenient to send it as the body of a POST request.

We can also use SQL to bypass the service and query the raw contents of the underlying
table (reformatted to fit)::

  $ cdap-cli.sh execute select '*' from cdap_user_pageviewstore where key = '"255.255.255.249"'
  +===============================================================================================+
  | cdap_user_pageviewstore.key: STRING | cdap_user_pageviewstore.value: map<string,bigint>       |
  +===============================================================================================+
  | 255.255.255.249                     | {"/about.html":2,"/world.html":4,"/index.html":14,      |
  |                                     |  "/news.html":4,"/team.html":2,"/cdap.html":4,          |
  |                                     |  "/contact.html":2,"/home.html":6,"/developers.html":4} |
  +===============================================================================================+

Here we can see that the storage format is one table row per IP address, with a column for
each URL that was requested from that IP address. This is an implementation detail that
the service hides from external clients. However, there are situations where inspecting
the underlying table is useful, for example, when debugging a problem.


Processing in Batch
===================
The Wise application also processes the web log to compute the “bounce count” of each URL.
For this purpose, we consider it a “bounce” if a user views a page but does not view
another page within a time threshold: That means the user has left the web site. 

Bounces are difficult to detect with a flow. This is because processing in a flow is
triggered by incoming events; a bounce, however, is indicated by the absence of an event
(the same user’s next page view). It is much easier to detect bounces with a MapReduce
job. The Wiuse application includes a MapReduce that computes the total number of bounces
for each URL. It is part of a workflow that is scheduled to run every 10 minutes. We can
also start the job immediately using the CLI::

  $ cdap-cli.sh start mapreduce Wise.WiseWorkflow_BounceCountsMapReduce

or using a REST call::

  $ curl -XPOST localhost:10000/v2/apps/Wise/mapreduce/WiseWorkflow_BounceCountsMapReduce/start

Note that this MapReduce job processes the exact same data that is consumed by the
WiseFlow, namely the log event stream, and both programs can run at the same time without
getting in each other’s way. 

We can inquire the status of this MapReduce job::

  curl localhost:10000/v2/apps/Wise/mapreduce/WiseWorkflow_BounceCountsMapReduce/status
  {"status":"RUNNING"}

When the job has finished, the returned status will be STOPPED. Now we can query the
bounce counts with SQL. Let us take a look at the schema first::

  $ cdap-cli.sh execute "describe cdap_user_bouncecountstore"
  Successfully connected CDAP instance at 127.0.0.1:10000
  +==========================================================+
  | col_name: STRING | data_type: STRING | comment: STRING   |
  +==========================================================+
  | uri              | string            | from deserializer |
  | totalvisits      | bigint            | from deserializer |
  | bounces          | bigint            | from deserializer |
  +==========================================================+

For example, to get the five URLs with the highest visit-to-bounce ratio::

  $ cdap-cli.sh execute SELECT uri, totalvisits/bounces AS ratio \
  >   FROM cdap_user_bouncecountstore ORDER BY ratio DESC LIMIT 5
  +====================================+
  | uri: STRING   | ratio: DOUBLE      |
  +====================================+
  | /contact.html | 8.666666666666666  |
  | /about.html   | 7.333333333333333  |
  | /home.html    | 6.560344827586207  |
  | /map.html     | 6.2727272727272725 |
  | /index.html   | 6.237288135593221  |
  +====================================+

Apparently, the /contact.html has the highest bounce rate of all the URLs. 

We can use the full power of the Hive query language. For example, it allows us to explode
the page view counts into a table with fixed columns::

  $ cdap-cli.sh execute "SELECT key AS ip, uri, count FROM cdap_user_pageviewstore \
      LATERAL VIEW explode(value) t AS uri,count ORDER BY count DESC LIMIT 10"
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

And we can even join the two datasets, one produced by a realtime flow, the other one
produced by a MapReduce job. This query returns, for each of the three URLs with the
highest bounce ratio, the IP addresses that have made more than three requests for that
URL. In other words: Who are the users who are most interested in the least interesting
pages?

::

  $ cdap-cli.sh execute "SELECT views.uri, ratio, ip, count FROM \
  >    (SELECT uri, totalvisits/bounces AS ratio \
          FROM cdap_user_bouncecountstore ORDER BY ratio DESC LIMIT 3) bounce, \
       (SELECT key AS ip, uri, count \
          FROM cdap_user_pageviewstore LATERAL VIEW explode(value) t AS uri,count) views \
    WHERE views.uri = bounce.uri AND views.count >= 3"
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

Summary
=======

Congratulations! You've just successfully run your first big data log analytics application on CDAP. 

You can deploy the same application on a real cluster and experience the power of CDAP.

More tutorial and guides for building applications on CDAP available here <link>