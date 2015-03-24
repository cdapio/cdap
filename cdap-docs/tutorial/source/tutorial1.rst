.. meta::
    :author: Cask Data, Inc.
    :description: Basic Tutorial, Web Analytics Application
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _cdap-tutorial-basic:

==================================================
Basic Tutorial: A Simple Web Analytics Application
==================================================

This tutorial provides the basic steps for the development of a data application using the
Cask Data Application Platform (CDAP). We will use a Web Analytics Application to
demonstrate how to develop with CDAP and how CDAP helps when building data applications
that run in the Hadoop ecosystem.

Web analytics applications are commonly used to generate statistics and to provide
insights about web usage through the analysis of web traffic. A typical web analytics
application consists of three components:

- **Data Collection:** Collects and persists web logs for further processing; 
- **Data Analysis:** Analyses the collected data and produces different measurements; and 
- **Insights Discovery:** Extracts insights from the analysis results. 

Additionally, it’s important that the application be scalable, fault tolerant, and
easy-to-operate. It needs to support ever-increasing amounts of data as well as be
flexible in its design to accomodate new application requirements.

In this tutorial, we’ll show how easy it is to build a web analytics application with
CDAP. In particular, we’ll use these CDAP components:

- A **Stream** for web server log collection and persistence to the file system; 
- A **Flow** for real-time data analysis over collected logs; and 
- A **SQL Query** to explore and develop insights from the data.


Build and Run
=============
.. highlight:: console

The source code of the Web Analytics Application for this tutorial is
included in the CDAP SDK, inside the ``examples/WebAnalytics`` directory.

When the Standalone CDAP instance is running, you can build and start the Web Analytics
Application with these commands, executed from the ``examples/WebAnalytics`` directory of the
SDK::

  $ mvn package 
  $ bin/app-manager.sh --action deploy 
  $ bin/app-manager.sh --action start 

On Windows, run these instead::

  > mvn package 
  > bin\app-manager.bat --action deploy 
  > bin\app-manager.bat --action start


How It Works
============
In this section, we will go through the details about how to develop the Web
Analytics Application using CDAP.

Data Collection with a Stream 
-----------------------------
The sole data source that the Web Analytics Application uses is web server logs. Log
events are ingested to a **Stream** called ``log`` using the RESTful API provided by CDAP.

To ingest a log event, you can use the ``curl`` command (the example shown has been 
reformatted to fit)::

  $ curl -d '192.168.252.135 - - [14/Jan/2014:00:12:51 -0400] "GET /products HTTP/1.1" 500 182 
       "http://www.example.org" "Mozilla/5.0"' http://localhost:10000/v2/streams/log 

This sends the log event (formatted in the Common Log Format or CLF) to the CDAP instance
located at ``localhost`` and listening on port ``10000``.

The Application includes sample logs, located in ``test/resources/access.log`` that you can
inject by running a provided script::

  $ bin/inject-data.sh
  
On Windows::

  $ bin/inject-data.bat 
  
Once an event is ingested into a Stream, it is persisted and available for processing.

Data Analysis using a Flow 
--------------------------
The Web Analytics Application uses a **Flow**, the real-time data
processor in CDAP, to produce real-time analytics from the web server logs. A Flow
contains one or more **Flowlets** that are wired together into a directed acyclic graph or
DAG.

To keep the example simple, we only compute the total visit count for each IP visiting the
site. We use a Flowlet called ``UniqueVisitor`` to keep track of the unique visit counts from
each client. It is done in three steps:

  1. Read a log event from the log Stream; 
  #. Parse the client IP from the log event; and
  #. Increment the visit count of that client IP by 1 and persist the change. 

The result of the increment is persisted to a custom **Dataset** ``UniqueVisitCount``.

.. highlight:: java

Here is what the UniqueVisitor Flowlet looks like::

  public class UniqueVisitor extends AbstractFlowlet {

    // Request an instance of the UniqueVisitCount Dataset
    @UseDataSet("UniqueVisitCount") private UniqueVisitCount table;

    @ProcessInput public void process(StreamEvent streamEvent) {
      // Decode the log line as a String
      String event = Charset.forName("UTF-8").decode(streamEvent.getBody()).toString();

      // The first entry in the log event is the IP address
      String ip = event.substring(0, event.indexOf(' '));

      // Increment the visit count of a given IP by 1
      table.increment(ip, 1L);
    }
  }

The ``UniqueVisitorCount`` Dataset provides an abstraction of the data logic for incrementing
the visit count for a given IP. It exposes an ``increment`` method, implemented as::

  /**
   * Performs increments of the visit count of the given IP.
   *
   * @param ip The IP to increment 
   * @param amount The amount to increment 
   */
  public void increment(String ip, long amount) {
    // Delegates to the system KeyValueTable for the actual storage operation
    keyValueTable.increment(Bytes.toBytes(ip), amount);
  }

The complete source code of the ``UniqueVisitorCount`` class can be found in the example in
``src/main/java/co/cask/cdap/examples/webanalytics/UniqueVisitCount.java``.

To connect the ``UniqueVisitor`` Flowlet to read from the ``log`` Stream, we define a
``WebAnalyticsFlow`` class that specifies the Flow::

  public class WebAnalyticsFlow implements Flow { 
    @Override 
    public FlowSpecification configure() { 
      return FlowSpecification.Builder.with() 
        .setName("WebAnalyticsFlow")
        .setDescription("Web Analytics Flow") 
        .withFlowlets()
          .add("UniqueVisitor", new UniqueVisitor())  // Only one Flowlet in this Flow 
        .connect()
          .fromStream("log").to("UniqueVisitor")      // Feed events written to the "log" Stream to UniqueVisitor 
        .build();
    }
  }
  
Lastly, we bundle up the Dataset and the Flow we’ve defined together to form an
``Application`` that can be deployed and executed in CDAP::

  public class WebAnalytics extends AbstractApplication {

    @Override 
    public void configure() { 
      addStream(new Stream("log")); 
      addFlow(new WebAnalyticsFlow()); 
      createDataset("UniqueVisitCount", UniqueVisitCount.class);
      setName("WebAnalytics"); 
      setDescription("Web Analytics Application");
    }
  }
  
Query the Unique Visitor Page Views
-----------------------------------
Once the log data has been processed by the *WebAnalyticsFlow*, we can explore the Dataset
*UniqueVisitCount* with a SQL query. You can easily execute SQL queries against Datasets
using the CDAP Console (open `http://localhost:9999 <http://localhost:9999>`__ in your
browser) by simply selecting **Store** on the left sidebar, clicking the **Explore**
button on the right, and then selecting the *UniqueVisitCount* Dataset:

.. image:: _images/wa_explore_store.png 
   :width: 6in
   :align: center

.. highlight:: console

You can then run SQL queries against the Dataset. Let’s try
to find the top five IP addresses that visited the site by running a SQL query::

  SELECT * FROM cdap_user_uniquevisitcount ORDER BY value DESC LIMIT 5

.. image:: _images/wa_explore_query.png 
   :width: 6in
   :align: center

You can copy and paste the above SQL into the **Query** box and
click **Execute** to run it. It may take a while for the query to finish. Once it’s finished,
you can click on the **Result** button at the bottom to show the query results:

.. image:: _images/wa_explore_result.png 
   :width: 6in
   :align: center
