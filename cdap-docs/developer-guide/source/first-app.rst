.. :author: Cask Data, Inc.
   :description: Advanced Cask Data Application Platform Features
   :copyright: Copyright © 2014 Cask Data, Inc.

Web Analytics Application
-------------------------
This tutorial provides basics of developing data application using the Cask Data Application Platform (CDAP).
We will use a Web Analytics Application to demonstrate how to develop with CDAP and how does CDAP
helps in building data application that runs in the Hadoop Ecosystem.

Web analytics application is commonly used to generate statistics and to provide insights
about the web usage through the analysis of web traffic. A typical web analytics application
always consists of the following three components:

* **Data Collection** - Collects and persists web logs for further processing
* **Data Analysis** - Analyses collected data and produce different measurements
* **Insights Discovery** - Extracts insights from the analysis results

Additionally, it is desirable that the application is scalable, fault tolerant and easy to operate
in order to support ever-increasing amount of data as well as new requirements.

In this tutorial, we would like to show how easy it is to build a web analytics application with CDAP.
In particular, we use the following components in CDAP:

* **Stream** for web server logs collection and persistence to the file system
* **Flow** for real-time data analysis over logs collected
* **SQL** to explore and getting insights form the data

Build and Run
-------------
The source code of Web Analytics Application for this tutorial comes with the CDAP SDK,
inside the ``examples/WebAnalytics`` directory.

When the SDK is running, you can build and start the Web Analytics Application by running::

  $ mvn package
  $ bin/app-manager.sh --action deploy
  $ bin/app-manager.sh --action start

Or in Windows, run::

  $ mvn package
  $ bin/app-manager.bat --action deploy
  $ bin/app-manager.bat --action start

in the ``examples/WebAnalytics`` directory inside the SDK.

How it is done
--------------
In this section, we will go through details about how to develop the Web Analytics Application using CDAP.

Data Collection with Stream
...........................
The only data source that the Web Analytics Application uses is web server logs. Log events are ingested to
a **Stream** called ``log`` using REST API provided by CDAP.

For example, to ingest a log event in the Common Log Format (CLF), you can do it by::

  $ curl -d '192.168.252.135 - - [14/Jan/2014:00:12:51 -0400] "GET /products HTTP/1.1" 500 182 "http://www.example.org" "Mozilla/5.0"' http://localhost:10000/v2/streams/log

The application includes some sample logs, located in ``test/resources/access.log``.
You can inject the sample logs by running::

  $ bin/inject-data.sh

Or in Windows::

  $ bin/inject-data.bat

Once events are ingested into a Stream, it is persisted and is available for processing.

Data Analysis using Flow
........................
The Web Analytics Application uses **Flow**, the real-time data processor in CDAP,
to produce real-time analytics from the web server logs. A **Flow** contains one more more
**Flowlets** that are wired together into a directed acyclic graph or DAG.

To keep it simple, we only computes total visit count for each IP visiting the site.
We use a Flowlet called ``UniqueVisitor`` to keep track of the unique visit counts from each client.
It is done with the following steps:

1. Read a log event from the ``log`` Stream
#. Parse the client IP from the log event
#. Increments the visit count of that client IP by 1 and persist the change.

The result of the increment is persisted using a custom **Dataset** ``UniqueVisitCount``.

Here is how the ``UniqueVisitor`` Flowlet looks like::

  public class UniqueVisitor extends AbstractFlowlet {

    // Request an instance of UniqueVisitCount Dataset
    @UseDataSet("UniqueVisitCount")
    private UniqueVisitCount table;

    @ProcessInput
    public void process(StreamEvent streamEvent) {
      // Decode the log line as String
      String event = Charset.forName("UTF-8").decode(streamEvent.getBody()).toString();

      // The first entry in the log event is the IP address
      String ip = event.substring(0, event.indexOf(' '));

      // Increments the visit count of a given IP by 1
      table.increment(ip, 1L);
    }
  }

The ``UniqueVisitorCount`` Dataset provides an abstraction of the data logic for incrementing the visit count for a
given IP. It exposes an ``increment`` method with implementation like this::

  /**
   * Performs increments of visit count of the given IP.
   *
   * @param ip The IP to increment
   * @param amount The amount to increment
   */
  public void increment(String ip, long amount) {
    // Delegates to the system KeyValueTable for actual storage operation
    keyValueTable.increment(Bytes.toBytes(ip), amount);
  }

You can find the complete source code of the ``UniqueVisitorCount`` class in
``src/main/java/co/cask/cdap/examples/webanalytics/UniqueVisitCount.java``

To wire up the ``UniqueVisitor`` Flowlet to read from the ``log`` Stream, we defined a ``WebAnalyticsFlow`` class to
setup the Flow::

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


Lastly, we bundle up the Dataset and the Flow we defined above to form an ``Application`` so that it can be deployed
and executes in CDAP::

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

Query Unique Visitor Page Views
...............................
Once log data is being processed by the ``WebAnalyticsFlow``, we can explore the Dataset ``UniqueVisitCount``
with SQL query. You can execute SQL query against Datasets easily through the CDAP Console by
simply select **Store** on the left sidebar, click the **Explore** button on the right and select
the **UniqueVisitCount** Dataset.

.. image:: _images/quickstart/wa_explore_store.png
   :width: 10in

You can then runs SQL query against the Dataset. Let's try to find the top 5 IP that visited the site most by running
the following SQL query::

  SELECT * FROM cdap_user_uniquevisitcount ORDER BY value DESC LIMIT 5

.. image:: _images/quickstart/wa_explore_query.png
   :width: 10in

You can copy and paste the above SQL into the **Query** box and click **Execute** to run it. It may takes a while for
the query to finish. Once it is finished, you can click on the result at the bottom to show the query result.

.. image:: _images/quickstart/wa_explore_result.png
   :width: 10in

What's Next
-----------
Congratulations on successfully build and run your first CDAP application. You can learn more about developing
data application using CDAP by:

* Explore the Web Analytics Application source code. It also includes test cases to show you how to unit-test your
  application.
* Tryout another CDAP tutorial in Building an Web Insights Engine (link).
* Get more in-depths understanding of what CDAP is capable of in *url*.