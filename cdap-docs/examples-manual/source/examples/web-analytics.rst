.. meta::
    :author: Cask Data, Inc.
    :description: An example Cask Data Application Platform application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-web-analytics:

=========================
Web Analytics Application
=========================

A Cask Data Application Platform (CDAP) tutorial demonstrating how to perform analytics
using access logs.


Overview
========
This tutorial provides the basic steps for the development of a data application using the
Cask Data Application Platform (CDAP). We will use a web analytics application to
demonstrate how to develop with CDAP and how CDAP helps when building data applications
that run in the Hadoop ecosystem.

Web analytics applications are commonly used to generate statistics and to provide insights
about web usage through the analysis of web traffic. A typical web analytics application
consists of three components:

- **Data Collection:** Collects and persists web logs for further processing;
- **Data Analysis:** Analyses the collected data and produces different measurements; and
- **Insights Discovery:** Extracts insights from the analysis results.

Additionally, it's important that the application be scalable, fault tolerant, and
easy-to-operate. It needs to support ever-increasing amounts of data as well as be flexible
in its design to accomodate new application requirements.

In this tutorial, we'll show how easy it is to build a web analytics application with CDAP.
In particular, we'll use these CDAP components:

- A **stream** for web server log collection and persistence to the file system;
- A **flow** for real-time data analysis over collected logs; and
- **SQL Queries** to explore and develop insights from the data.


How It Works
============
In this section, we’ll go through the details about how to develop a web analytics application using CDAP.

Data Collection with a Stream
-----------------------------
The sole data source that the web analytics application uses is web server logs. Log events are ingested to
a **stream** called *log* using the RESTful API provided by CDAP.

Once an event is ingested into a stream, it is persisted and available for processing.

Data Analysis using a Flow
--------------------------
The web analytics application uses a **flow**, the real-time data processor in CDAP,
to produce real-time analytics from the web server logs. A **flow** contains one or more
**flowlets** that are wired together into a directed acyclic graph or DAG.

To keep the example simple, we only compute the total visit count for each IP visiting the site.
We use a flowlet of type ``UniqueVisitor`` to keep track of the unique visit counts from each client.
It is done in three steps:

1. Read a log event from the *log* stream;
#. Parse the client IP from the log event; and
#. Increment the visit count of that client IP by 1 and persist the change.

The result of the increment is persisted to a custom **dataset** ``UniqueVisitCount``.

Here is what the ``UniqueVisitor`` flowlet looks like:

.. literalinclude:: /../../../cdap-examples/WebAnalytics/src/main/java/co/cask/cdap/examples/webanalytics/UniqueVisitor.java
   :language: java
   :lines: 33-

The ``UniqueVisitCount`` dataset provides an abstraction of the data logic for incrementing the visit count for a
given IP. It exposes an ``increment`` method, implemented as:

.. literalinclude:: /../../../cdap-examples/WebAnalytics/src/main/java/co/cask/cdap/examples/webanalytics/UniqueVisitCount.java
   :language: java
   :lines: 62-66
   :dedent: 2

The complete source code of the ``UniqueVisitCount`` class can be found in the example in
``src/main/java/co/cask/cdap/examples/webanalytics/UniqueVisitCount.java``

To connect the ``UniqueVisitor`` flowlet to read from the *log* stream, we define a ``WebAnalyticsFlow`` class
that specifies the flow:

.. literalinclude:: /../../../cdap-examples/WebAnalytics/src/main/java/co/cask/cdap/examples/webanalytics/WebAnalyticsFlow.java
   :language: java
   :lines: 26-35
   :dedent: 2

Lastly, we bundle up the dataset and the flow we've defined together to form an ``application`` that can be deployed
and executed in CDAP:

.. literalinclude:: /../../../cdap-examples/WebAnalytics/src/main/java/co/cask/cdap/examples/webanalytics/WebAnalytics.java
   :language: java
   :lines: 26-


.. Building and Starting
.. =====================
.. |example| replace:: WebAnalytics
.. |example-italic| replace:: *WebAnalytics*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <WebAnalytics>`

.. include:: _includes/_building-starting-running.txt


Running the Example
===================

.. Starting the Flow
.. -----------------
.. |example-flow| replace:: WebAnalyticsFlow
.. |example-flow-italic| replace:: *WebAnalyticsFlow*

.. include:: _includes/_starting-flow.txt

Injecting Log Events
---------------------
To inject a single log event, you can use the ``curl`` command:

.. tabbed-parsed-literal::

  $ curl -d "192.168.252.135 - - [14/Jan/2014:00:12:51 -0400] 'GET /products HTTP/1.1' 500 182 'http://www.example.org' 'Mozilla/5.0'" \
  "http://localhost:11015/v3/namespaces/default/streams/log"

This sends the log event (formatted in the Common Log Format or CLF) to the CDAP instance located at
``localhost`` and listening on port ``11015``.

The application includes sample logs, located in ``examples/resources/accesslog.txt`` that you can inject 
using the CDAP Commmand Line Interface:

.. tabbed-parsed-literal::

  $ cdap cli load stream log examples/resources/accesslog.txt
  
  Successfully loaded file to stream 'log'

Query the Unique Visitor Page Views
---------------------------------------
Once the log data has been processed by the ``WebAnalyticsFlow``, we can explore the
dataset ``UniqueVisitCount`` with a SQL query. You can easily execute SQL queries against
datasets using the CDAP UI by going to the *Data* page showing :cdap-ui-data:`All
Datasets <>`, entering *UniqueVisitCount* in the search box, and clicking on the
**UniqueVisitCount** dataset:

.. figure:: _images/web-analytics-0.png
   :figwidth: 100%
   :width: 800px
   :align: center
   :class: bordered-image
  
Then, once at the dataset detail page, select the *Explore* tab:

.. figure:: _images/web-analytics-1.png
   :figwidth: 100%
   :width: 800px
   :align: center
   :class: bordered-image

You can then run SQL queries against the dataset. Let's try to find the top five IP
addresses that visited the site by running a SQL query:

.. tabbed-parsed-literal::
    :single:
    :copyable:
    
    SELECT * FROM dataset_uniquevisitcount ORDER BY value DESC LIMIT 5   


You can copy and paste the above SQL into the text box as shown below (replacing the
default query that is there) and click the **Execute SQL** button to run it. It may take a
moment for the query to finish.

.. figure:: _images/web-analytics-2.png
   :figwidth: 100%
   :width: 800px
   :align: center
   :class: bordered-image

Once it's finished, click on the preview button the right side of the **Results**
table:

.. figure:: _images/web-analytics-3.png
   :figwidth: 100%
   :width: 800px
   :align: center
   :class: bordered-image

This will display the first five rows of the query results:

.. figure:: _images/web-analytics-4.png
   :figwidth: 100%
   :width: 800px
   :align: center
   :class: bordered-image


.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-flow-removing-application.txt
