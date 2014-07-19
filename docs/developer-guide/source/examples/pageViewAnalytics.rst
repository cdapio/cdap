.. :Author: Continuuity, Inc.
   :Description: Continuuity Reactor Advanced Apache Log Event Logger

============================
PageViewAnalytics Example
============================

**A Continuuity Reactor Application Demonstrating Custom Datasets and Metrics**

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

Overview
========
This example demonstrates use of custom Datasets, batch processing and
custom metrics in an Application.
It takes data from Apache access logs,
parses them and save the data in a custom Dataset. It then queries the results to find,
for a specific URI, pages that are requesting that page and the distribution of those requests.

The custom Dataset shows how you include business logic in the definition of a Dataset.
By doing so, the Dataset does more than just store or convert data–it
expresses methods that can perform valuable operations, such as counting and tabulating results
based on the Dataset's knowledge of its underlying data.

Data from a log will be sent to the Continuuity Reactor by an external script *inject-log*
to the *logEventStream*. Each entry of the log data—a page view—has two items of interest: 
the referrer page URI (which is sometimes blank)
and the requested page URI. Together these two tell us which pages are requesting a particular page.

The logs are processed by the
*PageViewFlow*, which parses the log event for its referrer tags, 
aggregates the counts of the requested pages and then
stores the results in the custom Dataset *pageViewCDS*, a instance of ``PageViewStore``.

You can view the user-defined ("custom") metric by adding—in the 
Continuuity Reactor Dashboard's Metrics Explorer—a metric
*logs.noreferrer* for the element Flow *PageViewFlow*.

Finally, you can query the *pageViewCDS* for a specified URI by using the ``getDistribution`` 
method of the *PageViewProcedure*. It will
send back a JSON-formatted result with the percentage of the requested pages viewed from the referrer page.

Let's look at some of these elements, and then run the Application and see the results.

The PageViewAnalytics Application
---------------------------------
As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``PageViewAnalyticsApp``::

  public class PageViewAnalyticsApp extends AbstractApplication {

    @Override
    public void configure() {
      setName("PageViewAnalytics");
      setDescription("Page view analysis");
      
      // Ingest data into the Application via Streams
      addStream(new Stream("logEventStream"));
      
      // Store processed data in Datasets
      createDataSet("pageViewCDS", PageViewStore.class);
      
      // Process log events in real-time using Flows
      addFlow(new LogAnalyticsFlow());
      
      // Query the processed data using a Procedure
      addProcedure(new PageViewProcedure());
    }
    // ...

``PageViewStore``: Custom Data Storage
--------------------------------------
The processed data is stored in a custom Dataset, ``PageViewStore``, with these
methods defined:

#. ``incrementCount(PageView pageView)``

   This method is what actually puts data into the Dataset, by incrementing the
   Dataset with each page view's referrer and originating URI.

#. ``Map<String, Long> getPageCount(String referrer)``

   This method gets the count of requested pages viewed from a specified referrer page.

#. ``getCounts(String referrer)``

   This method determines the total number of requested pages viewed from a specified referrer page.


``LogEventParseFlowlet``: Custom Metric
---------------------------------------
In this Flowlet the metric ``logs.noreferrer`` is defined and
counts those log events without a referrer page URI field.


``PageViewProcedure``: Real-time Queries
-----------------------------------------
The query (*getDistribution*) used to obtain results.


Building and Running the Application and Example
================================================
In this remainder of this document, we refer to the Continuuity Reactor runtime as "Reactor", and the
example code that is running on it as an "Application".

We show the Windows prompt as ``~SDK>`` to indicate a command prompt opened in the SDK directory.

In this example, you can either build the app from source or deploy the already-compiled JAR file.
In either case, you then start a Continuuity Reactor, deploy the app, and then run the example by
injecting Apache access log entries from an example file into the app. 

As you do so, you can query the app to see the results
of its processing the log entries.

When finished, stop the Application as described below.

Building the PageViewAnalyticsApp
----------------------------------
From the project root, build ``PageViewAnalytics`` with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

(If you modify the code and would like to rebuild the Application, you can
skip the tests by using the command::

	$ mvn -Dmaven.test.skip=true clean package


Deploying and Starting the Application
--------------------------------------
Make sure an instance of the Continuuity Reactor is running and available.
From within the SDK root directory, this command will start Reactor in local mode::

	$ ./bin/reactor.sh start

On Windows::

	~SDK> bin\reactor.bat start

From within the Continuuity Reactor Dashboard (`http://localhost:9999/ <http://localhost:9999/>`__ in local mode):

#. Drag and drop the Application .JAR file (``target/PageViewAnalytics-<version>.jar``)
   onto your browser window.
   Alternatively, use the *Load App* button found on the *Overview* of the Reactor Dashboard.
#. Once loaded, select the ``PageViewAnalytics`` Application from the list.
   On the Application's detail page, click the *Start* button on **both** the *Process* and *Query* lists.

On Windows:

#. To deploy the App JAR file, run ``~SDK> bin\app-manager.bat deploy`` or drag and drop the
   Application .JAR file (``target/PageViewAnalytics-<version>.jar`` onto your browser window.
   (Currently, the *Load App* button does not work under Windows.)
#. To start the App, run ``~SDK> bin\app-manager.bat start``

Running the Example
-------------------

Injecting Apache Log Entries
............................

Run this script to inject Apache access log entries 
from the log file ``src/test/resources/apache.accesslog``
to the Stream named *logEventStream* in the ``PageViewAnalyticsApp``::

	$ ./bin/inject-data.sh [--host <hostname>]

:Note:	[--host <hostname>] is not available for a *Local Reactor*.

On Windows::

	~SDK> bin\inject-data.bat

Querying the Results
....................
If the Procedure has not already been started, you start it either through the 
Continuuity Reactor Dashboard or via an HTTP request using the ``curl`` command::

	curl -v -X POST 'http://localhost:10000/v2/apps/PageViewAnalytics/procedures/PageViewProcedure/start'
	
There are two ways to query the *pageViewCDS* custom Dataset:

- Send a query via an HTTP request using the ``curl`` command. For example::

	curl -v -d '{"page": "http://www.continuuity.com"}' -X POST 'http://localhost:10000/v2/apps/PageViewAnalytics/procedures/PageViewProcedure/methods/getDistribution'

  On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the example::

	libexec\curl...

- Type a Procedure method name, in this case ``getDistribution``, in the Query page of the Reactor Dashboard:

	In the Continuuity Reactor Dashboard:

	#. Click the *Query* button.
	#. Click on the *PageViewProcedure* Procedure.
	#. Type ``getDistribution`` in the *Method* text box.
	#. Type the parameters required for this method, a JSON string with the name *page* and
	   value of a URI, ``"http://www.continuuity.com"``:

	   ::

		{ "page" : "http://www.continuuity.com" }

	   ..

	#. Click the *Execute* button.
	#. The results of the occurrences for each HTTP status code are displayed in the Dashboard
	   in JSON format. The returned results will be unsorted, with time stamps in milliseconds.
	   For example:

	   ::

		{"/careers":0.05,"/how-it-works":0.05,"/enterprise":0.05,"/developers":0.05,
		"https://accounts.continuuity.com/signup":0.2,"/":0.15,"/contact-sales":0.1,
		"https://accounts.continuuity.com/login":0.15,"/products":0.2}


Stopping the Application
------------------------
Either:

- On the Application detail page of the Reactor Dashboard, click the *Stop* button on **both** the *Process* and *Query* lists; or
- Run ``$ ./bin/app-manager.sh --action stop [--host <hostname>]``

  :Note:	[--host <hostname>] is not available for a *Local Reactor*.

  On Windows, run ``~SDK> bin\app-manager.bat stop``


Downloading the Example
=======================
This example (and more!) is included with our `software development kit <http://continuuity.com/download>`__.
