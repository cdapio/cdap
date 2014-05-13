.. :Author: John Jackson
   :Description: Continuuity Reactor Intermediate Apache Log Event Logger

==========================
TrafficAnalytics Example
==========================

----------------------------------------------------------
A Continuuity Reactor Application demonstrating MapReduce
----------------------------------------------------------

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

Overview
========
This example demonstrates an application of streaming log analysis. 
It computes the aggregate number of HTTP requests on an hourly basis
in each hour of the last twenty-four hours, processing in real-time Apache access log data. 
The application expands on the other `examples <http://continuuity.com/developers/examples>`__
to show how to use a MapReduce job.

Data from a log will be sent to the Continuuity Reactor by an external script *inject-log*
to the *logEventStream*. The logs are processed by the
``LogAnalyticsFlow``, which stores the log event in its entirety in *logEventTable*, a ``SimpleTimeseriesTable``.

As these entries are created, they are taken up by the *LogCountMapReduce* job, which
goes through the entries and tabulates results in another ``SimpleTimeseriesTable``, *countTable*.

Finally, you can query the *countTable* by using the ``getCounts`` method of the *LogCountProcedure*. It will
send back a JSON-formatted result with all the hours for which HTTP requests were tabulated.

Let's look at some of these elements, and then run the application and see the results.

The TrafficAnalytics Application
--------------------------------
As in the other `examples <http://continuuity.com/developers/examples>`__, the components 
of the application are tied together by the class ``TrafficAnalyticsApp``::

	public class TrafficAnalyticsApp implements Application {
	  // The row key of SimpleTimeseriesTable.
	  private static final byte[] ROW_KEY = Bytes.toBytes("f");
	  // The time window of 1 day converted into milliseconds.
	  private static final long TIME_WINDOW = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
	
	  @Override
	  public ApplicationSpecification configure() {
	    return ApplicationSpecification.Builder.with()
	      .setName("TrafficAnalytics")
	      .setDescription("HTTP request counts on an hourly basis")
	      // Ingest data into the Application via Streams.
	      .withStreams()
	        .add(new Stream("logEventStream"))
	      // Store processed data in Datasets.
	      .withDataSets()
	        .add(new SimpleTimeseriesTable("logEventTable"))
	        .add(new SimpleTimeseriesTable("countTable"))
	      // Process log data in real-time using Flows.
	      .withFlows()
	        .add(new LogAnalyticsFlow())
	      // Query the processed data using Procedures.
	      .withProcedures()
	        .add(new LogCountProcedure())
	      // Process log data in MapReduce.
	      .withMapReduce()
	        .add(new LogCountMapReduce())
	      .noWorkflow()
	      .build();
	  }

Many elements are similar, but there are a few new entries.

``SimpleTimeseriesTable``: Data Storage
---------------------------------------
The processed data is stored in SimpleTimeseriesTable DataSets:

- All entries are logically partitioned into time intervals of the same size based on the entry timestamp.
- Every row in the underlying Table holds entries of the same time interval with the same key.
- Each entry's data is stored in one column.

Once the log event data is stored in the *logEventTable*, it is already organized based on the
hourly partition that it falls into. It's then a matter of counting the number of entries in each hour
to determine the results for the *countTable*.

``LogCountMapReduce``: MapReduce Job
------------------------------------
This introduces us to a powerful element of Continuuity Reactor: its facility for running MapReduce jobs.
Once the data has been loaded into the Reactor, you can run the MapReduce job, which takes the 
data in the *logEventTable* and aggregates the log data by hour. 

There are three methods required for the implementation of a MapReduce job. In this case,
we'll use the default ``onFinish`` implementation (which does nothing), as we do not require
anything be done after the job has run. That leaves two methods to actually be 
implemented: ``configure`` and ``beforeSubmit``::

	  public static class LogCountMapReduce extends AbstractMapReduce {
	    // Annotation indicates the DataSet used in this MapReduce.
	    @UseDataSet("logEventTable")
	    private SimpleTimeseriesTable logs;
	
	    @Override
	    public MapReduceSpecification configure() {
	      return MapReduceSpecification.Builder.with()
	        .setName("RequestCountMapReduce")
	        .setDescription("Apache access log count MapReduce job")
	        // Specify the DataSet for Mapper to read.
	        .useInputDataSet("logEventTable")
	        // Specify the DataSet for Reducer to write.
	        .useOutputDataSet("countTable")
	        .build();
	    }
	...
	    @Override
	    public void beforeSubmit(MapReduceContext context) throws Exception {
	      Job job = context.getHadoopJob();
	      long endTime = System.currentTimeMillis();
	      long startTime = endTime - TIME_WINDOW;
	      // A Mapper processes log data for the last 24 hours in logs table by 2 splits.
	      context.setInput(logs, logs.getInput(2, ROW_KEY, startTime, endTime));
	      // Set the Mapper class.
	      job.setMapperClass(LogMapper.class);
	      // Set the output key of the Reducer class.
	      job.setMapOutputKeyClass(LongWritable.class);
	      // Set the output value of the Reducer class.
	      job.setMapOutputValueClass(IntWritable.class);
	      // Set the Reducer class.
	      job.setReducerClass(LogReducer.class);
	    }
	
These two methods configure and define the MapReduce job.
The work is done by instances of two additional classes—a *Mapper* and a *Reducer*.

The *Mapper*—implemented by the ``LogMapper`` class—transforms the log data into key-value pairs, 
where the key is the time stamp on the hour scale and the value (always the same, 1) is an
occurrence of a log event. The *Mapper* receives a log as a key-value pair
from the input DataSet and outputs the data as another key-value pair
to the *Reducer*.

The *Reducer*—implemented by the ``LogReducer`` class—aggregates the number of requests in each hour
and stores the results in an output ``SimpleTimeseriesTable``.


``LogCountProcedure``: Real-time Queries
----------------------------------------
The query (*getCounts*) used to obtain results defaults to a time range of
from now until 24 hours previous. You could pass in parameters to search for a different range,
and in an actual application that would be common.


Building and Running the Application and Example
================================================
In this remainder of this document, we refer to the Continuuity Reactor runtime as "Reactor", and the
example code that is running on it as an "Application".

We show the Windows prompt as ``>`` to indicate a command prompt opened in the SDK directory.

In this example, you can either build the Application from source or deploy the already-compiled JAR file.
In either case, you then start a Continuuity Reactor, deploy the Application, and then run the example by
injecting Apache access log entries from an example file into the Application. 

As you do so, you can query the Application to see the results
of its processing the log entries.

When finished, stop the Application as described below.

Building the AccessLogApp
-------------------------
From the project root, build ``TrafficAnalytics`` Application with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

(If you modify the code and would like to rebuild the Application, you can
skip the tests by using the command::

	$ mvn -Dmaven.test.skip=true clean package


Deploying and Starting the Application
--------------------------------------
Make sure an instance of the Continuuity Reactor is running and available. 
From within the SDK root directory, this command will start Reactor in local mode::

	$ bin/continuuity-reactor start

On Windows::

	> bin\reactor start

From within the Continuuity Reactor Dashboard (`http://localhost:9999/ <http://localhost:9999/>`__ in local mode):

#. Drag and drop the Application .JAR file (``target/TrafficAnalytics-1.0.jar``) onto your browser window.
   Alternatively, use the *Load App* button found on the *Overview* of the Reactor Dashboard.
#. Once loaded, select the ``TrafficAnalytics`` Application from the list.
   On the Application's detail page, click the *Start* button on **both** the *Process* and *Query* lists.
	
Command line tools are also available to deploy and manage apps. From within the project root:

#. To deploy the Application JAR file, run ``$ bin/appManager.sh --action deploy --gateway <hostname>``
#. To start the Application, run ``$ bin/appManager.sh --action start [--gateway <hostname>]``

:Note:	[--gateway <hostname>] is not available for a *Local Reactor*.

On Windows:

#. To deploy the App JAR file, run ``> bin\appManager deploy``
#. To start the App, run ``> bin\appManager start``

Running the Example
-------------------

Injecting Apache Log Entries
............................

Run this script to inject Apache access log entries 
from the log file ``src/test/resources/apache.accesslog``
to the Stream named *logEventStream* in the ``AccessLogApp``::

	$ ./bin/inject-log.sh [--gateway <hostname>]

:Note:	[--gateway <hostname>] is not available for a *Local Reactor*.

On Windows::

	> bin\inject-data

Running the MapReduce Job
.........................
Start the MapReduce job by:

- In the Continuuity Reactor Dashboard:

  #. Click the *Process* button.
  #. Click on the *RequestCountMapReduce* MapReduce.
  #. If its status is not **Running**, click the *Start* button.
  #. You should see the results change in the *Map* and *Reduce* icons, in the values
     shown for *In* and *Out*.
  #. If you check the *countTable* DataSet, you should find that its storage has changed from 0.

Querying the Results
....................
There are two ways to query the *countTable* DataSet:

- Send a query via an HTTP request using the ``curl`` command. For example::

	curl -v -X POST 'http://localhost:10000/v2/apps/TrafficAnalytics/procedures/LogCountProcedure/methods/getCounts'

  On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the example::

	libexec\curl...

- Type a Procedure method name, in this case ``getCounts``, in the Query page of the Reactor Dashboard:

  In the Continuuity Reactor Dashboard:

  #. Click the *Query* button.
  #. Click on the *LogCountProcedure* Procedure.
  #. Type ``getCounts`` in the *Method* text box.
  #. Click the *Execute* button.
  #. The results of the occurrences for each HTTP status code are displayed in the Dashboard
     in JSON format. The returned results will be unsorted, with time stamps in milliseconds.
     For example::

	{"1391706000000":3,"1391691600000":2,"1391702400000":2,
	 "1391688000000":2,"1391698800000":3,"1391695200000":4,
	 "1391684400000":1,"1391709600000":2,"1391680800000":2}

Stopping the Application
------------------------
Either:

- On the Application detail page of the Reactor Dashboard, click the *Stop* button on **both** the *Process* and *Query* lists; or
- Run ``$ ./bin/appManager.sh --action stop [--gateway <hostname>]``

  :Note:	[--gateway <hostname>] is not available for a *Local Reactor*.

  On Windows, run ``> bin\appManager stop``


Downloading the Example
=======================
`Download the example </developers/examples-files/continuuity-TrafficAnalytics-2.2.0.zip>`_
