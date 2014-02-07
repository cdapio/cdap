.. :Author: John Jackson
   :Description: Continuuity Reactor Intermediate Apache Log Event Logger

==========================
AccessLogHourlyApp Example
==========================

----------------------------------------------------------
A Continuuity Reactor Application demonstrating MapReduce
----------------------------------------------------------

.. reST Editor: section-numbering::

.. reST Editor: contents::

Overview
========
This example demonstrates an intermediate-level application of real-time 
streaming log analysis. It computes the aggregate number of HTTP requests on an hourly basis
in each hour of the last twenty-four hours, processing in real-time Apache access log data. 

The example expands on the `basic example <example1>`__ programming
to give an example of using a MapReduce job.

Data from a log will be sent to the Continuuity Reactor by an external script *inject-log*
to the *logEventHourlyStream*. The logs are processed by the
*LogAnalyticsFlow*, which stores the log event in its entirety in *logEventsTable*, a ``SimpleTimeseriesTable``.

As these entries are created, they are taken up by the *LogCountMapReduce* job, which
goes through the entries and tabulates results in another ``SimpleTimeseriesTable``, *countsTable*.

Finally, you can query the *countsTable* by using the ``getCounts`` method of the *LogCountProcedure*. It will
send back a JSON-formatted result with all the hours for which HTTP requests were tabulated.

Let's look at some of these elements, and then run the application and see the results.

The AccessLogHourlyApp Application
----------------------------------
As in the `basic example <example1>`__, the components 
of the application are tied together by the class ``AccessLogHourlyApp``::

	public class AccessLogHourlyApp implements Application {
	  // The row key of SimpleTimeseriesTable
	  private static final byte[] ROW_KEY = Bytes.toBytes("f");
	  // The time window of 1 day converted into milliseconds
	  private static final long TIME_WINDOW = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);
	
	  @Override
	  public ApplicationSpecification configure() {
	    return ApplicationSpecification.Builder.with()
	      .setName("AccessLogHourlyAnalytics")
	      .setDescription("HTTP request counts on an hourly basis")
	      // Ingest data into the app via Streams
	      .withStreams()
	        .add(new Stream("logEventHourlyStream"))
	      // Store processed data in Datasets
	      .withDataSets()
	        .add(new SimpleTimeseriesTable("logEventsTable"))
	        .add(new SimpleTimeseriesTable("countsTable"))
	      // Process log data in real-time using flows
	      .withFlows()
	        .add(new LogAnalyticsFlow())
	      // Query the processed data using procedures
	      .withProcedures()
	        .add(new LogCountProcedure())
	      // Process log data in MapReduce
	      .withMapReduce()
	        .add(new LogCountMapReduce())
	      .noWorkflow()
	      .build();
	  }

Many elements are similar to the `basic example <example1>`__, but there are a few new entries.

``SimpleTimeseriesTable``: Data Storage
---------------------------------------------------
The processed data is stored in SimpleTimeseriesTable DataSets:

- All entries are logically partitioned into time intervals of the same size based on the entry timestamp.
- Every row in the underlying Table holds entries of the same time interval with the same key.
- Each entry's data is stored in one column.

Once the log event data is stored in the *logEventsTable*, it is already organized based on the
hourly partition that it falls into. It's then a matter of counting the number of entries in each hour
to determine the results for the *countsTable*.

``LogCountMapReduce``: MapReduce Job
------------------------------------
This introduces us to a powerful element of Continuuity Reactor: its facility for running MapReduce jobs.
Once the data has been loaded into the Reactor, you can run the MapReduce job, which takes the 
data in the *logEventsTable* and aggregates the log data by hour. 

There are three methods required for the implementation of a MapReduce job. In this case,
we'll use the default ``onFinish`` implementation (which does nothing), as we do not require
anything be done after the job has run. That leaves two methods: ``configure`` and ``beforeSubmit``::

	  public static class LogCountMapReduce extends AbstractMapReduce {
	    // Annotation indicates the DataSet used in this MapReduce.
	    @UseDataSet("logEventsTable")
	    private SimpleTimeseriesTable logs;
	
	    @Override
	    public MapReduceSpecification configure() {
	      return MapReduceSpecification.Builder.with()
	        .setName("RequestCountMapReduce")
	        .setDescription("Apache access log count MapReduce job")
	        // Specify the DataSet for Mapper to read.
	        .useInputDataSet("logEventsTable")
	        // Specify the DataSet for Reducer to write.
	        .useOutputDataSet("countsTable")
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
The work is done by instances of two other classes—a *Mapper* and a *Reducer*.

The *Mapper*—implemented by the ``LogMapper`` class—transforms the log data into key-value pairs, 
where the key is the time stamp on the hour scale and the value (always the same, 1) is an
occurrence of a log event. The *Mapper* receive a log as a key-value pair
from the input DataSet and outputs the data as another key-value pair
to the *Reducer*.

The *Reducer*—implemented by the ``LogReducer`` class—aggregates the number of requests in each hour
and stores the results in an output ``SimpleTimeseriesTable``.


``LogCountProcedure``: Real-time Queries
----------------------------------------
The query (*getCounts*) used to obtain results defaults to a time range of
from now until 24 hours previous. You could pass in parameters to search for a different range,
and in an actual application that would be common.


Building and Running the App and Example
================================================
In this remainder of this document, we refer to the Continuuity Reactor runtime as "application", and the
example code that is running on it as an "app".

In this example, you can either build the app from source or deploy the already-compiled JAR file.
In either case, you then start a Continuuity Reactor, deploy the app, and then run the example by
injecting Apache access log entries from an example file into the app. 

As you do so, you can query the app to see the results
of its processing the log entries.

When finished, stop the app as described below.

Building the AccessLogApp
-------------------------
From the project root, build ``AccessLogHourlyApp`` with the 
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

(If you modify the code and would like to rebuild the app, you can
skip the tests by using the command::

	$ mvn -Dmaven.test.skip=true clean package


Deploying and Starting the App
------------------------------
Make sure an instance of the Continuuity Reactor is running and available. 
From within the SDK root directory, this command will start Reactor in local mode::

	$ bin/continuuity-reactor start

From within the Continuuity Reactor Dashboard (`http://localhost:9999/ <http://localhost:9999/>`__ in local mode):

#. Drag and drop the App .JAR file (``target/accessLogHourly-1.0.jar``) onto your browser window.
	Alternatively, use the *Load App* button found on the *Overview* of the Reactor Dashboard.
#. Once loaded, select the ``AccessLogHourlyAnalytics`` app from the list.
	On the app's detail page, click the *Start* button on **both** the *Process* and *Query* lists.
	
Command line tools are also available to deploy and manage apps. From within the project root:

#. To deploy the App JAR file, run ``$ bin/deploy --app target/accessLogHourly-1.0.jar``
#. To start the App, run ``$ bin/AccessLogHourlyAnalytics --action start [--gateway <hostname>]``

Running the Example
-------------------

Injecting Apache Log Entries
............................

Run this script to inject Apache access log entries 
from the log file ``src/test/resources/apache.accesslog``
to the Stream named *logEventHourlyStream* in the ``AccessLogApp``::

	$ ./bin/inject-log [--gateway <hostname>]

Running the MapReduce Job
.........................
Start the MapReduce job by either:

- In the Continuuity Reactor Dashboard:

	#. Click the *Process* button.
	#. Click on the *RequestCountMapReduce* MapReduce.
	#. If its status is not **Running**, click the *Start* button.
	#. You should see the results change in the *Map* and *Reduce* icons, in the values
	   shown for *In* and *Out*.
	#. If you check the *countsTable* DataSet, you should find that its storage has changed from 0.

- Using the command line:

	#. Run ``$ ./bin/AccessLogHourlyAnalytics --action start [--gateway <hostname>]``


Querying the Results
....................
There are two ways to query the *countsTable* DataSet:

- Send a query via an HTTP request using the ``curl`` command. For example::

	curl -v -X POST 'http://localhost:10000/v2/apps/AccessLogHourlyAnalytics/procedures/LogCountProcedure/methods/getCounts'

- Type a procedure method name, in this case ``getCounts``, in the Query page of the Reactor Dashboard:

	In the Continuuity Reactor Dashboard:

	#. Click the *Query* button.
	#. Click on the *LogCountProcedure* procedure.
	#. Type ``getCounts`` in the *Method* text box.
	#. Click the *Execute* button.
	#. The results of the occurrences for each HTTP status code are displayed in the Dashboard in JSON format.
	   The returned results will be unsorted, with time stamps in milliseconds. For example:

::

	{"1391706000000":3,"1391691600000":2,"1391702400000":2,
	"1391688000000":2,"1391698800000":3,"1391695200000":4,
	"1391684400000":1,"1391709600000":2,"1391680800000":2}


Stopping the App
----------------
Either:

- On the App detail page of the Reactor Dashboard, click the *Stop* button on **both** the *Process* and *Query* lists; or
- Run ``$ ./bin/AccessLogHourlyAnalytics --action stop [--gateway <hostname>]``

.. include:: ../includes/footer.rst