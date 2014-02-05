
## AccessLogApp Example ##


##### A Continuuity Reactor Application demonstrating Streams, Flows, DataSets and Procedures #####

### Overview###

This example demonstrates a simple application for real-time streaming log analysis—computing the number of HTTP requests per hour in the last 24 hours by processing real-time Apache access log data.

The example introduces the basic constructs of the Continuuity Reactor programming paradigm: Applications, Streams, Flows, Procedures, MapReduce and DataSets.

In any real-time application, there are four distinct areas:

1. Data Collection: how to get the relevant signals needed by the real-time application for processing
2. Data Processing: the application logic to process the signals into meaningful analysis
3. Data Storage: saving the computed analysis in appropriate data structures
4. Data Queries: serving the computed data to any down-stream application that wants to use the data

The `AccessLogApp` application demonstrates using the abstractions of the Continuuity Reactor to cover these four areas.

Let's look at each one in turn.


##### The AccessLogApp application #####

All of the components (Streams, Flows, DataSets, and Procedures) of the application are tied together as a deployable entity by the class `AccessLogApp`, an implementation of `com.continuuity.api.Application`.

		public class AccessLogApp implements Application {
  		  // The row key of SimpleTimeseriesTable
  		  private static final byte[] ROW_KEY = 		Bytes.toBytes("f");

  		  private static final long TIME_WINDOW = 	TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

 		  @Override
  		  public ApplicationSpecification configure() {
    		return ApplicationSpecification.Builder.with()
      		  .setName("access-log-2")
      		  .setDescription("Requests counts by hour")
      		  // Ingest data into the app via Streams
      		  .withStreams()
        		.add(new Stream("app2-log-events"))
      		  // Store processed data in Datasets
      		  .withDataSets()
        		.add(new SimpleTimeseriesTable("logs"))
        		.add(new SimpleTimeseriesTable("counts"))
      		  // Process log data in real-time using flows
      		  .withFlows()
        		.add(new LogAnalyticsFlow())
      		  // Query the processed data using procedures
      		  .withProcedures()
        		.add(new RequestProcedure())
      		  // Process log data in MapReduce
      		  .withMapReduce()
        		.add(new LogCount())
      		  .noWorkflow()
      		  .build();
  }
  
##### Streams for data collection #####

Streams are the primary means for bringing data from external systems into the Continuuity Reactor in real-time.

Data can be written to streams using REST. In this example, a Stream named *app2-log-events* is used to ingest Apache access logs.

The Stream is configured to ingest data using the Apache Common Log Format. Here is a sample event from a log:

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
- 
##### Flows and Flowlets for real-time data processing #####

Data ingested through Streams can be processed in real-time using Flows, which are user-implemented realtime-stream processors.

A Flow is comprised of one or more Flowlets that are wired together as a Directed Acyclic Graph (DAG). Each Flowlet is able to perform custom logic and execute data operations for each individual data object processed.

In the example, two Flowlets are used to process the data:

- *collector*: parses the Apache access log entries coming into the *app2-log-events* Stream and store them in *logs* DataSet. It is implemented by `LogEventStoreFlowlet`.

The *collector* Flowlet and *log-events* Stream are wired together by the Flow implementation class `LogAnalyticsFlow`.

The *parser* and *counter* Flowlets are wired together by the Flow implementation class `LogAnalyticsFlow`.


#####MapReduce for batch data processing#####

A MapReduce job, *LogCount*, aggregates log data by hour. A Mapper, *LogMapper*, processes logs retrieved from an input DataSet *logs*, into key value pairs. The key is the timestamp on the hour scale. The value is the currence of a log, i.e. 1. A Reducer, *LogReducer*, aggregates the key value pairs from the Mapper and stores the results to an output DataSet, *counts*. 

##### DataSets for data storage#####

In the example, the processed log data is stored in two SimpleTimeseriesTable.

- *logs*: All logs are stored in one row. Each log is an entry of the row. The log is stored in the value field of the entry. To distinguish the logs with the same timestamp, the MD5 hash code of a log is stored as tag. 
- *counts*: All aggregated results are stored in one row.  Each result, the number of HTTP requests per hour, is an entry of the row. The value of the entry is the number of the requests per hour. The timestamp is one hour in millisecond.

##### Procedures for real-time queries #####

The data in DataSets can be served using Procedures for any real-time querying of the aggregated results. The `AccessLogApp` example has a procedure to retrieve all status codes and counts.


### Building and running the App and example ###

In this remainder of this document, we refer to the Continuuity Reactor runtime as "application", and the example code that is running on it as an "app".

In this example, you can either build the app from source or deploy the already-compiled JAR file. In either case, you then start a Continuuity Reactor, deploy the app, and then run the example by injecting Apache access log entries from an example file into the app.

As you do so, you can query the app to see the results of its processing the log entries.

When finished, stop the app as described below.

##### Building the AccessLogApp####

From the project root, build AccessLogApp with the following [Apache Maven](http://maven.apache.org/) command: `$ mvn clean package`

##### Deploying and starting the App #####

Make sure an instance of the Continuuity Reactor is running and available. From within the SDK root directory, this command will start Reactor in local mode:`$ bin/continuuity-reactor start`

From within the Reactor Dashboard ([http://localhost:9999/](http://localhost:9999/) in local mode):

1. Drag and drop the App JAR file (`target/logger2-1.0-SNAPSHOT.jar`) onto your browser window.

	Alternatively, use the *Load App* button found on the *Overview* of the Reactor Dashboard.
	
2. Once loaded, select `access-log-2` from the list. 

	On the app's detail page, click the *Start* button on **both** the *Process* and *Query* lists.
	
Command line tools are also available to deploy and manage apps. From within the project root:

1. To deploy the App JAR file, run `$ bin/deploy --app target/logger2-1.0-SNAPSHOT.jar [--gateway <hostname>]`.
2. To start the App, run `$ bin/logger-app --action start [--gateway <hostname>]`.

##### Running the example ####

###### Injecting Apache access log entries into the App ######

Running this script will inject Apache access log entries from the log file `resources/apache.accesslog` to a Stream named *app2-log-events* in the `AccessLogApp`:

`$ bin/inject-log [--gateway <hostname>]`

###### Query ######

There are two ways to query the number of logs per hour in the last 24 hours:

1. Send a query via an HTTP request using the `curl` command. For example:
`curl-X POST 'http://localhost:10000/v2/apps/access-log-2/procedures/RequestProcedure/methods/get-counts'`
2. Type a procedure method name, in this case `get-counts`, in the Query page of the Reactor Dashboard:

	In the Continuuity Reactor Dashboard:
	- Click the *Query* button.
	- Click on the *RequestProcedure* procedure.
	- Type `get-counts` in the *Method* text box.
	- Provide parameters of start time and end time in millisecond in JSON format, e.g.`{"startTs":1389657600000, "endTs":1389744000000}` in the PARAMETERS text box, or leave it empty for the last 24 hours.
	- Click the *Execute* button.
	- The results of the number of HTTP requests per hour in a time range are displayed in the dashboard in JSON format. By default, the time range is the last 24 hours.

		{"1389704400000":3,"1389715200000":6,"1389697200000":1,"1389690000000":1,"1389700800000":2,"1389686400000":3,"1389708000000":2}

			
##### Stopping the App #####
	
Either:

- On the App detail page of the Reactor Dashboard, click the *Stop* button on **both** the *Process* and *Query* lists; or
- Run `$ bin/logger-app --action stop [--gateway <hostname>]`

##### Where to go next #####

- [Continuuity.com](http://continuuity.com/)
- [Download Continuuity Reactor](http://www.tele3.cz/jbar/rest/url)
- [Developer Examples](http://www.tele3.cz/jbar/rest/url)
- [Developer Guide](http://www.tele3.cz/jbar/rest/url)
- [Support](http://support.continuuity.com/)

Copyright © 2014 Continuuity, Inc.

Continuuity and Continuuity Reactor are trademarks of Continuuity, Inc. All rights reserved.

Apache is a trademark of the Apache Software Foundation.
