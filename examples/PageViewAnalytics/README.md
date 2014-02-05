## AccessLogApp Example ##

##### A Continuuity Reactor Application demonstrating Streams, Flows, DataSets and Procedures #####


### Overview ###

This example demonstrates a simple application for real-time streaming log analysis—analysis of the distribution of page views by processing real-time Apache access log data.

The example introduces the basic constructs of the Continuuity Reactor programming paradigm: Applications, Streams, Flows, Procedures and DataSets.

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

  		  @Override
  		  public ApplicationSpecification configure() {
   			return ApplicationSpecification.Builder.with()
      		  .setName("access-log-3")
      		  .setDescription("Page view analysis")
      		  // Ingest data into the app via Streams
      		  .withStreams()
        	  .add(new Stream("app3-log-events"))
      		  // Store processed data in DataSets
     		  .withDataSets()
        		.add(new PageViewStore("page-views"))
      		  // Process log events in real-time using Flows
      		  .withFlows()
        		.add(new LogAnalyticsFlow())
      		  // Query the processed data using Procedures
      		  .withProcedures()
        		.add(new PageViewProcedure())
      		  .noMapReduce()
      		  .noWorkflow()
      		  .build();
  		  }


##### Streams for data collection #####

Streams are the primary means for bringing data from external systems into the Continuuity Reactor in real-time.

Data can be written to streams using REST. In this example, a Stream named *app3-log-events* is used to ingest Apache access logs.

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


##### Flows and Flowlets for real-time data processing #####

Data ingested through Streams can be processed in real-time using Flows, which are user-implemented realtime-stream processors.

A Flow is comprised of one or more Flowlets that are wired together as a Directed Acyclic Graph (DAG). Each Flowlet is able to perform custom logic and execute data operations for each individual data object processed.

In the example, two Flowlets are used to process the data:

- *parser*: parses the Apache access log entries coming into the *app3-log-events* Stream. It is implemented by `LogEventParseFlowlet`.
- *page-count*: aggregates aggregate the counts of requested pages viewed from referrer pages. It is implemented by `PageCouFlowlet`.

The *parser* and *page-count* Flowlets are wired together by the Flow implementation class `LogAnalyticsFlow`. In this flow, a user-defined matric, batch execution and hash partition are used. 

- Metrics

   A user-defined metric *logs.noreferrer* is used to count the number of logs whose referrer page uris are "-". Metrics can be retrieved in two ways:
   
   - Exploring Metrics in the Dashboard. 

   - Sending HTTP request - `curl -X GET 'http://localhost:10000/v2/metrics/user/apps/access-log-3/flows/pageview-analytics-flow/flowlets/parser/logs.noreferrer?start=1390587752&count=10'`.
   
- Batch

	In order to increase throughput, a batch of data objects are processed within the same transaction by using Batch annotation. In this example, 10 data objects can be read from the input and porcessed at one time.
	
- Hash partition

	In case of multiple instances of the flowlet, hash partitioning is used to distribute data objects to flowlet instances. The emitting flowlet need specify the partition key for each partitioning. In this example, hash code of referrer page uri is used as partitioning key. Logs with same referrer page uri are sent to one flowlet instance to process. Hash partition prevents the potential write conflicts to increment the counts of requested pages viewed from the same referrer page at the same time. 


##### DataSets for data storage#####

The processed data is stored in a user-defined DataSet named *page-views*. It is built on a table to store the number of requested pages viewed from the referrer pages. The operations provided by *page-views* are listed in the following.

- increamentCount: to increment the count of the requested page viewed from the referrer page by 1.
- getPageCount: to get a map of requested pages to therir counts, where requested pages are viewed from a specified referrer page.
- getCounts: to get the total number of requested pages view from a specified referrer page.

##### Procedures for real-time queries #####

The data in DataSets can be served using Procedures for any real-time querying of the aggregated results. The `AccessLogApp` example has a procedure to retrieve all status codes and counts.

The Reactor supports logging through standard SLF4J APIs. In the procedure, the requested page and their counts returned by *getPageDistribution* method of the *PageViewProcedure* are logged. The log messages emitted by this procedure can be viewed in two ways:
	- All log messages can be viewed in the Dashboard by click the Logs button in the Flow screen.
	- Sending HTTP request - `curl -X GET 'http://localhost:10000/v2/apps/access-log-3/procedures/PageViewProcedure/logs?start=1390542610&stop=1390542615'`.


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

1. Drag and drop the App JAR file (`target/logger3-1.0-SNAPSHOT.jar`) onto your browser window.

	Alternatively, use the *Load App* button found on the *Overview* of the Reactor Dashboard.
	
2. Once loaded, select `access-log-3` from the list. 

	On the app's detail page, click the *Start* button on **both** the *Process* and *Query* lists.
	
Command line tools are also available to deploy and manage apps. From within the project root:

1. To deploy the App JAR file, run `$ bin/deploy --app target/logger3-1.0-SNAPSHOT.jar [--gateway <hostname>]`.
2. To start the App, run `$ bin/logger-app --action start [--gateway <hostname>]`.

##### Running the example ####

###### Injecting Apache access log entries into the App ######

Running this script will inject Apache access log entries from the log file `resources/apache.accesslog` to a Stream named *app3-log-events* in the `AccessLogApp`:

`$ bin/inject-log [--gateway <hostname>]`

###### Query ######

There are two ways to query the *page-views* DataSet:

1. Send a query via an HTTP request using the `curl` command. For example:
`curl -X POST -d '{"page":"http://www.continuuity.com"}' 'http://localhost:10000/v2/apps/access-log-3/procedures/PageViewProcedure/methods/get-dist' `
2. Type a procedure method name, in this case `get-counts`, in the Query page of the Reactor Dashboard:

	In the Continuuity Reactor Dashboard:
	- Click the *Query* button.
	- Click on the *PageViewProcedure* procedure.
	- Type `get-dist` in the *Method* text box.
	- Type `{"page":"http://www.continuuity.com"}` in the PARAMETERS text box.
	- Click the *Execute* button.
	- The result of the distribution of the pages viewed from a specified referrer page is displayed in the dashboard:
	
		{"/careers":0.15,"/enterprise":0.45,"/developers":0.20,"/products":0.20}

 
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
