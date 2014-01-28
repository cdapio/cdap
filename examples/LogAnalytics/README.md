
### Overview

This example focuses on a simple real-time streaming log analytics use-case - computing number of occurrences of each HTTP status code processing real-time apache access log data. The example will introduce basic constructs of Continuuity Reactor programming paradigm, namely, Streams, Flows, Procedures, Datasets and Applications.


In any real-time application there are four distinct areas
a) Data Ingest - how to get relevant signals needed by the real-time application for processing
b) Data Processing - the application logic to process to signals into meaningful analytics
c) Data Storage - concerns with saving the computed analytics in appropriate data structures 
d) Queries - serving the computed data to any down-stream application that requires to use the data.


The reactor programming model an application is bundled with following abstractions each covering the four areas mentioned above.


**Streams for Data ingest**


Streams are primary means for bringing data from external systems into the Reactor in real-time. Data can be written to streams using REST. In the example a Stream called log-events is used to ingest apache logs. The Streams are configured to ingest data in the following apache log format: Sample event 
210.160.252.167 - - [09/Jan/2014:21:28:53 -0400] "GET /index.html HTTP/1.1" 200 225 "http://continuuity.com"

Reading from left to right, this format contains the following information about the request:

	the source IP address
	the client's identity
	the remote username (if using HTTP authentication)
	the date, time, and time zone of the request
	the actual content of the request
	the server's response code to the request
	the size of the data block returned to the client, in bytes
	the referrer HTTP request header
	the user-agent HTTP request header

**Flows for real-time data processing**


Data ingested through Streams can be processed in real-time using Flows, which are user-implemented realtime-stream processors. A flow is comprised of one or more flowlets that are wired together as a Directed Acyclic Graph (DAG). Each flowlet is able to perform custom logic and execute data operations for each individual data object processed. In the example two flowlets are used to process the data
- Parser :  to parse the apache access logs coming into log-events stream
- Counter: to aggregate the logs by HTTP status code

The parse and counter flowlets are wired together using a flow named "log-analytics-flow"


**DataSets for data storage**
In the example, the processed data is stored in a Table using table datasets. The computed analytics - count of each HTTP status code is stored on a row called "status" with the HTTP status code as the column key and the count as column value. e


**Procedures for real-time queries**

The data stored in datasets can be served using Procedures for any real-time quering of the aggregated results. The
logger example has a procedure to retrieve all status codes and counts. There are three ways to query datasets.

1. Type the procedure method name, get-counts, in the Query page of Reactor Dashboard, as shown in the Query section.
2. Send the query via a HTTP request by a curl command. For example, `curl -v -X POST 'http://localhost:10000/v2/apps/accesslog/procedures/LogProcedure/methods/get-counts'`

**Logger application**

All the components mentioned above streams, datasets, flows and procedures can be tied together as a deployable entity
called Application.

### Building Logger
From the project root, build Logger with the following [Apache Maven](http://maven.apache.org/) command:

`$ mvn clean package`

### Deploying and Running the App
Make sure an instance of the Continuuity Reactor is running and available. From within the SDK root directory,
the following command will start Reactor in local mode:

`$ bin/continuuity-reactor start`

From within the Reactor Dashbaord ([http://localhost:9999/](http://localhost:9999/) in local mode),

1. Drag and drop the App JAR (target/logger-1.0-SNAPSHOT.jar) onto your browser window. Alternatively, use the
"Load App" button found on the Overview.
2. Once loaded, select accesslog from the list. On the App detail page, click 'Start' on both the Process list and
Query list.

Command line tools are also available. From within the project root,

1. To deploy the App JAR file, run `$ bin/deploy --app target/logger-1.0-SNAPSHOT.jar`
2. To start the App, run `$ bin/logger-app --action start [--gateway <hostname-port>]`

### Stop the App
Either of the following ways stops the running app.

1. On the App detail page, click 'Stop' on both the Process list and Query list.
2. Run `$ bin/logger-app --action stop [--gateway <hostname-port>]`

### Inject Apache access logs into the App
The injected Apache access logs come from the site [http://notebook.cowgar.com/access.log](http://notebook.cowgar.com/access.log). Running the following command will inject Apache access logs to a Stream
named *log-events* in the Logger App from the log file `src/test/resources/apache.accesslog`.

`$ bin/inject-log [--gateway <hostname-port>]`

### Query

1. Click Query.
2. Click the *LogProcedure* procedure.
3. Type `get-counts` in the METHOD text box.
4. Click EXECUTE.
5. The results of the occurrences of each HTTP status code are displayed in the dashboard in JSON format:

		{"200":21, "301":1,"404":19}
