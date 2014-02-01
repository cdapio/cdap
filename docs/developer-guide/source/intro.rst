.. :Author: John Jackson
   :Description: Introduction to Continuuity Reactor

===================================
Introduction to Continuuity Reactor
===================================

.. reST Editor: section-numbering::

.. reST Editor: contents::

Big Data Applications
=====================
Big data applications are applications built to process unpredictable volumes and velocity of data. Building big data applications is challenging on many fronts — the following section will introduce some of the challenges in building big data applications.

Challenges in Building Big Data Applications
--------------------------------------------

Challenges in building big data applications are manifold.

#. Expanding application development concerns in application and infrastructure level

   One of the main challenges in building a Big Data application is that an application developer
   has to focus not just on the application layer of code—the business logic—but also on the infrastructure layer,
   making technical decisions about the underlying technology frameworks, which ones to use and how to integrate
   them together into an effective application.

#. Steeper learning curve

   Broadly speaking, the application developer will be concerned with developing four areas:

   #. Data collection framework
   #. Data processing framework
   #. Data storage framework
   #. Data serving framework

   There are numerous technical choices in each of these four areas; understanding
   and evaluating the pros and cons of each of the technical areas and learning them
   to effectively build an application requires climbing a very steep learning curve.

#. Various integration points

   Putting together technologies to collect, process, store and serve data
   using different infrastructure components would require a huge integration
   efforts from the application developer. Each of the components come with
   their own APIs making it harder to integrate them quickly.

#. Operability of different infrastructure components

   Operability of each of the infrastructure components presents its own set
   of challenges in developing monitoring and alerting solutions across different technology stacks. 

#. Lack of development tools 

   Big data application development involves dealing with many distributed system components,
   and there is no development framework to make it easy to develop, test and debug these applications.
   The lack of tools makes it especially difficult to debug applications in a distributed environment.


Architecture Comparison: Building a Log Analytic Application
============================================================
Consider a problem of building a real­time log analytic application that takes access log from Apache™ servers and computes simple analyses on the logs—such as computing throughput per second, error rates, finding the top referral sites.

Traditional Database Log Analysis Framework
-------------------------------------------
A traditional architecture that is not based on *Apache™ Hadoop®* will involve using a log collector that gathers logs from different application servers and then writes to a database, either flat-file or relational. A reporting framework acts as the processing layer to crunch the log signals into meaningful statistics and information.

The disadvantages of this approach include:

- Complexity of the application increases when processing large volumes of data
- The architecture will not be horizontally scalable
- Producing results in a real­time at high volume rate will prove quite challenging

.. image:: /doc-assets/_images/ArchitectureDiagram_1.png

Real­time Apache™ Hadoop®-based Log Analysis Framework
------------------------------------------------------
To achieve horizontal scalability, the database architecture of the preceding design has evolved to include scalable log collection, 
processing and storage layers. One of the most commonly used architectural patterns consists of *Apache Kafka* as the distributed log collection framework, *Storm* as the data processing layer, *Apache™ HBase™* as the storage layer of results and a custom serving layer reading the computed results for visualization by a presentation layer.

The disadvantages of this approach include:

- Need to integrate different systems
- Operability of the different software stack
- No single unified architecture

.. image:: /doc-assets/_images/ArchitectureDiagram_2.png

Continuuity Reactor Log Analysis Framework
------------------------------------------
Using **Continuuity Reactor™** introduces a clear separation between infrastructure components and application code. Reactor functions as a middle-tier application platform which exposes simple high level abstractions to perform data collection, processing, storage and serving. There is a single unified architecture to perform these four tasks, with interoperability designed into the framework. Horizontal scalability is derived from the underlying *Apache Hadoop* layer, while the **Continuuity Reactor** APIs reduce the application complexity and development time. 

.. image:: /doc-assets/_images/ArchitectureDiagram_3.png

Continuuity Reactor Overview
============================
**Continuuity Reactor** is a Java-based, integrated data and application framework that layers on top of Apache Hadoop®, Apache HBase, and other Hadoop ecosystem components. It surfaces the capabilities of the underlying infrastructure through simple Java and REST interfaces. Rather than piecing together different open source frameworks and runtimes to assemble a Big Data infrastructure stack, the Reactor provides an integrated platform that makes it easy to create the different elements of your Big Data application: collecting, processing, storing, and querying data. Data can be collected and stored in both structured and unstructured forms, processed in real-time or in batch, and then the results can be made available for retrieval and visualization.

.. [DOCNOTE: Describe distinction between API and Runtime]

Continuuity Reactor constitutes of both an elastic runtime application and a set of APIs for talking to the runtime and developing distributed Big Data applications.

Continuuity Reactor aims to reduce the time it takes to create and implement applications by hiding the complexity of these technologies with a set of powerful and simple APIs.

Reactor provides four basic abstractions:

- **Streams** for real-time data collection from any external system;
- **Processors** for performing elastically scalable, real-time stream or batch processing;
- **DataSets** for storing data in simple and scalable ways without worrying about formats and schema; and
- **Procedures** for exposing data to external systems through interactive queries. 

These are grouped into **Applications** for configuring and packaging.

Applications are built in Java using the Continuuity Core APIs. Once an application is deployed and running, you can easily interact with it from virtually any external system by accessing the streams, data sets, and procedures using the Java APIs, REST or other network protocols.

Introduction to Reactor Components
==================================

We'll now take a look at the different components of the Reactor API. All Reactor APIs are written in a "fluent" style, and in an IDE, completion of methods will show all the elements required.

Applications
------------

An application is a collection of **Streams**, **DataSets**, **Flows**, **Procedures**, **MapReduce**, and **Workflows**. To create an application, you simply implement the Application interface. Here you specify the application metadata and declare and configure each application element::

	public class MyApp implements Application {
	  @Override
	  public ApplicationSpecification configure() {
	    return ApplicationSpecification.Builder.with()
	      .setName("myApp")
	      .setDescription("my sample app")
	      .withStreams()
	        .add(...) ...
	      .withDataSets()
	        .add(...) ...
	      .withFlows()
	        .add(...) ...
	      .withProcedures()
	        .add(...) ...
	      .withMapReduce()
	        .add(...) ...
	      .withWorkflows()
	        .add(...) ...
	      .build();
	  }
	}

You can specify that an application does not use a particular element. In this code snippet, streams are not used::

		 ...
	      .setDescription("my sample app")
	      .noStream()
		 .withDataSets()
		   .add(...)
		 ...

Data Collection : Streams
-------------------------

**Streams** are the primary means for bringing data from external systems into the Reactor in real time. You can write to Streams either one operation at a time or in batches, using either the Continuuity Reactor HTTP REST API or command line tools. 

Each individual signal sent to a stream is stored as an Event, which is comprised of a header (a map of strings for metadata) and a body (a blob of arbitrary binary data).

Streams are uniquely identified by an ID string and are explicitly created before being used. They can be created programmatically within your application, through the Management Dashboard, or by or using a command line tool. Data written to a Stream can be consumed by Flows and processed in real-time. 

You can specify a stream in your application using::

	.withStreams()
	  .add(new Stream("myStream")) ...

Data Processing: Flows
----------------------

**Flows** are developer-implemented, real-time stream processors. They are comprised of one or more **Flowlets** that are wired together into a directed acyclic graph or DAG. A DAG is a directed graph that does not loop back onto itself. Think of it as the description of steps in a recipe to cook food.

Flowlets pass DataObjects between one another. Each Flowlet is able to perform custom logic and execute data operations for each individual data object processed. All data operations happen in a consistent and durable way.

Flows are deployed to the Reactor and hosted within containers. Each Flowlet instance runs in its own container. Each flowlet in the DAG can have multiple concurrent instances, each consuming a partition of the flowlet’s inputs.

To put data into your Flow, you can either connect the input of the Flow to a Stream, or you can implement a Flowlet to generate data or pull the data from an external source.

Here is an example of a Flow *MyExampleFlow* which references two Flowlets ::

	class MyExampleFlow implements Flow {
	  @Override
	  public FlowSpecification configure() {
	    return FlowSpecification.Builder.with()
	      .setName("mySampleFlow")
	      .setDescription("Flow for showing examples")
	      .withFlowlets()
	        .add("flowlet1", new MyExampleFlowlet())
	        .add("flowlet2", new MyExampleFlowlet2())
	      .connect()
	        .fromStream("myStream").to("flowlet1")
	        .from("flowlet1").to("flowlet2")
	      .build();
	}

Data Processing: Flowlets
-------------------------
**Flowlets**, the basic building blocks of a Flow, represent each individual processing node within a Flow. Flowlets consume data objects from their inputs and execute custom logic on each data object, allowing you to perform data operations as well as emit data objects to the Flowlet’s outputs. Flowlets specify an ``initialize()`` method, which is executed at the startup of each instance of a Flowlet before it receives any data.

The example below shows a Flowlet that reads *Double* values, rounds them, and emits the results. It has a simple configuration method and does nothing for initialization and destruction::

	class RoundingFlowlet implements Flowlet {

	  @Override
	  public FlowletSpecification configure() { 
	    return FlowletSpecification.Builder.with().
	      setName("round").
	      setDescription("a rounding flowlet").
	      build();
	  }

	  @Override
	    public void initialize(FlowletContext context) throws Exception {
	  }

	  @Override
	  public void destroy() { 
	  }


Data Processing: Batch: MapReduce
---------------------------------

**MapReduce** is used to process data in batch. MapReduce jobs can be written as in a conventional Hadoop system. Additionally, Reactor **DataSets** can be accessed from MapReduce jobs as both input and output.

To process data using MapReduce, specify withMapReduce() in your application specification::

	public ApplicationSpecification configure() {
	return ApplicationSpecification.Builder.with()
	  ...
	  .withMapReduce()
	    .add(new WordCountJob())

You must implement the MapReduce interface, which requires the three methods: configure(), beforeSubmit(), and onFinish()::
 
	public class WordCountJob implements MapReduce {
	  @Override
	  public MapReduceSpecification configure() {
	    return MapReduceSpecification.Builder.with()
	      .setName("WordCountJob")
	      .setDescription("Calculates word frequency")
	      .useInputDataSet("messages")
	      .useOutputDataSet("wordFrequency")
	      .build();
	}

Data Processing: Batch: Workflows
---------------------------------

**Workflows** are used to execute a series of MapReduce jobs. A Workflow is given a sequence of jobs that follow each other, with an optional schedule to run the Workflow periodically. On successful execution of a job, the control is transferred to the next job in sequence until the last job in the sequence is executed. On failure, the execution is stopped at the failed job and no subsequent jobs in the sequence are executed.

To process one or more MapReduce jobs in sequence, specify withWorkflows() in your application::

	public ApplicationSpecification configure() {
	  return ApplicationSpecification.Builder.with()
	  ...
	  .withWorkflows()
	    .add(new PurchaseHistoryWorkflow())

You must implement the Workflow interface, which requires the configure() method. Use the addSchedule() method to run a workflow job periodically::

	public static class PurchaseHistoryWorkflow implements Workflow {
	  @Override
	  public WorkflowSpecification configure() {
	    return WorkflowSpecification.Builder.with()
	    .setName("PurchaseHistoryWorkflow")
	    .setDescription("PurchaseHistoryWorkflow description")
	    .startWith(new PurchaseHistoryBuilder())
	    .last(new PurchaseTrendBuilder())
	    .addSchedule(new DefaultSchedule("FiveMinuteSchedule", "Run every 5 minutes",
	                 "0/5 * * * *", Schedule.Action.START))
	    .build();
	   }
	 }
	
Data Storage: DataSets
----------------------

**DataSets** store and retrieve data. DataSets are your interface to the Reactor’s storage capabilities. Instead of requiring you to manipulate data with low-level APIs, DataSets provide higher-level abstractions and generic, reusable Java implementations of common data patterns.

The core DataSet of the Reactor is a Table. Unlike relational database systems, these tables are not organized into rows with a fixed schema. They are optimized for efficient storage of semi-structured data, data with unknown or variable schema, or sparse data.

Other DataSets are built on top of Tables. A DataSet can implement specific semantics around a Table, such as a key/value Table or a counter Table. A DataSet can also combine multiple DataSets to create a complex data pattern. For example, an indexed Table can be implemented by using one Table for the data to index and a second Table for the index itself.

You can implement your own data patterns as custom DataSets on top of Tables. Because a number of useful datasets, including key/value tables, indexed tables and time series are already included with the Reactor, we call them system datasets.

A number of useful DataSets—we refer to them as system DataSets—are included with Reactor, including key/value tables, indexed tables and time series.

For your application to use a DataSet, you must declare it in the ApplicationSpecification. For example, to specify that your application uses a KeyValueTable—a Reactor implementation of DataSet as a key/value table—named “myCounters”, write::

	public ApplicationSpecification configure() {
	  return ApplicationSpecification.Builder.with()
	  ...
	  .withDataSets().add(new KeyValueTable("myCounters"))
	  ...

To use the DataSet in a flowlet or a procedure, instruct the runtime system to inject an instance of the DataSet with the *@UseDataSet* annotation::

	Class MyFowlet extends AbstractFlowlet {
	  @UseDataSet("myCounters")
	  private KeyValueTable counters;
	  ...
	  void process(String key) {
	    counters.increment(key.getBytes());
	  }

The runtime system reads the DataSet specification for “myCounters” from the metadata store and injects a functional instance of the DataSet class.

.. [DOCNOTE: elaborate]

You can implement custom DataSets by extending the DataSet base class or existing DataSet types.

Data Query: Procedures
----------------------

To query the Reactor and its DataSets and retrieve results, you use Procedures.

Procedures allow you to make synchronous calls into the Reactor from an external system and perform server-side processing on-demand, similar to a stored procedure in a traditional database. 

Procedures are typically used to post-process data at query time. This post-processing can include filtering, aggregating, or joins over multiple DataSets—in fact, a procedure can perform all the same operations as a flowlet with the same consistency and durability guarantees. They are deployed into the same pool of application containers as flows, and you can run multiple instances to increase the throughput of requests.

A Procedure implements and exposes a very simple API: a method name (String) and arguments (map of Strings). This implementation is then bound to a REST endpoint and can be called from any external system.

To create a Procedure you implement the Procedure interface, or more conveniently, extend the AbstractProcedure class. A Procedure is configured and initialized similarly to a Flowlet, but instead of a process method you’ll define a handler method. 

Upon external call, the handler method receives the request and sends a response. The most generic way to send a response is to obtain a Writer and stream out the response as bytes. Make sure to close the Writer when you are done::

	class HelloWorld extends AbstractProcedure {
	  @Handle("hello")
	  public void wave(ProcedureRequest request,
	                   ProcedureResponder responder) throws IOException {
	    String hello = "Hello " + request.getArgument("who");
	    ProcedureResponse.Writer writer = 
	      responder.stream(new ProcedureResponse(SUCCESS));
	    writer.write(ByteBuffer.wrap(hello.getBytes())).close();
	  }
	}

Further details about implementing Procedures are in the 
`Continuuity Reactor Programming Guide <programming.html>`_. 

.. include:: includes/footer.rst

