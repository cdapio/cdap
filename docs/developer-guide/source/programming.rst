.. :Author: John Jackson
   :Description: Introduction to Programming Applications for the Continuuity Reactor

===================================================
Continuuity Reactor Programming Guide
===================================================

-----------------------------------------------------------------------
Introduction to Programming Applications for the Continuuity Reactor
-----------------------------------------------------------------------

.. reST Editor: section-numbering::

.. reST Editor: contents::

Introduction
============

This document covers in detail the Continuuity Reactor core elements—Streams, Datasets, Flows, Procedures, MapReduce, and Workflows—and how you work with them in Java to build a Big Data application.

For a high-level view of the concepts of the Continuuity Reactor Java APIs, please see the `Introduction to Continuuity Reactor <intro.html>`_.

.. The implementation of an example is described to illustrate these concepts
.. and show how to build an entire application.

For more information beyond this document, see both the `Javadocs <javadocs>`_  and the code in the `examples <examples>`_ directory, both of which are on the Continuuity.com `Developers website <developers>`_ as well as in your Reactor installation directory.


Conventions
-----------

In this document, *Application* refers to a user Application that has been deployed into the Continuuity Reactor.

Text that are variables that you are to replace is indicated by a series of angle brackets (``< >``). For example::

	PUT /v2/streams/<new-stream-id>

indicates that the text ``<new-stream-id>`` is a variable and that you are to replace it with your value,
perhaps in this case *mystream*::

	PUT /v2/streams/mystream

Writing a Continuuity Reactor Application
-----------------------------------------

Note that the Continuuity Reactor API is written in a 
`"fluent" interface style <http://en.wikipedia.org/wiki/Fluent_interface>`_, 
and relies heavily on ``Builder`` methods for creating many parts of the Application.

In writing a Continuuity Reactor, it's best to use an integrated development environment that understands
the application interface to provide code-completion in writing interface methods.

Using the Reactor Maven Archetype
---------------------------------

To help you, Continuuity has created a Maven archetype to generate a skeleton for your Java project.

`Maven <http://maven.apache.org>`_ is a very popular Java build and dependencies management tool for creating and managing a Java application projects.

This Maven archetype generates a Reactor application Java project with the proper dependencies and sample code as a base to start writing your own Big Data application. To generate a new project, execute the following command::

	$ mvn archetype:generate \
	  -DarchetypeCatalog=https://repository.continuuity.com/content/groups/releases/archetype-catalog.xml \
	  -DarchetypeGroupId=com.continuuity \
	  -DarchetypeArtifactId=Reactor-app-archetype \
	  -DarchetypeVersion=2.0.0

In the interactive shell that appears, specify basic properties for the new project. For example, to create a new project called *MyFirstBigDataApp*, setting appropriate properties, such as your domain and a version identifier::

	Define value for property 'groupId': : com.example
	Define value for property 'artifactId': : MyFirstBigDataApp
	Define value for property 'version': 1.0-SNAPSHOT: : 
	Define value for property 'package': com.example: :
	Confirm properties configuration:
	groupId: com.example
	artifactId: MyFirstBigDataApp
	version: 1.0-SNAPSHOT
	package: org.myorg 
	Y: : Y

After you confirm the settings, the directory ``MyFirstBigDataApp`` is created under the current directory. To build the project::

	$ cd MyFirstBigDataApp
	$ mvn clean package

This creates ``MyFirstBigDataApp-1.0-SNAPSHOT.jar`` in the target directory. This JAR file is a skeleton Reactor application that is ready to deploy to the Continuuity Reactor. Just drag and drop it anywhere on the Reactor Dashboard and it will be deployed. The remainder of this document covers what to put in that JAR file.


Programming APIs
================

.. _Applications:
.. _Application:

Applications
------------

An **Application** is a collection of `Streams`_, `DataSets`_, `Flows`_, `Procedures`_, `MapReduce`_, and `Workflows`_.

To create an Application, implement the ``Application`` interface, specifying the Application metadata and declaring and configuring each of the Application elements::

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

You must specify all of the Continuuity Reactor elements. You can specify that an Application
does not use a particular element, for example, a Stream, by using a ``.no...`` method::

	      ...
	      .setDescription("my sample app")
	      .noStream()
	      .withDataSets()
	        .add(...) ...

and so forth for all of the elements.

All elements must be specified, either using ``.with...`` or ``.no...``.

Notice that in coding the application, *Streams* and *DataSets* are defined using Continuuity classes,
and are referenced by names, while *Flows*, *Flowlets* and *Procedures* are defined using user-written classes
that implement Continuuity classes and are referenced by passing an object, in addition to being assigned a unique name.

Names used for *Streams* and *DataSets* need to be unique across the Reactor instance,
while names used for *Flows*, *Flowlets* and *Procedures* need to be unique only to the application.

.. _streams:

Collecting Data: Streams
------------------------
**Streams** are the primary means for bringing data into the Continuuity Reactor. You specify a Stream in your `Application`_ metadata::

	.withStreams()
	  .add(new Stream("myStream")) ...

specifies a new Stream named *myStream*. Names used for Streams need to be unique across the Reactor instance.

You can write to Streams either one operation at a time or in batches, using either the `Continuuity Reactor HTTP REST API <rest_api_html>`_ or command line tools. 

Each individual signal sent to a Stream is stored as an ``StreamEvent``, which is comprised of a header (a map of strings for metadata) and a body (a blob of arbitrary binary data).

Streams are uniquely identified by an ID string (a "name") and are explicitly created before being used. They can be created programmatically within your application, through the Management Dashboard, or by or using a command line tool. Data written to a Stream can be consumed by Flows and processed in real-time. 

.. _flows:

Processing Data: Flows
----------------------

.. [DOCNOTE: FIXME!]Need better intro here: see Streams above

**Flows** are composed of connected `Flowlets`_ wired into a DAG.

The ``Flow`` interface allows you to specify the Flow’s metadata, `Flowlets`_, 
`Flowlet connections <#connection>`_, `Stream to Flowlet connections <#connection>`_,
and any `DataSets`_ used in the Flow.

To create a Flow, implement ``Flow`` via a ``configure`` method that returns a ``FlowSpecification`` using ``FlowSpecification.Builder()``::

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

In this example, the *name*, *description*, *with* (or *without*) Flowlets, and *connections* are specified before building the Flow.

.. _flowlets:

Processing Data: Flowlets
-------------------------
**Flowlets**, the basic building blocks of a Flow, represent each individual processing node within a Flow. Flowlets consume data objects from their inputs and execute custom logic on each data object, allowing you to perform data operations as well as emit data objects to the Flowlet’s outputs. Flowlets specify an ``initialize()`` method, which is executed at the startup of each instance of a Flowlet before it receives any data.

The example below shows a Flowlet that reads *Double* values, rounds them, and emits the results. It has a simple configuration method and doesn't do anything for initialization or destruction::

	class RoundingFlowlet implements Flowlet {

	  @Override
	  public FlowletSpecification configure() { 
	    return FlowletSpecification.Builder.with().
	      setName("round").
	      setDescription("A rounding Flowlet").
	      build();
	  }

	  @Override
	    public void initialize(FlowletContext context) throws Exception {
	  }

	  @Override
	  public void destroy() { 
	  }

	  OutputEmitter<Long> output;
	  @ProcessInput
	  public void round(Double number) {
	    output.emit(Math.round(number));
	  }


The most interesting method of this Flowlet is ``round()``, the method that does the actual processing. It uses an output emitter to send data to its output. This is the only way that a Flowlet can emit output::

	OutputEmitter<Long> output;
	@ProcessInput
	public void round(Double number) {
	  output.emit(Math.round(number));
	}

Note that the Flowlet declares the output emitter but does not initialize it. The Flow system initializes and injects its implementation at runtime.

The method is annotated with @ProcessInput – this tells the Flow system that this method can process input data.

You can overload the process method of a Flowlet by adding multiple methods with different input types. When an input object comes in, the Flowlet will call the method that matches the object’s type::

	OutputEmitter<Long> output;

	@ProcessInput
	public void round(Double number) {
	  output.emit(Math.round(number));
	}
	@ProcessInput
	public void round(Float number) {
	  output.emit((long)Math.round(number));
	}

If you define multiple process methods, a method will be selected based on the input object’s origin; that is, the name of a Stream or the name of an output of a Flowlet. 

A Flowlet that emits data can specify this name using an annotation on the output emitter. In the absence of this annotation, the name of the output defaults to “out”::

	@Output("code")
	OutputEmitter<String> out;

Data objects emitted through this output can then be directed to a process method of a receiving Flowlet
by annotating the method with the origin name::

	@ProcessInput("code")
	public void tokenizeCode(String text) {
	  ... // perform fancy code tokenization
	}

Input Context
.............
A process method can have an additional parameter, the ``InputContext``. The input context provides information about the input object, such as its origin and the number of times the object has been retried. For example, this Flowlet tokenizes text in a smart way and uses the input context to decide which tokenizer to use::

	@ProcessInput
	public void tokenize(String text, InputContext context) throws Exception {
	  Tokenizer tokenizer;
	  // if this failed before, fall back to simple white space
	  if (context.getRetryCount() > 0) {
	    tokenizer = new WhiteSpaceTokenizer();
	  }
	  // is this code? If its origin is named "code", then assume yes 
	  else if ("code".equals(context.getOrigin())) {
	    tokenizer = new CodeTokenizer();
	  }
	  else {
	    // use the smarter tokenizer
	    tokenizer = new NaturalLanguageTokenizer();
	  }
	  for (String token : tokenizer.tokenize(text)) {
	    output.emit(token);
	  }
	}

Type Projection
...............
Flowlets perform an implicit projection on the input objects if they do not match exactly what the process method accepts as arguments. This allows you to write a single process method that can accept multiple **compatible** types. For example, if you have a process method::

	@ProcessInput
	count(String word) {
	  ... 
	}

and you send data of type ``Long`` to this Flowlet, then that type does not exactly match what the process method expects. You could now write another process method for ``Long`` numbers::

	@ProcessInput count(Long number) {
	count(number.toString());
	}

and you could do that for every type that you might possibly want to count, but that would be rather tedious. Type projection does this for you automatically. If no process method is found that matches the type of an object exactly, it picks a method that is compatible with the object.

In this case, because Long can be converted into a String, it is compatible with the original process method. Other compatible conversions are:

- Every primitive type that can be converted to a ``String`` is compatible with ``String``.
- Any numeric type is compatible with numeric types that can represent it.
  For example, ``int`` is compatible with ``long``, ``float`` and ``double``,
  and ``long`` is compatible with ``float`` and ``double``, but ``long`` is not 
  compatible with ``int`` because ``int`` cannot represent every ``long`` value.
- A byte array is compatible with a ``ByteBuffer`` and vice versa.
- A collection of type A is compatible with a collection of type B,
  if type A is compatible with type B. 
  Here, a collection can be an array or any Java ``Collection``. 
  Hence, a ``List<Integer>`` is compatible with a ``String[]`` array.
- Two maps are compatible if their underlying types are compatible. 
  For example, a ``TreeMap<Integer, Boolean>`` is compatible with a ``HashMap<String, String>``.
- Other Java objects can be compatible if their fields are compatible.
  For example, in the following class ``Point`` is compatible with ``Coordinate``, 
  because all common fields between the two classes are compatible. 
  When projecting from ``Point`` to ``Coordinate``, the color field is dropped, 
  whereas the projection from ``Coordinate`` to ``Point`` will leave the ``color`` field as ``null``::

	class Point {
	  private int x;
	  private int y;
	  private String color;
	}

	class Coordinates { 
	  int x;
	  int y;
	}

Type projections help you keep your code generic and reusable. They also interact well with inheritance. If a Flowlet can process a specific object class, then it can also process any subclass of that class.

Stream Event
............
A Stream event is a special type of object that comes in via Streams. It consists of a set of headers represented by a map from String to String, and a byte array as the body of the event. To consume a Stream with a Flow, define a Flowlet that processes data of type ``StreamEvent``::

	class StreamReader extends AbstractFlowlet {
	  ...
	  @ProcessInput
	  public void processEvent(StreamEvent event) {
	    ... 
	  }

Flowlet Method @Tick Annotation
...............................

A Flowlet’s method can be annotated with @Tick. Instead of processing data objects from a flowlet input, this method is invoked periodically, without arguments. This can be used, for example, to generate data, or pull data from an external data source periodically on a fixed cadence.
In this code snippet from the CountRandom example, the @Tick method in the flowlet emits random numbers::

	public class RandomSource extends AbstractFlowlet { 
	
	  private OutputEmitter<Integer> randomOutput; 
	
	  private final Random random = new Random();
	
	  @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS) 
	  public void generate() throws InterruptedException {
	    randomOutput.emit(random.nextInt(10000));
	  }
	}

Connection
..........
There are multiple ways to connect the Flowlets of a Flow. The most common form is to use the Flowlet name. Because the name of each Flowlet defaults to its class name, when building the flow specification you can simply do::

	.withFlowlets()
	  .add(new RandomGenerator()) 
	  .add(new RoundingFlowlet())
	.connect() 
	  .fromStream("RandomGenerator").to(“RoundingFlowlet”)

If you have two Flowlets of the same class, you can give them explicit names:

	.withFlowlets()
	  .add("random", new RandomGenerator())
	  .add("generator", new RandomGenerator())
	  .add("rounding", new RoundingFlowlet())
	.connect()
	  .fromStream("random").to("rounding")

.. _MapReduce:

Processing Data: MapReduce
--------------------------

To process data using MapReduce, specify ``withMapReduce()`` in your Application specification::

	public ApplicationSpecification configure() {
	return ApplicationSpecification.Builder.with()
	   ...
	   .withMapReduce()
	     .add(new WordCountJob())
	   ...

You must implement the ``MapReduce`` interface, which requires the implementation of three methods:

- ``configure()``
- ``beforeSubmit()``
- ``onFinish()``

::

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

The configure method is similar to the one found in Flow and Application. It defines the name and description of the MapReduce job. You can also specify DataSets to be used as input or output for the job.

The ``beforeSubmit()`` method is invoked at runtime, before the MapReduce job is executed. Through a passed instance of the ``MapReduceContext`` you have access to the actual Hadoop job configuration, as though you were running the MapReduce job directly on Hadoop. For example, you can specify the Mapper and Reducer classes as well as the intermediate data format::

	@Override
	public void beforeSubmit(MapReduceContext context) throws Exception {
	  Job job = context.getHadoopJob();
	  job.setMapperClass(TokenizerMapper.class);
	  job.setReducerClass(IntSumReducer.class);
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(IntWritable.class);
	}

The ``onFinish()`` method is invoked after the MapReduce job has finished. You could perform cleanup or send a notification of job completion, if that was required. Because many MapReduce jobs do not need this method, the ``AbstractMapReduce`` class provides a default implementation that does nothing::

	@Override
	public void onFinish(boolean succeeded, MapReduceContext context) {
	  // do nothing
	}

Continuuity Reactor ``Mapper`` and ``Reducer`` implement the standard Hadoop APIs::

	public static class TokenizerMapper
	    extends Mapper<byte[], byte[], Text, IntWritable> {
	
	  private final static IntWritable one = new IntWritable(1); 
	  private Text word = new Text();
	  public void map(byte[] key, byte[] value, Context context)
	      throws IOException, InterruptedException {
	    StringTokenizer itr = new StringTokenizer(Bytes.toString(value)); 
	    while (itr.hasMoreTokens()) {
	      word.set(itr.nextToken());
	      context.write(word, one);
	    }
	  }
	}
	
	public static class IntSumReducer
	    extends Reducer<Text, IntWritable, byte[], byte[]> {
	
	  public void reduce(Text key, Iterable<IntWritable> values, Context context)
	      throws IOException, InterruptedException {
	    int sum = 0;
	    for (IntWritable val : values) {
	      sum += val.get();
	    }
	    context.write(key.copyBytes(), Bytes.toBytes(sum));
	  }
	}

MapReduce and DataSets
......................

Both Continuuity Reactor ``Mapper`` and ``Reducer`` can directly read from a DataSet or write to a DataSet similar to the way a Flowlet or Procedure can.

To access a DataSet directly in Mapper or Reducer, you need (1) a declaration and (2) an injection :

#. Declare the DataSet in the MapReduce job’s configure() method. 
   For example, to have access to a DataSet named *catalog*::

	public class MyMapReduceJob implements MapReduce {
	  @Override
	  public MapReduceSpecification configure() {
	    return MapReduceSpecification.Builder.with()
	      ...
	    .useDataSet("catalog")
	      ...

#. Inject the DataSet into the mapper or reducer that uses it::

	public static class CatalogJoinMapper extends Mapper<byte[], Purchase, ...> {
	  @UseDataSet("catalog")
	  private ProductCatalog catalog;
	
	  @Override
	  public void map(byte[] key, Purchase purchase, Context context)
	      throws IOException, InterruptedException {
	    // join with catalog by product ID
	    Product product = catalog.read(purchase.getProductId());
	    ...
	  }


.. _Workflows:

Processing Data: Workflows
--------------------------

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
	
If there is only one MapReduce job to be run as a part of a workflow, use the onlyWith() method after setDescription() when building the Workflow::

	public static class PurchaseHistoryWorkflow implements Workflow {

	  @Override
	  public WorkflowSpecification configure() {
	    return WorkflowSpecification.Builder.with() .setName("PurchaseHistoryWorkflow")
	      .setDescription("PurchaseHistoryWorkflow description")
	      .onlyWith(new PurchaseHistoryBuilder())
	      .addSchedule(new DefaultSchedule("FiveMinuteSchedule", "Run every 5 minutes",
	                   "0/5 * * * *", Schedule.Action.START))
	      .build();
	  }
	}

.. _DataSets:

Store Data: DataSets
--------------------
DataSets store and retrieve data. If your Application uses a DataSet, you must declare it in the Application specification. For example, to specify that your Application uses a ``KeyValueTable`` DataSet named *myCounters*, write::

	public ApplicationSpecification configure() { 
	  return ApplicationSpecification.Builder.with()
	    ...
	    .withDataSets().add(new KeyValueTable("myCounters"))
	    ...

To use the DataSet in a Flowlet or a Procedure, instruct the runtime system to inject an instance of the DataSet with the @UseDataSet annotation::

	Class MyFowlet extends AbstractFlowlet {
	  @UseDataSet("myCounters")
	  private KeyValueTable counters; 
	  ...
	  void process(String key) {
	    counters.increment(key.getBytes());
	  }

The runtime system reads the DataSet specification for the key/value table *myCounters* from the metadata store and injects a functional instance of the DataSet class into the Application.
You can also implement custom DataSets by extending the ``DataSet`` base class or by extending existing DataSet types.

.. _Procedures:

Query Data: Procedures
----------------------
Procedures receive calls from external systems and perform arbitrary server-side processing on demand.

To create a Procedure, implement the Procedure interface. More conveniently, the standard design pattern is to extend the ``AbstractProcedure`` class. 

A Procedure is configured and initialized similarly to a Flowlet, but instead of a process method you’ll define a handler method. Upon external call, the handler method receives the request and sends a response. The most generic way to send a response is to obtain a Writer and stream out the response as bytes. Make sure to close the Writer when you are done::

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

This uses the most generic way to create the response, which allows you to send arbitrary byte content as the response body. In many cases, you will actually respond with JSON. A Continuuity Reactor ``ProcedureResponder`` has convenience methods for returning JSON maps::

	// return a JSON map
	Map<String, Object> results = new TreeMap<String, Object>();
	results.put("totalWords", totalWords);
	results.put("uniqueWords", uniqueWords);
	results.put("averageLength", averageLength);
	responder.sendJson(results);

There is also a convenience method to respond with an error message::

	@Handle("getCount")
	public void getCount(ProcedureRequest request, ProcedureResponder responder) {
	  String word = request.getArgument("word"); 
	  if (word == null) {
	    responder.error(Code.CLIENT_ERROR,
	                    "Method 'getCount' requires argument 'word'");
	    return;
	  }

[DOCNOTE: FIXME!] Shouldn't getCount throws IOException?


..  [rev 2]

Testing and Debugging
=====================

Strategies in testing applications
----------------------------------

The Reactor comes with a convenient way to unit test your applications. The base for these tests is ReactorTestBase, which is packaged separately from the API in its own artifact because it depends on the Reactor’s runtime classes. You can include it in your test dependencies in two ways:

- Include all JAR files in the lib directory of the Reactor Development Kit installation.
- Include the continuuity-test artifact in your Maven test dependencies 
  (see the ``pom.xml`` file of the *WordCount* example).

Note that for building an application, you only need to include the Reactor API in your dependencies. For testing, however, you need the Reactor run-time. To build your test case, extend the ``ReactorTestBase`` class. Let’s write a test case for the *WordCount* example::

	public class WordCountTest extends ReactorTestBase {
	  @Test
	  public void testWordCount() throws Exception {


The first thing we do in this test is deploy the application, then we’ll start the flow and the procedure:

	  // deploy the application
	  ApplicationManager appManager = deployApplication(WordCount.class);
	  // start the flow and the procedure
	  FlowManager flowManager = appManager.startFlow("WordCounter");
	  ProcedureManager procManager = appManager.startProcedure("RetrieveCount");

Now that the flow is running, we can send some events to the stream:

	  // send a few events to the stream
	  StreamWriter writer = appManager.getStreamWriter("wordStream");
	  writer.send("hello world");
	  writer.send("a wonderful world");
	  writer.send("the world says hello");

To wait for all events to be processed, we can get a metrics observer for the last flowlet in the pipeline (the word associator) and wait for its processed count to reach 3, or time out after 5 seconds::

	  // wait for the events to be processed, or at most 5 seconds
	  RuntimeMetrics metrics = RuntimeStats.
	    getFlowletMetrics("WordCount", "WordCounter", "associator");
	  metrics.waitForProcessed(3, 5, TimeUnit.SECONDS);

Now we can start verifying that the processing was correct by obtaining a client for the procedure, and then submitting a query for the global statistics:

	  // Call the procedure
	  ProcedureClient client = procManager.getClient();
	  // query global statistics
	  String response = client.query("getStats", Collections.EMPTY_MAP);

If the query fails for any reason this method would throw an exception. In case of success, the response is a JSON string. We must deserialize the JSON string to verify the results::

	  Map<String, String> map = new Gson().fromJson(response, stringMapType);
	  Assert.assertEquals("9", map.get("totalWords"));
	  Assert.assertEquals("6", map.get("uniqueWords"));
	  Assert.assertEquals(((double)42)/9,
	    (double)Double.valueOf(map.get("averageLength")), 0.001);

Then we ask for the statistics of one of the words in the test events. The verification is a little more complex, because we have a nested map as a response, and the value types in the top- level map are not uniform::

	  // verify some statistics for one of the words
	  response = client.query("getCount", ImmutableMap.of("word","world")); 
	  Map<String, Object> omap = new Gson().fromJson(response, objectMapType); 
	  Assert.assertEquals("world", omap.get("word"));
	  Assert.assertEquals(3.0, omap.get("count"));
	  // the associations are a map within the map
	  Map<String, Double> assocs = (Map<String, Double>) omap.get("assocs"); 
	  Assert.assertEquals(2.0, (double)assocs.get("hello"), 0.000001); 
	  Assert.assertTrue(assocs.containsKey("hello"));
	}


Debugging a Continuuity Reactor Application
-------------------------------------------

Any Continuuity Reactor application can be debugged in the Local Reactor by attaching a remote debugger to the Reactor JVM. To enable remote debugging, start the Local Reactor with the ``--enable-debug`` option specifying ``port 5005``.
The Reactor should confirm that the debugger port is open with a meesage such as *Remote debugger agent started on port 5005*.

#. Deploy the *HelloWorld* application to the Reactor by dragging and dropping the ``HelloWorld.jar`` file from the /examples/HelloWorld directory onto the Reactor Dashboard.

#. Open the HelloWorld application in an IDE and connect to the remote debugger. 

For more information, see either Debugging with IntelliJ or Debugging with Eclipse.

Debugging with IntelliJ
.......................

#. From the *IntelliJ* toolbar, select ``Run -> Edit Configurations``.
#. Click ``+`` and choose ``Remote Configuration``:

.. image:: /doc-assets/_images/IntelliJ_1.png

#. Create a debug configuration by entering a name, for example, ``Continuuity``.
#. Enter ``5005`` in the Port field:

.. image:: /doc-assets/_images/IntelliJ_2.png

#. To start the debugger, select ``Run -> Debug -> Continuuity``.
#. Set a breakpoint in any code block, for example, a Flowlet method:

.. image:: /doc-assets/_images/IntelliJ_3.png

#. Start the Flow in the Dashboard.
#. Send an event to the Stream. The control will stop at the breakpoint
   and you can proceed with debugging.


Debugging with Eclipse
......................

#. In Eclipse, select ``Run-> Debug`` configurations.
#. In the pop-up, select ``Remote Java application``.
#. Enter a name, for example, ``Continuuity``.
#. In the Port field, enter ``5005``.
#. Click ``Debug`` to start the debugger:

.. image:: /doc-assets/_images/Eclipse_1.png

#. Set a breakpoint in any code block, for example, a Flowlet method:

.. image:: /doc-assets/_images/Eclipse_2.png

#. Start the flow in the Dashboard.
#. Send an event to the Stream.
#. The control stops at the breakpoint and you can proceed with debugging.

.. Unit testing [rev 2]
.. ------------

.. Local Continuuity Reactor [rev 2]
.. -------------------------

include:: includes/footer.rst
