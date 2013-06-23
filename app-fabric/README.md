Continuuity App Fabric
=======================

Runtime management component of a **Reactor**

First there is an overview of all of the high-level concepts and core Java APIs.  Then a deep-dive into the Flow System, Procedure System, Datasets, and Transactions will give an understanding of how these systems function. Finally an example application will be implemented to help illustrate these concepts and describe how an entire application is built.

Core APIs
==========
Application  An application is a collection of streams, flows, datasets, and procedures. To create an application you implement the Application interface and its configure() method.  This allows you to specify the application metadata, declare and configure the streams and datasets used, and add the associated flows and procedures.

<pre>
public class MyApplication implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("myApp")
      .setDescription("my sample application”)	
      .withStreams().add(...) ...
      .withDataSets().add(...) ...
      .withFlows().add(...) ...
      .withProcedures().add(...) ...
      .build();
	You can also specify that an application does not use a stream:
      .setDescription("my sample application”)	
      .noStream().
      .withDataSets().add(...) ...
</pre>

and similarly for all of the other constructs. Stream	Streams are the primary means for pushing data into the AppFabric.  You can specify a stream in your application as follows:
<pre>
      .withStreams().add(new Stream(“myStream”)) ...
</pre>

Flow	Flows are a collection of connected flowlets, wired into a DAG. To create a flow you implement the Flow interface and it’s configure() method. This allows you to specify the flow’s metadata, flowlets, flowlet connections, stream to flowlet connections, and any datasets used in the flow using a FlowSpecification:

<pre>
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
</pre>

Flowlet	The basic building blocks of a flow, flowlets represent each individual processing node within a flow.  Flowlets consume data objects from their inputs and execute custom logic on each data object, allowing you to perform data operations as well as emit data objects to the flowlet’s outputs. Flowlets also specify an initialize() method, which is executed at the startup of each instance of a flowlet before it receives any data. 

The example below shows a flowlet that reads Double values, rounds them and emits the results.  It has a very simple configuration method, and does nothing for initialization and destruction (see below for a simpler way to declare these methods):

<pre>
class RoundingFlowlet implements Flowlet {

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with().
      setName("round").
      setDescription("a rounding flowlet").
      build();
  }

  @Override
  public void initialize(FlowletContext context) throws FlowletException {
  }

  @Override
  public void destroy() {
  }
</pre>

The most interesting method of this flowlet is round(). It is the method that does the actual processing. It uses an output emitter to send data to its output. This is the only way that a flowlet can emit output: 
<pre>
  OutputEmitter<Long> output;

  @ProcessInput
  public void round(Double number) {
    output.emit(Math.round(number));
  }
</pre>

**Note** that the flowlet declares the `OutputEmitter` but does not initialize it: The flow system injects its implementation at runtime. Note also that the method is annotated with @ProcessInput –  this tells the flow system that this method can process input data. Another way to define this method is to have its name start with process – in which case no annotation is needed:
<pre>
  public void processDouble(Double number) {
    output.emit(Math.round(number));
  }
</pre>

You can also overload the process method of a flowlet by adding multiple methods with different input types. When an input object comes in, the flowlet will call the method that matches the objects type.

<pre>
  OutputEmitter<Long> output;

  @ProcessInput
  public void round(Double number) {
    output.emit(Math.round(number));
  }
  @ProcessInput
  public void round(Float number) {
    output.emit((long)Math.round(number));
  }
</pre>

If you define multiple process methods, a method can be selected based on the input object’s origin, that is, the name of a stream or the name of an output of a flowlet. A flowlet that emits data can specify this name using an annotation on the output emitter (in the absence of this annotation, the name of the output defaults to “out”):

<pre>
  @Output(“code”)
  OutputEmitter<String> out;
	Data objects emitted through this output can then be directed to a process method by annotating the method with the origin name:
  @ProcessInput(“code”)
  public void tokenizeCode(String text) {
    ... // perform fancy code tokenization
  }
</pre>

A process method can have an additional parameter, the input context. The input context provides information about the input object, such as its origin and the number of times the object has been retried (see Section 4.2 for the retry logic of a flowlet). For example, the following flowlet tokenizes text in a smart way and it uses the input context to decide what tokenizer to use. 

<pre>
  @ProcessInput
  public void tokenize(String text, InputContext context) throws Exception {
    Tokenizer tokenizer;
    // if this failed before, fall back to simple white space
    if (context.getRetryCount() > 0) {
      tokenizer = new WhiteSpaceTokenizer();
    }
    // is this code? If it’s origin is named “code”, then assume yes
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
</pre>

Type Projection	Flowlets perform an implicit projection on the input objects if they do not match exactly what the process method accepts as arguments. This allows you to write a single process method that can accept multiple compatible types. For example, if you have a process method:

<pre>
@ProcessInput
count(String word) {
  ...
}
</pre>

and you send data of type long to this flowlet, then that type does not exactly match what the process method expects. You could now write another process method for long numbers: 

<pre>
@ProcessInput
count(Long number) {
  count(number.toString());
}
</pre>

and you could do that for every type that you might possibly want to count, but that would be rather tedious. Type projection does this for you automatically: If no process method is found that matches the type of an object exactly, it picks a method that is compatible with the object. In this case, because long can be converted into a String, it is compatible with the original process method. Other compatibilities include:

  * Every primitive type that can be converted to a string is compatible with String. 
  * Any numeric type is compatible with numeric types that can represent it. For example, int is compatible with long, float and double, and long is compatible with float and double, but long is not compatible with int because int cannot represent every long value. 
  * A byte array is compatible with a ByteBuffer and vice versa.
  * A collection of type A is compatible with a collection of type B, if A is compatible with B. Here, a collection can be an array or any Java Collection. Hence, a List<Integer> is compatible with a String[] array.
  * Two maps are compatible if their underlying types are compatible. For example, a TreeMap<Integer, Boolean> is compatible with a HashMap<String, String>.
  * Other Java objects can be compatible if their fields are compatible. For example, in the following class Point is compatible with Coordinate, because all common fields between the two classes are compatible. When projecting from Point to Coordinate, the color field is dropped, whereas the projection from Coordinate to Point will leave the color field as null.

<pre>
class Point {
  private int x;
  private int y;
  private String color;
}

class Coordinates {
  int x;
  int y;
}   
</pre>

Type projections help you keep your code generic and reusable. They also interact well with inheritance: If a flowlet can process a specific object class, then it can also process any subclass of that class. Stream Event	A stream event is a special type of object that comes in via streams. It consists of a set of headers represented by a map from string to string, and a byte array as the body of the event. To consume a stream with a flow, define a flowlet that processes data of type StreamEvent:
<pre>
  class StreamReader extends AbstractFlowlet {
    ...
    public void processEvent(StreamEvent event) {
      ...      
    }
</pre>

Generator	A special case of a flowlet is a generator flowlet. The difference from a standard flowlet is that a generator has no inputs. Instead of a process method, it defines a generate() method to emit data. This can be used, for instance, to generate test data, or connect to an external data source and pull data from there.

For example, the following generator flowlet emits random numbers. It extends the AbstractFlowlet class that provides simple default implementations of the configure, initialize and destroy methods:

<pre>
class RandomGenerator extends AbstractFlowlet implements GeneratorFlowlet {

  private OutputEmitter<Double> output;
  private Random random = new Random();

  @Override
  public void generate() {
    this.output.emit(random.nextDouble());
  }
}
</pre>

Because this generator flowlet has an output of type Double, it can be connected to a DoubleRoundingFlowlet, which has a process method that accepts Double. 

Connection	There are multiple ways to connect the flowlets of a flow. The most common form is to use the flowlet name. Because the name of each flowlet defaults to its class name, you can do the following when building the flow specification:

<pre>
      .withFlowlets()
        .add(new RandomGenerator())
        .add(new RoundingFlowlet())
      .connect()
        .fromStream(“RandomGenerator”).to(“RoundingFlowlet”)
</pre>

If you have two flowlets of the same class you can give them explicit names:

<pre>
      .withFlowlets()
        .add(“random”, new RandomGenerator())
        .add(“generator”, new RandomGenerator())
        .add(“rounding”, new RoundingFlowlet())
      .connect()
        .fromStream(“random”).to(“rounding”)
</pre>

Procedure	Procedures are a way to make calls into the AppFabric from external systems and perform arbitrary server-side processing on-demand. 

To create a procedure you implement the Procedure interface, or more conveniently, you extend the  AbstractProcedure class.  It is configured and initialized similarly to a flowlet but instead of a process method, you define a handler method that, given a method name and map of string arguments, translated from an external request, sends a response. The most generic way to send a response is to obtain a Writer and stream out the response as bytes. You should make sure to close the writer when you are done:

<pre>
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
</pre>

This uses the most generic way to create the response, which allows you to send arbitrary byte content as the response body. In many case, you will actually respond in JSON. Procedures have a convenience methods for this:

<pre>
    // return a JSON map 
    Map<String, Object> results = new TreeMap<String, Object>();
    results.put("totalWords", totalWords);
    results.put("uniqueWords", uniqueWords);
    results.put("averageLength", averageLength);
    responder.sendJson(new ProcedureResponse(Code.SUCCESS), results);
</pre>

There is also a convenience method to respond with an error message:

<pre>
  @Handle("getCount")
  public void getCount(ProcedureRequest request, ProcedureResponder responder) {
    String word = request.getArgument("word");
    if (word == null) {
      responder.error(Code.CLIENT_ERROR, 
                      "Method 'getCount' requires argument 'word'");
      return;
    }
</pre>

Dataset	Datasets are the way you store and retrieve data. If your application uses a dataset, then you must declare it in the application specification. For example, to specify that your application uses a KeyValueTable dataset named “myCounters”, you write: 

<pre>
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      ...
      .withDataSets().add(new KeyValueTable(“myCounters”))
      ...
</pre>
In order to use the dataset inside a flowlet or a procedure, you rely on the runtime system to inject an instance of the dataset. You do that with an annotation:
<pre>
Class myFowlet extends AbstractFlowlet {
  
  @UseDataSet("myCounters")
  private KeyValueTable counters;
  ...
  void process(String key) throws OperationException {
    counters.increment(key.getBytes());
  }	
</pre>
The runtime system reads the dataset specification for the key/value table named “myCounters” from the metadata store and injects a functional instance of the dataset class into your code. You can also implement your own datasets by extending the DataSet base class or extending any other existing type of dataset. The AppFabric supports logging from flows and procedures using standard SLF4J APIs. For instance, in a flowlet you can write:

<pre>
  private static Logger LOG = LoggerFactory.getLogger(WordCounter.class);
  ...
  public void process(String line) {
    LOG.debug(this.getContext().getName() + ": Received line " + line);
    ... // processing
    LOG.debug(this.getContext().getName() + ": Emitting count " + wordCount);
    output.emit(wordCount);
  }
</pre>
The log messages emitted by your application code can be viewed in two different ways: 
  * All log messages of an application can be viewed in the UI, by clicking on the “Logs” button in the Flow and Procedure screens.
  * In the local AppFabric, application log messages are also written to the system log files along with the messages emitted by the AppFabric itself. These files are not available for viewing in the developer sandbox.   
  
The Flow System
===============
Flows are user-implemented real-time stream processors.  They are comprised of one or more flowlets that are wired together into a DAG (Directed Acyclic Graph). Flowlets pass data between one another; each flowlet is able to perform custom logic and execute data operations for each individual data object it processes. 

A flowlet processes the data objects from its input one by one. If a flowlet has multiple inputs, they are consumed in a round-robin fashion. When processing a single input object, all operations, including the removal of the object from the input, and emission of data to the outputs, are executed in a transaction. This provides us with ACID properties, and helps assure a unique and core property of the flow system: It guarantees atomic and exactly-once-processing of each input object by each flowlet in the DAG. 

Sequential and Asynchronous Flowlet Execution
----------------------------------------------
A flowlet processes the data from its inputs one object at a time, by repeating the following steps:

  * An object is dequeued from one of the inputs. This does not completely remove it from the input, but marks it as in-progress. 
  * The matching process() method is selected and invoked, in a new transaction. The process method can perform dataset operations and emit data to its outputs.
  * Once the process() method returns, the transaction is committed, and:
     * If the transaction fails, all the dataset operations are rolled back, and all emitted output data objects is discarded. The onFailure() callback of the flowlet is invoked, which allows you to retry or ignore
     * If you retry, process() will be invoked with the same input object again
     * If you ignore, the failure will be ignored and the input object is permanently removed from the input.
  * If the transaction succeeds, all dataset operations are persistently committed, all emitted output data is sent downstream, and the input object is permanently removed from the input. Then the onSuccess() callback of the flowlet is invoked
By default, steps 1, 2 and 3 above happen in sequence, one input object at a time. You can increase the throughput of the flowlet by declaring it as asynchronous: 

<pre>
@Async
class MyFlowlet implements Flowlet { 
  ...
</pre>

Then the three steps happen concurrently: While the transaction of one data object is committing, the flowlet already processes the next object from the input, and yet another object is already being read from the input at the same time. However, you should be aware that processing now happens in overlapping transactions, and the probability of write conflicts increases – especially if the duration of the transactions is long (The transaction system has special ways to avoid conflicts on the flowlet’s inputs).

Flows & Instances
-----------------
You can have one or more instances of any given flowlet, each consuming a disjoint partition of each input. You can control the number of instances programmatically, using the UI, or using the command line interface. This enables you to shape your application to meet capacity at runtime the same way you will do in production. In the single-node version provided with the Developer Suite, multiples instances of a flowlet are run in threads, so in some cases actual performance may not be affected. In production, each instance is a separate JVM with independent compute resources.


Getting Data In
---------------
Input data can be pushed to a flow using streams or pulled from within a flow using a generator flowlet.
  * A Generator Flowlet actively retrieves or generates data, and the logic to do so is coded into the flowlet’s generate() method. For instance, a generator flowlet can connect to the Twitter fire hose or another external data source.  
  * A Stream is passively receiving events from outside (remember that streams exist outside the scope of a flow). To consume a stream, connect the stream to a flowlet that implements a process method for StreamEvent. This is useful when your events come from an external system that can push data using REST calls. It is also useful when you’re developing and testing your app, because your test driver can send mock data to the stream that covers all your test cases. 

