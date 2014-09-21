.. :author: Cask Data, Inc.
   :description: Advanced Cask Data Application Platform Features
   :copyright: Copyright © 2014 Cask Data, Inc.

===========================
CDAP Application Case Study
===========================

**Web Analytics using CDAP**

Introduction
============
... *More to come* ...

The Wise application - Web Insights Engine application - uses the following CDAP constructs to analyze web server logs:

- **Stream**: Ingests log data in real-time
- **Flow**: Applies transformation on log data in real-time
- **Dataset**: Stores log analytics according to custom data pattern
- **MapReduce**: Processes chunks of log data in batch jobs to extract deeper information
- **Service**: Exposes APIs to query Datasets
- **Explore**: Executes Ad-hoc queries over Datasets


Running Wise
============
Building and running Wise is easy. Simply download the source tarball from the following location::

  url

You can then expand the tarball and build the application by executing::

  $ tar -xzf cdap-wise-0.1.0.tar.gz
  $ cd cdap-wise-0.1.0
  $ mvn package

To deploy the application, first make sure CDAP standalone (*link*) is running, then execute::

  $ bin/deploy.sh

Overview of Wise
================
Throughout this case study, we are going to present and explain the different constructs that the Wise application
uses. Let's first have a look at a diagram showing a overview of the Wise application's architecture.

.. image:: _images/wise_architecture_diagram.png

The Wise application has one Stream - ``logEventStream`` - which receives Apache access logs. It sends the events
it receives to two CDAP components: the ``WiseFlow`` flow and the ``WiseWorkflow`` workflow.

``WiseFlow`` has two flowlets. The first one, ``parser``, extracts information out of the logs received from the
stream. It then sends the information to the second flowlet, ``pageViewCount``, which role is to store
the information in a custom-defined Dataset, ``pageViewCDS``.

``WiseWorkflow`` executes a Map/Reduce job every ten minutes. The input of this job are the events from the stream
which have not yet been processed by the workflow. For each Web page recorded in the access logs, this job counts
the number of time people have bounced from it. A bounce is counted whenever a user's activity stops for a
certain amount of time. The last page they visited is counted as a bounce. This information is stored in a Dataset,
``bounceCounts``.

The Wise application contains a Service, the ``WiseService``. It exposes REST endpoints to query the ``pageViewCDS``
Dataset.

Finally, both the ``pageViewCDS`` and ``bounceCounts`` Datasets expose a SQL interface. They can be queried using
SQL queries through our ``Explore`` module in CDAP dashboard.

All it takes to create the Wise application with those components is to define a class that extends
``AbstractApplication``::

  public class WiseApp extends AbstractApplication {
    @Override
    public void configure() {
      setName("Wise");
      setDescription("Web Insights Engine");
      // The logEventStream stream will receive the access logs
      addStream(new Stream("logEventStream"));
      // Custom Dataset to store basic page view information by IP address
      createDataset("pageViewCDS", PageViewStore.class);
      // Custom Dataset to store bounce information for every web page
      createDataset("bounceCounts", BounceCountsStore.class);
      // Utility Dataset used in the Map/Reduce job
      createDataset("bounceCountsMapReduceLastRun", KeyValueTable.class);
      // Add the WiseFlow flow
      addFlow(new WiseFlow());
      // Add the WiseWorkflow workflow
      addWorkflow(new WiseWorkflow());
      // Add the WiseService service
      addService(new WiseService());
    }
  }

Let's now talk about each of these components in more detail.

Wise Data Patterns
==================
In CDAP, we have put data in the center of everything. Our system Datasets can express common data patterns,
like key-value storage or time series. We also expose simple APIs to let our users define their custom
Datasets, which entirely define the way they want data to be stored and accessed.

Wise has two custom-defined Datasets, ``pageViewCDS`` and ``bounceCounts``. A custom-defined Dataset is backed by
a ``Table``, which is where data will be stored internally. A ``Table`` exposes the same data pattern as an HBase table.
That is, a ``Table`` stores raw keys; each raw key can have as many columns as it wants, with two raw keys having
not necessarily the same columns; and each column contains one value. The difference between a ``Table`` and an
HBase table is that timestamps are not conserved in a ``Table``. Only the latest value of a column will be saved.
Using the Java ``Map`` interface, a ``Table`` can be seen as being of type ``Map<byte[], Map<byte[], byte[]>>``.

Let's first have a look at what an access log looks like::

  47.41.156.173 - - [18/Sep/2014:12:52:52 -0400] "POST /index.html HTTP/1.1" 404 1490 " " "Mozilla/2.0 (compatible; Ask Jeeves)"

Wise is only interested in 3 parts of those logs:

- The IP address, here *47.41.156.173*
- The time the log was saved, *18/Sep/2014:12:52:52 -0400* in this case
- The Web page visited, */index.html* in the example.

The pageViewCDS Dataset
-----------------------
The ``pageViewCDS`` custom Dataset stores for every IP address, the number of times it visited each web page. To do so,
it uses a ``Table`` with the following specifications: the raw key is the IP address, each Web page visited by the IP
address is a column, and the value of each column is the count of visits.
``pageViewCDS`` is defined by the ``PageViewStore`` class. It extends ``AbstractDataset`` and has a constructor
which allows to use a ``Table`` to store the data::

  public class PageViewStore extends AbstractDataset
    ... {

    // Define the underlying table
    private Table table;

    public PageViewStore(DatasetSpecification spec, @EmbeddedDataset("tracks") Table table) {
      super(spec.getName(), table);
      this.table = table;
    }
    ...
  }

This is the common way of defining a custom Dataset. The next step is to define the APIs that this Dataset exposes
to store data and access data. Here is the API for storing data::

  public void incrementCount(LogInfo logInfo) {
    table.increment(new Increment(logInfo.getIp(), logInfo.getUri(), 1L));
  }

``incrememtCount()`` takes a ``LogInfo`` object containing the 3 parts of a log that we are interested about -
IP address, timestamp and web page - and increments the number of visits of the Web page for the given IP address.
We use the underlying ``Table`` API ``increment()`` to store this information.

To access data, we define two APIs to retrieve the total count of visits for a given IP address, and to retrieve
the count of visits per Web page for a given IP address::

  /**
   * Get the count of requested pages viewed from a specified IP address.
   *
   * @param ipAddress an IP address used to look for requested pages counts.
   * @return a map of a requested page URI to its count
   */
  public Map<String, Long> getPageCount(String ipAddress) {
    Row row = this.table.get(Bytes.toBytes(ipAddress));
    Map<String, Long> pageCount = getPageCounts(row);
    return pageCount;
  }

  /**
   * Get the total number of requested pages viewed from a specified IP address.
   *
   * @param ipAddress an IP address used to look for requested pages counts.
   * @return the number of requested pages
   */
  public long getCounts(String ipAddress) {
    Row row = this.table.get(Bytes.toBytes(ipAddress));
    if (row == null || row.isEmpty()) {
      return 0;
    }
    int count = 0;
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      count += Bytes.toLong(entry.getValue());
    }
    return count;
  }

  private Map<String, Long> getPageCounts(Row row) {
    if (row == null || row.isEmpty()) {
      return null;
    }
    Map<String, Long> pageCount = new HashMap<String, Long>();
    for (Map.Entry<byte[], byte[]> entry : row.getColumns().entrySet()) {
      pageCount.put(Bytes.toString(entry.getKey()), Bytes.toLong(entry.getValue()));
    }
    return pageCount;
  }

Those APIs access data using the ``Table.get()`` method, which returns a ``Row`` object containing all the columns
associated to the raw key passed as argument of ``Table.get()``.

To use this Dataset, we simply had to create it in the ``configure()`` method of our ``WiseApp`` class::

  createDataset("pageViewCDS", PageViewStore.class);

We give it an ID as a ``String`` and let our application know which class defines it. In every component of the Wise
application, we will now be able to access the ``pageViewCDS`` Dataset.

The bounceCounts Dataset
------------------------
The ``bounceCounts`` Datasets stores the total number of visits for each Web page, as well as the number
of times users bounced off them. It is defined in the ``BounceCountsStore`` class which also extends
``AbstractDataset`` and is backed by a ``Table`` object. Let's detail the API we use to store this information::

  static final byte[] COL_VISITS = new byte[] { 'v', 'i', 's', 'i', 't', 's' };
  static final byte[] COL_BOUNCES = new byte[] { 'b', 'o', 'u', 'n', 'c', 'e', 's' };

  /**
   * Increment a bounce counts entry with the specified {@code visits} and {@code bounces}.
   *
   * @param uri URI of the Web page.
   * @param visits number of visits to add to the Web page.
   * @param bounces number of bounces to add to the Web page.
   */
  public void increment(String uri, long visits, long bounces) {
    table.increment(Bytes.toBytes(uri),
                    new byte[][] { COL_VISITS, COL_BOUNCES },
                    new long[] { visits, bounces });
  }

Data is stored in the ``Table`` object with the following pattern:

- The raw key is the Web page URI.
- Each raw key has two columns, the byte arrays ``COL_VISITS`` and ``COL_BOUNCES``.
- The ``COL_VISITS`` column stores the total number of visits for the Web page considered.
- The ``COL_BOUNCES`` column stores the number of times users bounced off the Web page.

The ``increment()`` method adds to a Web page a number of `visits` and a number of `bounces`. It uses the
``Table.increment()`` API to do so.

To retrieve the number of `visits` and the number of `bounces` for one Web page, we define a ``get()`` API::

  /**
   * Retrieve a bounce counts entry from this {@link BounceCountsStore}.
   *
   * @param uri URI of the Web page.
   * @return the bounce ratio entry associated to the Web page with the {@code uri}.
   */
  public PageBounce get(String uri) {
    Row row = table.get(Bytes.toBytes(uri), new byte[][] { COL_VISITS, COL_BOUNCES });
    if (row.isEmpty()) {
      return new PageBounce(uri, 0, 0);
    }
    long visits = Bytes.toLong(row.get(COL_VISITS));
    long bounces = Bytes.toLong(row.get(COL_BOUNCES));
    return new PageBounce(uri, visits, bounces);
  }

The ``get()`` API reads the two columns ``COL_VISITS`` and ``COL_BOUNCES`` of a Web page. Once again,
we use the ``Table.get()`` API which returns a ``Row`` object. From the information contained in the ``Row``
object, we build a ``PageBounce`` object containing a ``uri``, a ``visits`` count and a ``bounces`` count::

  public class PageBounce {
    private final String uri;
    private final long totalVisits;
    private final long bounces;

    public PageBounce(String uri, long totalVisits, long bounces) {
      this.uri = uri;
      this.totalVisits = totalVisits;
      this.bounces = bounces;
    }
    ...
  }

We define the ``bounceCounts`` Dataset in the same way that we define the ``pageViewCDS`` Dataset - in the
``configure()`` method of our ``WiseApp`` class, we had the following::

  createDataset("bounceCounts", BounceCountsStore.class);

Ingesting Access Logs in Wise
=============================
CDAP has an extremely easy way to ingest data in real time into an application, using **Streams**. A stream exposes
a simple REST API to ingest data events. An event is composed of a header and a body. The body is a byte array, hence
it can contain anything.

Wise has one stream, ``logEventStream``. This is the data entry point of the Wise application. We inject to this stream
the Apache access logs. This stream is defined in the ``configure()`` method of the ``WiseApp`` class::

  addStream(new Stream("logEventStream"));

The REST API to inject one log into the stream in a running CDAP standalone instance is the following::

  POST http://localhost:10000/v2/streams/logEventStream

We use the ID we gave to the stream - ``logEventStream`` - when we defined it in the ``WiseApp`` class.
The body of the request is the body of the stream. For example, it could be this access log::

  47.41.156.173 - - [18/Sep/2014:12:52:52 -0400] "POST /index.html HTTP/1.1" 404 1490 " " "Mozilla/2.0 (compatible; Ask Jeeves)"

To make it easy for you to inject a lot of sample logs, we have created a script in the ``bin`` directory of the
``Wise`` application. On Unix systems, run::

  $ bin/inject-data.sh

On Windows, run::

  $ bin/inject-data.bat

This requires that a CDAP standalone instance be running with the Wise application already deployed.

Real-time Logs Transformation with WiseFlow
===========================================
*Flows* are a powerful way in CDAP to transform data coming from streams in real-time. A flow is composed of several
flowlets connected in a directed acyclic graph. Each flowlet performs one job on the data it receives and outputs the
results for the next flowlet to process. The last flowlet of a flow usually stores data in a Dataset.

Wise has one flow - ``WiseFlow`` - containing two flowlets, ``parser`` and ``pageViewCount``. A flow is defined as
follows in the ``configure()`` method of an application::

  addFlow(new WiseFlow());

The ``WiseFlow`` class implements the ``Flow`` interface which only consists of one ``configure()`` method::

  public class WiseFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("WiseFlow")
        .setDescription("Wise Flow")
        .withFlowlets()
          .add("parser", new LogEventParserFlowlet())
          .add("pageViewCount", new PageViewCounterFlowlet())
        .connect()
          .fromStream("logEventStream").to("parser")
          .from("parser").to("pageViewCount")
        .build();
    }
    ...
  }

We set the ID of the flow - ``WiseFlow`` - and define the flowlets that compose the flow as well as how they are
connected to each other. The two flowlets ``parser`` and ``pageViewCount`` are defined here. The stream
``logEventStream`` is connected to the flowlet ``parser``, which in turn is connected to the ``pageViewCount``
flowlet. When the Wise application is deployed in CDAP, ``WiseFlow`` has this form:

.. image:: _images/wise_flow.png

The parser Flowlet
------------------
The input of the ``parser`` flowlet is the stream ``logEventStream``. It outputs ``LogInfo`` objects. Here is its
implementation::

  public static class LogEventParserFlowlet extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(LogEventParserFlowlet.class);

    // Emitter for emitting a LogInfo instance to the next Flowlet
    private OutputEmitter<LogInfo> output;

    // Annotation indicates that this method can process incoming data
    @ProcessInput
    public void processFromStream(StreamEvent event) throws CharacterCodingException {

      // Get a log event in String format from a StreamEvent instance
      String log = Charsets.UTF_8.decode(event.getBody()).toString();

      try {
        LogInfo logInfo = LogInfo.parse(log);
        if (logInfo != null) {
          output.emit(logInfo, "ipHash", logInfo.getIp().hashCode());
        }
      } catch (IOException e) {
        LOG.info("Could not parse log event {}", log);
      }
    }
  }

It extends ``AbstractFlowlet`` and contains one method to process the data it receives from ``logEventStream``.
This method can have any name. Here we call it ``processFromStream``. It has to bear the ``@ProcessInput``
annotation indicating that the method will be used to process incoming data. Also, because the ``parser`` flowlet
receives data from a stream, the ``processFromStream`` method has to take one and only one argument of type
``StreamEvent``. A ``StreamEvent`` object contains the header and the body of a stream event. In the Wise application,
the body of a ``StreamEvent`` will be a Apache access log.

The ``parser`` flowlet outputs data as ``LogInfo`` objects. One log line will be parsed into one ``LogInfo`` object.
To output those, an ``OutputEmitter<LogInfo>`` object has to be defined in the flowlet class. This object is then used
to emit ``LogInfo``s to the next flowlet in the flow - ``pageViewCount``.

The pageViewCount Flowlet
-------------------------
As we have seen, the input of the ``pageViewCount`` flowlet is the output of the ``parser`` flowlet. It means that
``pageViewCount`` has to deal with ``LogInfo`` objects. Also, this flowlet's role is to store in the ``pageViewCDS``
Dataset the counts of visits of different Web pages per IP address. Here is how we make that possible::

  public static class PageViewCounterFlowlet extends AbstractFlowlet {

    // UseDataSet annotation indicates the page-views Dataset is used in the Flowlet
    @UseDataSet("pageViewCDS")
    private PageViewStore pageViews;

    // Batch annotation indicates processing a batch of data objects to increase throughput
    // HashPartition annotation indicates using hash partition to distribute data in multiple Flowlet instances
    // ProcessInput annotation indicates that this method can process incoming data
    @Batch(10)
    @HashPartition("ipHash")
    @ProcessInput
    public void count(Iterator<LogInfo> trackIterator) {
      while (trackIterator.hasNext()) {
        LogInfo logInfo = trackIterator.next();

        // Increment the count of a logInfo by 1
        pageViews.incrementCount(logInfo);
      }
    }
  }

Once again, this flowlet extends ``AbstractFlowlet``. It contains one method, ``count``, which bear the annotation
``ProcessInput`` indicating that this method can process incoming data. Because it deals with ``LogInfo`` objects,
this method takes one argument of type ``Iterator<LogInfo>``. The reason it takes an ``Iterator`` and not a
``LogInfo`` directly is because the ``@Batch(10)`` annotation allows this flowlet to receive multiple objects
at once.

This flowlet stores information in the ``pageViewCDS`` Dataset. To do so, it references the Dataset in an attribute
of the class using the annotation ``@UseDataset``. Because the Dataset as been defined in the ``configure()`` method
of the ``WiseApp`` class, the ``PageViewCounterFlowlet`` can have access to the Dataset using the same ID. Once it
is obtained, we can use its APIs in the core of the ``count()`` method to store information.

Batch Processing of Logs with WiseWorkflow
==========================================
