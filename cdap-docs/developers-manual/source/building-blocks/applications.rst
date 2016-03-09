.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _applications:

============
Applications
============

.. highlight:: java

An *Application* is a collection of building blocks that read and write data through the data
abstraction layer in CDAP. 

*Applications* are composed from *Programs,* *Services,* and *Schedules*.

Programs include :doc:`Flows <flows-flowlets/index>`, :doc:`MapReduce programs <mapreduce-programs>`,
:doc:`Workflows <workflows>`, :doc:`Spark programs <spark-programs>`, and :doc:`Workers <workers>` are used to process
data. :doc:`Services <services>` are used to serve data.

**Data abstractions** include :doc:`Streams <streams>` and :doc:`Datasets <datasets/index>`.

*Applications* are created using an :doc:`Artifact <artifacts>` and optional configuration.
An *Artifact* is a JAR file that packages the Java Application class that defines how the
*Programs*, *Services*, *Schedules*, *Streams*, and *Datasets* interact.
It also packages any dependent classes and libraries needed to run the *Application*. 

Implementing an Application Class
=================================
To implement an application class, extend the ``AbstractApplication`` class,
specifying the application metadata and declaring and
configuring each of the application components::

  public class MyApp extends AbstractApplication {
    @Override
    public void configure() {
      setName("myApp");
      setDescription("My Sample Application");
      addStream(new Stream("myAppStream"));
      createDataset("myAppDataset", Table.class);
      addFlow(new MyAppFlow());
      addService(new MyService());
      addMapReduce(new MyMapReduce());
      addWorkflow(new MyAppWorkflow());
    }
  }

Notice that *Streams* are defined using the provided ``Stream`` class, and *Datasets* are
defined by passing a ``Table`` class; both are referenced by name.

Other components are defined using user-written classes that implement correspondent
interfaces and are referenced by passing an object, in addition to being assigned a unique
name.

Names used for streams and datasets need to be unique across the CDAP namespace, while
names used for programs and services need to be unique only to the application.

A Typical CDAP Application Class
================================
A typical design of a CDAP application class consists of:

- *Streams* to ingest data into CDAP;
- *Flows*, consisting of *Flowlets* linked together, to process the ingested data
  in real time or batch;
- *MapReduce programs*, *Spark programs*, and *Workflows* for batch processing tasks;
- *Workers* for processing data in an ad-hoc manner that doesn't fit into real-time or batch paradigms
- *Datasets* for storage of data, either raw or the processed results; and
- *Services* for serving data and processed results.

Of course, not all components are required: it depends on the application. A minimal
application could include a stream, a flow, a flowlet, and a dataset. It's possible a
stream is not needed, if other methods of bringing in data are used. In the next pages,
we'll look at these components, and their interactions.

Application Configuration
=========================
Application classes can use a ``Config`` class to receive a configuration when an Application is created.
For example, configuration can be used to specify |---| at application creation time |---| a stream to be created or
a dataset to be read, rather than having them hard-coded in the ``AbstractApplication``'s ``configure`` method.
The configuration class needs to be the type parameter of the ``AbstractApplication`` class.
It should also extend the ``Config`` class present in the CDAP API. The configuration is provided as part of the
request body to create an application. It is available during
configuration time through the ``getConfig()`` method in ``AbstractApplication``.

Information about the RESTful call is available in the :ref:`Lifecycle HTTP RESTful API documentation <http-restful-api-lifecycle>`.

We can modify the ``MyApp`` class above to take in a Configuration ``MyApp.MyAppConfig``::

  public class MyApp extends AbstractApplication<MyApp.MyAppConfig> {

    public static class MyAppConfig extends Config {
      String streamName;
      String datasetName;

      public MyAppConfig() {
        // Default values
        this.streamName = "myAppStream";
        this.datasetName = "myAppDataset";
      }
    }

    @Override
    public void configure() {
      MyAppConfig config = getConfig();
      setName("myApp");
      setDescription("My Sample Application");
      addStream(new Stream(config.streamName));
      createDataset(config.datasetName, Table.class);
      addFlow(new MyAppFlow(config));
      addService(new MyService(config.datasetName));
      addMapReduce(new MyMapReduce(config.datasetName));
      addWorkflow(new MyAppWorkflow());
    }
  }

In order to use the configuration in programs, we pass it to individual programs using their constructor. If
the configuration parameter is also required during runtime, you can use the ``@Property`` annotation.
In the example below, the ``uniqueCountTableName`` is used in the ``configure`` method to register the
usage of the dataset. It is also used during the runtime to get the dataset instance using ``getDataset()`` method::

  public class UniqueCounter extends AbstractFlowlet {
    @Property
    private final String uniqueCountTableName;

    private UniqueCountTable uniqueCountTable;

    @Override
    public void configure(FlowletConfigurer configurer) {
      super.configure(configurer);
      useDatasets(uniqueCountTableName);
    }

    public UniqueCounter(String uniqueCountTableName) {
      this.uniqueCountTableName = uniqueCountTableName;
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      uniqueCountTable = context.getDataset(uniqueCountTableName);
    }

    @ProcessInput
    public void process(String word) {
      this.uniqueCountTable.updateUniqueCount(word);
    }
  }

Application Example
===================
Applications are included in just about every CDAP :ref:`application <apps-and-packs>`,
:ref:`tutorial <tutorials>`, :ref:`guide <guides-index>` or :ref:`example <examples-index>`.

An example demonstrating the usage of a configuration is the :ref:`WordCount example <examples-word-count>`.
