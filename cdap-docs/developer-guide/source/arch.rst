.. :author: Cask Data, Inc.
   :description: Architecture of the Cask Data Application Platform
     :copyright: Copyright © 2014 Cask Data, Inc.

========================================================
Cask Data Application Platform Architecture and Concepts
========================================================

Introduction
============

Before you learn how to write applications on CDAP, this section will introduce its concepts and architecture.

Virtualization
==============

CDAP lets you virtualize your data and applications by injecting abstraction layers over the various components
of the Hadoop ecosystem. To access and manipulate data, you interact with CDAP's Datasets rather than actual
storage engines such as HDFS or HBase. Similarly, you write your applications using CDAP's application interfaces
and run them inside application containers. These containers are a logical abstraction that can be realized
differently in several runtime environments, such as in-memory, single-node standalone, or distributed cluster,
without the need to change a single line of application code.

.. image:: _images/layers.png
   :width: 4in
   :align: center

Data Virtualization
-------------------

In CDAP applications, you interact with data through Datasets. Datasets provide virtualization through:

- Abstraction of the actual representation of data in storage. You can write your code or queries without
  having to know where and how your data is stored - be it in HBase, LevelDB or a relational database.
- Encapsulation of data access patterns and business logic as reusable, managed Java code.
- Consistency of your data under highly concurrent access using CDAP's transaction system provoded by
  `Cask Tephra <http://github.com/continuuity/tephra/>`__
- Injection of datasets into different programming paradigms and runtimes: As soon as your data is in a
  dataset, you can instantly use it in real-time programs, batch processing such as Map/Reduce or Spark,
  and ad-hoc SQL queries.

Application Virtualization
--------------------------

CDAP's programming interfaces have multiple implementations in different runtime environments. You build
your applications against the CDAP application API. This API not only hides the low-level details of
individual programming paradigms and runtimes, it also gives access to many useful services provided by
CDAP, such as the dataset service or the service discovery. When you deploy and run the application into a
specific installation of CDAP, the appropriate implementations of all services and program runtimes are
injected by CDAP - the application does not need to change based on the environment. This allows you
develop applications in one environent - say, on your laptop using a stand-alone CDAP for testing - and
then seamlessly deploy it in a different environemnt - say, your distributed staging cluster.


Cask Data Application Platform Overview
=======================================
Under the covers, **Cask Data Application Platform (CDAP)** is a Java-based middleware solution that
abstracts the complexities and integrates the components of the Hadoop ecosystem (YARN, MapReduce,
HBase, Zookeeper, etc.). Simply stated, CDAP behaves like a modern-day application
server, distributed and scalable, sitting on top of a Hadoop distribution (such as CDH,
HDP, or Apache). It provides a programming framework and scalable runtime environment
that allows any Java developer to build Big Data applications without having to
understand all of the details of Hadoop.

Integrated Framework
--------------------
Without a Big Data middleware layer, a developer has to piece together multiple open
source frameworks and runtimes to assemble a complete Big Data infrastructure stack.
CDAP provides an integrated platform that makes it easy to create all the elements of
Big Data applications: collecting, processing, storing, and querying data. Data can be
collected and stored in both structured and unstructured forms, processed in real-time
and in batch, and results can be made available for retrieval, visualization, and
further analysis.

Simple APIs
-----------
CDAP aims to reduce the time it takes to create and implement applications
by hiding the complexity of these distributed technologies with a set of powerful yet
simple APIs. You don’t need to be an expert on scalable, highly-available system
architectures, nor do you need to worry about the low-level Hadoop and HBase APIs.

Full Development Lifecycle Support
----------------------------------
CDAP supports developers through the entire application development lifecycle:
development, debugging, testing, continuous integration and production. Using familiar
development tools such as *IntelliJ* and *Eclipse*, you can build, test and debug your
application right on your laptop with a *Standalone CDAP*. Utilize the application unit
test framework for continuous integration. Deploy it to a development cloud or production
cloud (*Distributed CDAP*) with a push of a button.

Easy Application Operations
---------------------------
Once your Big Data application is in production, CDAP is designed
specifically to monitor your applications and scale with your data processing needs:
increase capacity with a click of a button without taking your application offline. Use
the CDAP Console or RESTful APIs to monitor and manage the lifecycle and scale of your
application.


Components of CDAP and their Interactions
=========================================

CDAP consists mainly of these components:

- The Router is the only public access point into CDAP for external clients. It forwards client requests to
  the appropriate system service or application. In a secure setup, the router also performs authentication;
  It is then complemented by an authentication service that allows clients to obtain credentials for CDAP.
- The Master controls and manages all services and applications.
- System Services provide vital platform features such datasets, transactions, service discovery logging,
  and metrics collection. System services run in application containers.
- Application containers provide virtualization and isolation for execution of application code (and, as a
  special case, system services). Application containers scale linearly and elastically with the underlying
  infrastructure.

.. image:: _images/components.png
   :width: 6in
   :align: center

CDAP Programming Interfaces
===========================

We distinguish between the Developer interface and the Platform interface.

- The Developer interface is used to build applications and exposes various Java APIs that are only available to
  code that runs inside application containers, for example the Dataset and Transaction APIs as well as the
  various supported programming paradigms.
- The Platform interface is a RESTful API and the only way that external clients can interact with CDAP and
  applications. It includes APIs that are not accessible from inside containers, such as application
  management and monitoring. As an alternative to HTTP, clients can also use the client libraries
  provided for different programming languages, including Java, JavaScript and Python.

Note that some interfaces are included in both the Developer and the Platform APIs, for example, Service Discovery
and Dataset Management.

.. image:: _images/api-venn.png
   :width: 6in
   :align: center

Anatomy of a Big Data Application
=================================

As an application developer building a Big Data application, you are primarily concerned with four areas:

- Data Collection: A way to get data into the system, so that it can be processed. We distinguish these types
  of collecting data:

  - A system or application service may poll an external source for available data and then retrieve it ("pull"),
    or external clients may send data to a public endpoint of the platform ("push").
  - Data can come steadily, one event at a time ("realtime") or in bulk, many events at once ("batch").
  - Data can be acquired with a fixed schedule ("periodic") or whenever new data is available ("on-demand").

  CDAP provides streams as a means to push events into the platform in real-time. It also provides tools that
  pull data in batch, be it periodic or on-demand, from external sources.

  Streams are special type of dataset that are exposed as a push endpoint for external clients. They support
  ingesting events in realtime at massive scale. Events in the stream can then be consumed by applications in
  real-time or batch.

- Data Exploration: One of the most powerful paradigms of Big Data is "schema-on-write". This means the ability
  to collect and store data without knowing details about its schema or structure. These details are only needed
  a processing time. An important step between collecting the data and processing it exploration, that is,
  examining data with ad-hoc queries to learn about its structure and nature.

  NOTE: This is not exactly what CDAP allows! FIXME!

- Data Processing: After data is collected, we need to process it in various ways. For example:

  - Raw events are filtered and transformed into a canonical form, to ensure quality of input data for
    down-stream processing.
  - Events (or certain dimensions of the events) are counted or aggregated in other ways.
  - Events are annotated and used by an iterative algorithm to train a machine learned model.
  - Events from different sources are joined to find associations, correlations or other views across
    multiple sources.
  - Etc.

  Processing can happen in realtime, where a stream processor consumes events immediately after they are collected.
  Such processing provides has less expressive power than other processing paradigms but provides insights into the
  data in a very timely manner. CDAP offers Flows as the realtime processing framework.

  Processing can also happen in batch, where many events are processed at the same time to analyze an entire data
  corpus at once. Batch processing is more powerful than realtime processing, but due its very nature is always
  time-lagging and therefore often performed over historical data. In CDAP, batch processing can be done via
  Map/Reduce or Spark, and it can also be scheduled on a periodic basis as part of a workflow.

- Data Storage: The results of processing data must be stored in a persistent and durable way, that allows other
  programs or applications to further process or analyze this data. In CDAP, data is stored in datasets.

- Data Serving: The ultimate purpose of processing data is not to store the results, but to make these results
  available to other applications. For example, a web analytics application may find ways to optimize the traffic
  on a website. However, these insights are worthless without a way to feed them back to the actual web application.
  CDAP allows serving datasets to external clients through procedures and services.

Hence, a CDAP application consists of the following components:

- `Streams <dev-guide.html#streams>`__ for real-time data collection;
- Programs (`Flows <dev-guide.html#flows>`__, `MapReduce <dev-guide.html#mapreduce>`__,
  `Spark <dev-guide.html#spark>`__) for data processing in realtime or in batch;
- `Datasets <dev-guide.html#datasets>`__ for virtualized data storage; and
- `Procedures <dev-guide.html#procedures>`__ and `Services <dev-guide.html#services>`__
  for data serving to external clients.

The follwoing diagram illustrates a typical Big Data application:

.. image:: _images/unified_realtime_batch.JPG
   :width: 6in
   :align: center

This also illustrates the power of data virtualization in CDAP: A stream is not only a means to collect data, it can
also be consumed by realtime and batch processing at the same time. Similarly, datasets allow sharing of data between
programs of different paradigms, be they in realtime or batch.

Where to Go Next
================
Now that you understand the concepts and the architecture of CDAP, you are ready to write an application:

- `Cask Data Application Platform Developer Guide <dev-guide.html>`__,
  which guides you through all the development of all the components of an application.

.. |(TM)| unicode:: U+2122 .. trademark sign
   :trim:
