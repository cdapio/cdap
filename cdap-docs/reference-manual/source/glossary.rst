.. :author: Cask Data, Inc.
   :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-nav: true
:orphan:

.. _glossary:

========
Glossary
========

.. glossary::
   :sorted:

   Application
      A collection of Programs and Services that read and write through the data
      abstraction layer in CDAP.

   Stream
      The primary means of bringing data from external systems into CDAP in realtime; an
      ordered, time-partitioned sequences of data, usable for realtime collection and
      consumption of data.
      
   Dataset
      Datasets store and retrieve data and are a high-level abstraction of the underlying
      data storage with generic reusable implementations of common data patterns.
      
   Flow
      Flows are user-implemented realtime stream processors, comprised of one or
      more Flowlets that are wired together into a directed acyclic graph. 
      
   Flowlet
      A Flowlet represents an individual processing node within a Flow. Flowlets consume
      data objects from their inputs and execute custom logic on each data object, able to
      perform data operations as well as emit data objects to the Flowlet’s outputs.
      
   MapReduce
      MapReduce is a processing model used to process data in batch. MapReduce programs can be
      written as in a conventional Apache Hadoop system. CDAP Datasets can be accessed
      from MapReduce programs as both input and output.
      
   Workflow
      A Workflow is used to execute a series of MapReduce programs, with an optional schedule
      to run itself periodically.
      
   Spark
      Spark is a fast and general processing engine, compatible with Hadoop data, used for
      in-memory cluster computing. It lets you load large sets of data into memory and
      query them repeatedly, making it suitable for both iterative and interactive
      programs. Similar to :term:`MapReduce`, Spark can access Datasets as both input and output.
      Spark programs in CDAP can be written in either Java or Scala.

   Service
      Services can be run in a Cask Data Application Platform (CDAP) Application to serve
      data to external clients. Similar to Flows, Services run in containers and the
      number of running service instances can be dynamically scaled. Developers can
      implement Custom Services to interface with a legacy system and perform additional
      processing beyond the CDAP processing paradigms. Examples could include running an
      IP-to-Geo lookup and serving user-profiles.      

   Worker
      Workers are typically long-running background programs that can be used to execute tasks.
      Each instance of a worker runs either in its own YARN container (CDAP distributed mode) or
      a single thread (CDAP Standalone or In-Memory mode) and the number of instances may be updated
      via RESTful APIs or the CLI. Datasets can be accessed from inside Workers.

   Data Abstraction
      Abstraction of the actual representation of data in storage.
      
   Application Abstraction
      Application abstraction allows the same application to run in multiple  environments
      without modification.
      
   CDAP
      The Cask Data Application Platform; refers to both the platform, and an installed instance of it.

   Standalone CDAP
      A version of the Cask Data Application Platform, supplied as a downloadable SDK,
      that runs on a single machine in a single Java Virtual Machine (JVM). It provides
      all of the CDAP APIs without requiring a Hadoop cluster, using alternative,
      fully-functional implementations of CDAP features. For example, application
      containers are implemented as Java threads instead of YARN containers.

   Distributed CDAP
      A version of the Cask Data Application Platform, supplied as either Yum ``.rpm`` or
      APT ``.deb`` packages, that runs on a :term:`Hadoop` cluster. Packages are available
      for *Ubuntu 12* and *CentOS 6*.

   Hadoop
      Refers to the `Apache Hadoop® <http://hadoop.apache.org>`__ project, which describes
      itself as:

      *"The Apache Hadoop software library is a framework that allows for the distributed
      processing of large data sets across clusters of computers using simple programming
      models. It is designed to scale up from single servers to thousands of machines,
      each offering local computation and storage. Rather than rely on hardware to deliver
      high-availability, the library itself is designed to detect and handle failures at
      the application layer, so delivering a highly-available service on top of a cluster
      of computers, each of which may be prone to failures."*

   DAG
      A directed acyclic graph. Flows are wired together and displayed as a DAG in the CDAP UI.
      
   CDAP UI
      The CDAP UI is a web-based application used to deploy CDAP Applications, create 
      :term:`ETL Adapters <ETL Adapter>`, and query and manage the Cask Data Application 
      Platform instance.

   CDAP Console
      See :term:`CDAP UI`.

   Apache Spark
      See :term:`Spark Program <spark>`.

   Apache Hadoop
      See :term:`Hadoop`.

   Avro
      Refers to the `Apache Avro™ <http://avro.apache.org>`__ project, which is a
      data serialization system that provides rich data structures and a compact, fast, binary data format.

   Namespace
      A namespace is a physical grouping of application, data and its metadata in CDAP.
      Conceptually, namespaces can be thought of as a partitioning of a CDAP instance. Any
      application or data (referred to here as an “entity”) can exist independently in
      multiple namespaces at the same time. The data and metadata of an entity is stored
      independent of another instance of the same entity in a different namespace. The
      primary motivation for namespaces in CDAP is to achieve application and data
      isolation.

   Master Services
      CDAP system services that are run in YARN containers like Transaction Service,
      Dataset Executor, Log Saver, Metrics Processor, etc.

   FileSet
      A :term:`Dataset` composed of collections of files in the file system that share
      some common attributes such as the format and schema, which abstracts from the
      actual underlying file system interfaces.

   Time-partitioned FileSet
      A :term:`FileSet` :term:`Dataset` that uses a timestamp as the partitioning key to
      split the data into indivividual files. Though it is not required that data in each
      partition be organized by time, each partition is assigned a logical time. Typically
      written to in batch mode, at a set time interval.

   Timeseries Dataset
      A :term:`Dataset` where time is the primary means of how data is organized, and both
      the data model and the schema that represents the data are optimized for querying
      and aggregating over time ranges.

   Exploring
      Streams and Datasets in CDAP can be explored through ad-hoc SQL-like queries. To
      enable exploration, you must set several properties when creating the Stream or
      Dataset, and the files in a Dataset must meet certain requirements.

   .. ETL and Application Templates

   Structured Record
      The data format used to exchange events between most of the pre-built CDAP ETL :term:`Plugins <plugin>`.
      
   Adapter Configuration
      A JSON String that defines an :term:`Adapter`.
      
   Application Template
      An application that is reusable through configuration and extensible through plugins.
  
   Template
      See :term:`Application Template`.

   App Template
      See :term:`Application Template`.

   Adapter
      An Adapter is an instantiation of an :term:`Application Template` that has been created
      from a specific configuration. Adapters combine :term:`Plugins <plugin>` to access
      CDAP programs and resources.
   
   ETL
      Refers to the *Extract*, *Transform* and *Load* of data. 
    
   ETL Application Template
      Also referred to as an ETL Template. A type of :term:`Application Template`,
      designed to create an :term:`ETL Adapter`. Two ETL Templates are shipped with CDAP:
      ``ETLBatch`` and ``ETLRealtime``, for the creation of either batch or realtime
      :term:`ETL` pipelines.
  
   ETL Template
      See :term:`ETL Application Template`.

   ETL Adapter
      An ETL :term:`Adapter` is an Adapter created from an :term:`ETL Template`,
      specifically for creating :term:`ETL` applications.

   ETL Plugin
      A :term:`Plugin` of type BatchSource, RealtimeSource, BatchSink, RealtimeSink, or
      Transformation, packaged in a JAR file format, for use as a :term:`Plugin`
      in an ETL Adapter.

   Plugin
      A Plugin extends an :term:`Application Template` by implementing an interface
      expected by the Template. One or more Plugins are packaged in a specifically
      constructed JAR file.
