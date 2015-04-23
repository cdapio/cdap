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
      A directed acyclic graph. Flows are wired together and displayed as a DAG in the CDAP Console.
      
   CDAP UI
      The CDAP UI is a web-based application used to deploy CDAP Applications, create ETL Applications, and
      query and manage the Cask Data Application Platform instance.

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

   .. ETL and Application Templates

   Structured Record
      The format used to exchange events between different ETL :term:`Components <component>`
      
   Sink
      A :term:`Component` that accepts events and persists them.
      
   Source
      A :term:`Component` that produces events.
      
   Transformation
      A :term:`Component` that accepts events, performs modifications on them, and then transmits them.

   Filter
      A type of :term:`Transformation` that only passes events that meet a specific criteria.
      
   Projection
      A type of :term:`Transformation` that modifies events that meet a specific criteria.
      Possible modifications include renaming and dropping of fields.

   Manifest
      A JSON Object, either in-memory or in a file, that defines either an :term:`ETL Application`,
      an :term:`App-Template`, a :term:`Component`, or an :term:`ETL Pipeline`.
      
   App-Template
      A set of :term:`Components <component>` that can combined together to create an
      :term:`ETL Pipeline`, consisting of one or more :term:`Sources <source>`, one or
      more :term:`Sinks <sink>`, and one or more :term:`Transformations <transformation>`.
      Each must be interchangeable, in that any Source can hook to any Transform and any Sink.
    
   Pipeline
      A linked set of a specific :term:`Source` and :term:`Sink`, with a set of one or more
      :term:`Transformations <transformation>` in between.
    
   ETL
      Refers to the Engesting, Transforming and Loading of data. 
    
   ETL Pipeline
      See :term:`Pipeline`.

   ETL Application
      A packaged :term:`ETL Pipeline <pipeline>`, either in a JAR file or manifested in the :term:`CDAP UI`.
    
   Component
      One of a :term:`Sink`, :term:`Source` or :term:`Transformation`, packaged in a JAR file format, for use as a
      :term:`Plugin`.

   ETL Component
      See :term:`Component`.

   Plugin
      Specially-constructed files (JAR Files) that add custom features to CDAP.
      Current Plugins include an :term:`App-Template` and an :term:`ETL Component` JAR.
      
   Plugins Directory
      The specific directory on a CDAP installation where a :term:`Plugin` is placed.
      
   Realtime
      A sequence of events characterized as ones that happen outside of the control of the
      receiving system. The flow of the sequence cannot be initiated, started nor stopped
      by the receiving system; they must simply be accepted or rejected as they arrive.
    
   Batch
      A sequence of events characterized as ones that happen inside the control of the
      receiving system. The flow of the sequence is initiated, started or stopped by the
      receiving system; the number of events that are dealt with in a single interaction
      can be controlled and set by the receiving system.
      