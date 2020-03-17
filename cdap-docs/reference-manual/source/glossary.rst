.. :author: Cask Data, Inc.
   :copyright: Copyright © 2014-2017 Cask Data, Inc.

:hide-nav: true

.. _glossary:

========
Glossary
========

.. glossary::
   :sorted:

   Application
      A collection of programs and services that read and write through the data
      abstraction layer in CDAP.

   Application Template
      An artifact, that with the addition of a configuration file, can be used to create
      manifestations of applications.

   Artifact
      A JAR file containing Java classes and resources required to create and run an
      :term:`Application`. Multiple applications can be created from the same artifact.

   Batch Pipeline
      A type of :term:`CDAP Pipeline` that runs on a schedule, performing actions on
      a distinct set of data.

   CDAP Application
      See :term:`Application`.

   CDAP Studio
      A visual editor, part of the :term:`CDAP UI`, for creating and configuring
      pipelines. You click and drag sources, transformations, and sinks, and can name and
      configure the pipelines. It provides an operational view of the resulting pipeline
      that allows for monitoring of metrics, logs, and other run-time information.

   Dataset
      Datasets store and retrieve data and are a high-level abstraction of the underlying
      data storage with generic reusable implementations of common data patterns.

   MapReduce
      MapReduce is a processing model used to process data in batch. MapReduce programs can be
      written as in a conventional Apache Hadoop system. CDAP datasets can be accessed
      from MapReduce programs as both input and output.

   Workflow
      A workflow is used to execute a series of MapReduce programs, with an optional schedule
      to run itself periodically.

   Secure Key
      An identifier or an alias for an entry in :term:`Secure Storage`. An entry in secure
      storage can be referenced and retrieved using a Secure Key using :ref:`programmatic <secure-keys-programmatic>`
      or :ref:`RESTful <http-restful-api-secure-storage>` APIs.

   Secure Storage
      Encrypted storage for sensitive data using :term:`Secure Keys <Secure Key>`. CDAP supports :ref:`File-backed
      <admin-secure-storage-file>` (for :term:`CDAP Sandbox`) as well as :ref:`Apache Hadoop KMS-backed
      <admin-secure-storage-kms>` (for :term:`Distributed CDAP`) Secure Storage.

   Spark
      Spark is a fast and general processing engine, compatible with Hadoop data, used for
      in-memory cluster computing. It lets you load large sets of data into memory and
      query them repeatedly, making it suitable for both iterative and interactive
      programs. Similar to :term:`MapReduce`, Spark can access datasets as both input and output.
      Spark programs in CDAP can be written in either Java or Scala.

   Service
      Services can be run in a Cask Data Application Platform (CDAP) application to serve
      data to external clients. Services run in containers and the
      number of running service instances can be dynamically scaled. Developers can
      implement custom services to interface with a legacy system and perform additional
      processing beyond the CDAP processing paradigms. Examples could include running an
      IP-to-Geo lookup and serving user-profiles.

   Worker
      Workers are typically long-running background programs that can be used to execute tasks.
      Each instance of a worker runs either in its own YARN container (Distributed CDAP mode) or
      a single thread (CDAP Sandbox or in-memory mode) and the number of instances may be updated
      via RESTful APIs or the CLI. Datasets can be accessed from inside workers.

   Data Abstraction
      Abstraction of the actual representation of data in storage.

   Data Pipeline
      A type of :term:`pipeline`, often not linear in nature and require the performing of
      complex transformations including forks and joins at the record and feed level. They
      can be configured to perform various functions at different times, including
      machine-learning algorithms and custom processing.

   ETL
      Abbreviation for *extract,* *transform,* and *loading* of data.

   Application Abstraction
      Application abstraction allows the same application to run in multiple environments
      without modification.

   CDAP
      The Cask Data Application Platform; refers to both the platform, and an installed instance of it.

   CDAP Sandbox
      A version of the Cask Data Application Platform, supplied as a downloadable archive,
      that runs on a single machine in a single Java Virtual Machine (JVM). It provides
      all of the CDAP APIs without requiring a Hadoop cluster, using alternative,
      fully-functional implementations of CDAP features. For example, application
      containers are implemented as Java threads instead of YARN containers. Formerly
      known as the :term:`Standalone CDAP`.

   Standalone CDAP
      See :term:`CDAP Sandbox`.

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
      A directed acyclic graph. A Pipeline is displayed as a DAG in the CDAP UI.

   CDAP UI
      The CDAP UI is a web-based application used to deploy CDAP applications, create
      :term:`pipelines <pipeline>` using the :term:`CDAP Studio`, and query and
      manage the Cask Data Application Platform instance.

   CDAP Console
      See :term:`CDAP UI`.

   CDAP CLI
      See :term:`Command Line Interface`.

   Command Line Interface
      The :ref:`Command Line Interface (CLI) <cli>` provides methods to interact with CDAP
      from within a shell, similar to the HBase or ``bash`` shells.

   Apache Spark
      See :term:`Spark Program <spark>`.

   Apache Hadoop
      See :term:`Hadoop`.

   Avro
      Refers to the `Apache Avro™ <http://avro.apache.org>`__ project, which is a
      data serialization system that provides rich data structures and a compact, fast, binary data format.

   Namespace
      A namespace is a logical grouping of application, data and its metadata in CDAP.
      Conceptually, namespaces can be thought of as a partitioning of a CDAP instance. Any
      application or data (referred to here as an “entity”) can exist independently in
      multiple namespaces at the same time. The data and metadata of an entity is stored
      independent of another instance of the same entity in a different namespace. The
      primary motivation for namespaces in CDAP is to achieve application and data
      isolation.

   Master Services
      CDAP system services that are run in YARN containers, such as the Transaction Service,
      Dataset Executor, Log Saver, Metrics Processor, etc.

   FileSet
      A :term:`dataset` composed of collections of files in the file system that share
      some common attributes such as the format and schema, which abstracts from the
      actual underlying file system interfaces.

   Time-partitioned FileSet
      A :term:`FileSet` :term:`dataset` that uses a timestamp as the partitioning key to
      split the data into indivividual files. Though it is not required that data in each
      partition be organized by time, each partition is assigned a logical time. Typically
      written to in batch mode, at a set time interval.

   Timeseries Dataset
      A :term:`dataset` where time is the primary means of how data is organized, and both
      the data model and the schema that represents the data are optimized for querying
      and aggregating over time ranges.

   Exploring
      Datasets in CDAP can be explored through ad-hoc SQL-like queries. To
      enable exploration, you must set several properties when creating the
      dataset, and the files in a dataset must meet certain requirements.

   Structured Record
      The data format used to exchange events between most of the pre-built CDAP ETL :term:`plugins <plugin>`.

   Plugin
      A plugin extends an :term:`application` by implementing an interface
      expected by the :term:`application`. Plugins are packaged in an :term:`artifact`.

   Pipeline
      CDAP provides an easy method of configuring
      pipelines using a visual editor, called :term:`CDAP Studio`. You click and
      drag sources, transformations, and sinks, configuring an pipeline within minutes. It
      provides an operational view of the resulting pipeline that allows for monitoring of
      metrics, logs, and other run-time information.

   Storage Provider
      For :term:`datasets <dataset>`, a storage provider is the underlying
      system that CDAP uses for persistence. Examples include HDFS, HBase, and Hive.

   Route Configuration
      Also known as a *route config*, a map that allocates requests for a service between
      different versions of the service.

   Route Config
      See :term:`route configuration`.

   CDAP Pipeline
      A CDAP application; created from an application template, generally one
      of the system artifacts shipped with CDAP; defines a source to read from, zero or more
      transformations or other steps to perform on the data that was read from the source, and
      one or more sinks to write the transformed data to.

   CDAP Pipeline Plugin
      A plugin of type BatchSource, RealtimeSource, BatchSink, RealtimeSink, or
      Transformation, packaged in a JAR file format, for use as a plugin in a
      CDAP pipeline.

   Logical Pipeline
      A view of a :term:`pipeline` composed of sources, sinks, and other plugins, and does
      not show the underlying technology used to actually manifest and run the pipeline.

   Physical Pipeline
      A physical pipeline is the manifestation of a :term:`logical pipeline` as a CDAP
      application, which is a collection of programs and services that read and write
      through the data abstraction layer in CDAP.

   Pipeline
      A pipeline is a series of stages |---| linked usages of individual programs |---|
      configured together into an application.

   Plugin
      A plugin extends an application template by implementing an interface expected by
      the application template. Plugins are packaged in an artifact.

   Real-time Pipeline
      A type of :term:`CDAP Pipeline` that runs continuously, performing actions on
      a distinct set of data.

   Structured Record
      A data format, defined in CDAP, that can be used to exchange events
      between plugins. Used by many of the :term:`CDAP pipeline plugins <CDAP Pipeline
      Plugin>` included in CDAP.

   System Artifact
      An application template, shipped with CDAP, that with the addition of a
      configuration file, can be used to create manifestations of applications.

