.. :author: Cask Data, Inc.
   :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-nav: true
:orphan:

.. _glossary:

============================================
Glossary
============================================

.. glossary::
   :sorted:

   Application
      A collection of Programs, Services, and Procedures (deprectated) that read and write
      through the data abstraction layer in CDAP.
      
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

   Procedure
      Procedures (deprecated) are used to query CDAP and its Datasets and retrieve results, making
      synchronous calls into CDAP from an external system and perform server-side
      processing on-demand. They are similar to a stored procedure in a traditional database
      system. Procedures are typically used to post-process data at query time.
      
   Data Abstraction
      Abstraction of the actual representation of data in storage.
      
   Application Abstraction
      Application abstraction allows the same application to run in multiple implementations 
      and environments without modification.
      
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
      Refers to the `Apache™ Hadoop® <http://hadoop.apache.org>`__ project, which describes
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
      
   CDAP Console
      The CDAP Console is a web-based application used to deploy CDAP Applications, and
      query and manage the Cask Data Application Platform instance.

   Apache Spark
      See :term:`Spark Program <spark>`.

   Apache Hadoop
      See :term:`Hadoop`.

   Avro
      Refers to the `Apache Avro™ <http://avro.apache.org>`__ project, which is a
      data serialization system that provides rich data structures and a compact, fast, binary data format.



.. |(TM)| unicode:: U+2122 .. trademark sign
   :ltrim:

.. |(R)| unicode:: U+00AE .. registered trademark sign
   :ltrim:

.. Apache |(TM)| Hadoop |(R)|
.. Apache™ Hadoop®

