.. :author: Cask Data, Inc.
   :description: Getting Started with Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

.. _get-started:

=======================================================
Getting Started with the Cask Data Application Platform
=======================================================

This chapter is a guide to help you get started with CDAP. At the end of this topic, you
will have the CDAP SDK up and running in your development environment, and you will have
built, deployed, and run a sample application.

Introduction to the CDAP SDK
============================

The CDAP Software Development Kit (SDK) is all that is needed to develop CDAP applications
in your development environment, either your laptop or a work station. It includes:

- A Standalone CDAP that can run on a single machine in a single JVM. It provides all of
  the CDAP APIs without requiring a Hadoop cluster, using alternative, fully functional
  implementations of CDAP features. For example, application containers are implemented as
  Java threads instead of YARN containers.
- The CDAP Console, a web-based graphical user interface to interact with the Standalone CDAP
  and the applications it runs.
- The complete CDAP documentation, including this document and the Javadocs for the CDAP APIs.
- A set of tools, datasets and example applications that help you get familiar with CDAP, and
  can also serve as templates for developing your own applications.

System Requirements and Dependencies for the SDK
------------------------------------------------

The CDAP SDK runs on Linux, Unix, MacOS and Windows, and only has three requirements:

- `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__ 
  (required to run CDAP; note that $JAVA_HOME should be set)
- `Node.js 0.8.16+ <http://nodejs.org>`__ (required to run the CDAP Console UI)
- `Apache Maven 3.0+ <http://maven.apache.org>`__ (required to build CDAP applications)

.. highlight:: console

Downloading and Setting Up the CDAP SDK
=======================================
The SDK is available as a binary on the Downloads section of the 
`Cask Website. <http://cask.co/downloads>`__
Once downloaded, unzip the SDK to a directory on your machine::

  $ tar -zxvf cdap-sdk-2.5.0.zip
  $ cd cdap-sdk-2.5.0

Running CDAP SDK
----------------
::

    $ ./bin/cdap.sh start

If you are using Windows, use the batch script ``cdap.bat`` instead to start the SDK.

Once CDAP is started successfully, you can see the CDAP Console running in a web browser
at ``localhost:9999``, where you can deploy example applications and interact with CDAP.

Creating an Application
=======================

The best way to start developing a CDAP application is by using the Maven archetype::

  $ mvn archetype:generate \
    -DarchetypeCatalog=https://repository.cask.co/content/groups/releases/archetype-catalog.xml \
    -DarchetypeGroupId=co.cask.cdap \
    -DarchetypeArtifactId=cdap-app-archetype \
    -DarchetypeVersion=2.5.0

This creates a Maven project with all required dependencies, Maven plugins, and a simple
application template for the development of your application. You can import this Maven project
into your preferred IDE—such as Eclipse or IntelliJ—and start developing your first
CDAP application.

.. _examples:

First Steps
===========

Before you start developing your own applications, it is recommended that you familiarize yourself with the
APIs and concepts of CDAP as well as the CDAP Console using the example applications that are provided
with the SDK. Let's take a look at one of these:

[TODO: Insert tutorial #1 by Terence].

Other Example Applications
==========================

In addition to the previous example, these examples are included in the SDK:

HelloWorld
----------

A Simple HelloWorld App that's written using CDAP. It introduces Streams, Datasets, Flows, and Procedures,
and how they are used in a CDAP application.

Purchase
--------

This example demonstrates use of each of the CDAP elements: Streams, Flows, Flowlets,
Datasets, Queries, Procedures, MapReduce Jobs, Workflows, and Custom Services, all in a single Application.

 - The PurchaseFlow receives Events from a Stream, each event ("John bought 5 apples for $2")
   describing a purchase by a customer. The Flow processes and stores the events in a ``purchases`` Dataset.
 - A Mapreduce Job reads the ``purchase`` Dataset, compiles the purchases of each customer into a purchase
   history, and stores them in a ``history`` Dataset.
 - The ``history`` Dataset can then be queried through a Procedure and also through ad-hoc SQL queries.

:doc:`Read more about this example. <examples/purchase>`

SparkKMeans
-----------

An application that demonstrates streaming text analysis using a Spark program. It calculates the centers of points
from an input stream using the KMeans Clustering method.

:doc:`Read more about this example. <examples/sparkKMeans>`

SparkPageRank
-------------

An application that demonstrates streaming text analysis using a Spark program.
It computes the page rank of URLs from an input stream.

:doc:`Read more about this example. <examples/sparkPageRank>`

WordCount
---------

A simple application that counts words, unique words, and tracks word associations as seen on a Stream.
It demonstrates the power of using datasets and how they can be used to simplify storing complex data.


Where to Go Next
================
Now that you've had an introduction to CDAP, take a look at:

- :doc:`Concepts and Architecture, <arch>` an overview of CDAP's underlying technology.

