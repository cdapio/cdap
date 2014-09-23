.. :author: Cask Data, Inc.
   :description: Getting Started with Cask Data Application Platform
         :copyright: Copyright © 2014 Cask Data, Inc.

===================================================
Getting Started with Cask Data Application Platform
===================================================

This chapter is a guide to help you get started with CDAP and prepares you to be familiar with the environment, At the
end of this topic you will have CDAP up and running on your platform, you would learn how to develop, deploy and play with
CDAP  with the help of the sample CDAP App provided in this chapter.

CDAP SDK - Quick Intro
----------------------

SDK is your entry door for developing apps that can run on CDAP,it provides nice and clean abstractions like ``Streams, Flows,Datasets`` to help solve your Big-data problems.
  - Stream is entry point for data ingestion into CDAP, users can create and define streams and add streams to their application for processing.
  - Processing happens at Flows, and the flow system is a Directed-acyclic graph constructed by the user and processing logic is provided by the user.
  - CDAP SDK comes up with a good collection of in-built Datasets, that is developed to suit the needs of the Big-Data Storage and these datasets make it
    easy for a user to think and store his data in a dataset - which is reflective of the kind of data. we believe this abstraction is more intuitive and
    removes the complexity of the underlying storage engines for the user.
  - The SDK comes with a standalone CDAP that implements all the features of CDAP, including data and app virtualizatiion, in a single JVM and can run on your laptop.
  - The SDK also includes tools and clients that can help in developing, interacting-with and debugging CDAP applications.
  - The SDK has features for running Map-Reduce and Spark Jobs
  - The SDK provides Ad-hoc SQL support to query the datasets.

Before Getting Started
======================
The minimum requirements to run CDAP applications are only three,
 - `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__ (required to run CDAP; note that $JAVA_HOME should be set)
 - `Node.js 0.8.16+ <http://nodejs.org>`__ (required to run the CDAP UI)
 - `Apache Maven 3.0+ <http://maven.apache.org>`__ (required to build CDAP applications)

Getting CDAP
------------
CDAP is available as a source tarball and binary on the Downloads section of Cask Website. If you want to get started quickly, the binary is the easiest way to get started.


Building from Source
====================

**Check out the source** ::

    $ git clone https://github.com/caskco/cdap cdap
    $ cd cdap

**Compile the Project** ::

  $ mvn clean package -DskipTests -P examples -pl cdap-examples -am -amd && mvn package -pl cdap-standalone -am -DskipTests -P dist,release


If you are an app-developer, you would want the binary distribution ::

  $ cp cdap-standalone/target/cdap-sdk-<version>.zip .
  $ tar -zxvf cdap-sdk-<version>.zip
  $ cd cdap-sdk-<version>

**Running CDAP SDK** ::

    $ ./bin/cdap.sh start (If you are using Windows, use the batch script to start)

Once CDAP is started successfully, you can see the CDAP web UI running at localhost:9999, you can head there to deploy sample example apps and experience CDAP.

For Developers
==============

To generate a sample CDAP application, you would use the maven archetype ::

   ``$ mvn archetype:generate \
    -DarchetypeCatalog=https://repository.cask.co/content/groups/releases/archetype-catalog.xml \
    -DarchetypeGroupId=co.cask.cdap \
    -DarchetypeArtifactId=cdap-app-archetype \
    -DarchetypeVersion=2.5.0``

To setup the CDAP application development environment, you need to import the generated pom file in your IDE,
Now you are all set to start developing your first CDAP application. To help you familiarize with the environment and get developing CDAP apps, we have provided a set of Example Apps ,
which uses and demonstrates the components of CDAP and we highly recommend you to read them before diving into CDAP.

.. _examples:

Example Apps
------------

HelloWorld
==========

A Simple HelloWorld App that's written using CDAP. It introduces how Stream,Dataset, Flow and Procedure are used in an CDAP application.

Purchase
========
This example demonstrates use of each of the CDAP elements: Streams, Flows, Flowlets,
Datasets, Queries, Procedures, MapReduce Jobs, Workflows, and Custom Services in a single Application.

 - PurchaseFlow Receives Events from a PurchaseStream about Purchases in the format "X bought Y apples for $Z", processes them
   and stores it in a ``purchases dataset``.
 - A Mapreduce Job reads the ``purchase dataset`` , creates a purchase history object and stores them in a
   ``history dataset``
 - Procedure and Ad-hoc SQL query support enables to query the ``history dataset`` to discover the purchase history
   of users.

Read more about this example :doc:`here <examples/purchase>`.

SparkKMeans
===========

An application that demonstrates streaming text analysis using a Spark program. It calculates the centers of points
from an input stream using the KMeans Clustering method.

Read more about this example :doc:`here <examples/sparkKMeans>`.

SparkPageRank
=============

An application that demonstrates streaming text analysis using a Spark program.
It computes the page rank of URLs from an input stream.

Read more about this example :doc:`here <examples/sparkPageRank>`.

WordCount
=========

A simple application that counts words and tracks word associations and unique words seen on the Stream.
It demonstrates the power of using Datasets and how they can be used to simplify storing complex data.
