.. :author: Cask Data, Inc.
   :description: Getting Started with Cask Data Application Platform
   :copyright: Copyright © 2014 Cask Data, Inc.

.. _get-started:

===================================================
Getting Started with Cask Data Application Platform
===================================================

This chapter is a guide to help you get started with CDAP. At the end of this topic you will have the CDAP SDK up
and running in your development environment, and you will have built, deployed, run, and interacted with a sample
application.

Introduction to the CDAP SDK
============================

The CDAP Software Developers Kit (SDK) is all you need to develop CDAP applications in your development environment
(for example, your laptop or work station). It includes:

- A Standalone CDAP that can run on a single machine in a single JVM. It provides all of
  the CDAP APIs without requiring a Hadoop cluster, using alternative, fully functional
  implementations of CDAP features. For example, application containers are implemented as
  Java threads instead of YARN containers.
- The CDAP console, a web-based graphical user interface to interact with the Standalone CDAP
  and the applications it runs.
- The complete CDAP documentation, including this document and the Javadocs for the CDAP APIs.
- A set of tools, datasets and example applications that help you get familiar with CDAP, and
  can also serve as templates for developing for own applications.

System Requirements and Dependencies for the SDK
------------------------------------------------

The CDAP SDK runs on Linux, Unix, MacOS and Windows, and only has three requirements:

 - `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__ (required to run CDAP;
   note that $JAVA_HOME should be set)
 - `Node.js 0.8.16+ <http://nodejs.org>`__ (required to run the CDAP UI)
 - `Apache Maven 3.0+ <http://maven.apache.org>`__ (required to build CDAP applications)

Downloading and Setting Up the CDAP SDK
=======================================
The SDK is available as a binary on the Downloads section of the Cask Website.
[TODO: add link] Once downloaded, unzip it to a directory on your machine:
::

  $ tar -zxvf cdap-sdk-2.5.0.zip
  $ cd cdap-sdk-2.5.0

**Running CDAP SDK** ::

    $ ./bin/cdap.sh start

Or, if you are using Windows, use the batch script cdap.bat to start the SDK.

Once CDAP is started successfully, you can see the CDAP console running at localhost:9999, and you can
head there to deploy example applications and interact with CDAP.

Creating an Application
=======================

The best way to start developing a CDAP application is using the Maven archetype::

  $ mvn archetype:generate \
    -DarchetypeCatalog=https://repository.cask.co/content/groups/releases/archetype-catalog.xml \
    -DarchetypeGroupId=co.cask.cdap \
    -DarchetypeArtifactId=cdap-app-archetype \
    -DarchetypeVersion=2.5.0``

This creates a Maven project with all required dependencies and Maven plugins as well as a simple
application template that you can modify to develop your application. You can import this Maven project
into your favorite IDE, such as Eclipse or IntelliJ, and you are ready to start developing your first
CDAP application.

First Steps
===========

Before you start developing your own applications, it is recommended to familiarize yourself with the
APIs and concepts of CDAP as well as the CDAP console using example applications that are provided together
with the SDK. Let's take a look at one of these:

.. include:: first-app.rst

.. _examples:

Other Example Applications
==========================

The SDK includes these examples:

.. list-table::
  :widths: 15 60
  :header-rows: 1

  * - Example Name
    - Description
  * - :ref:`HelloWorld<hello-world>`
    - A simple HelloWorld App that's written using CDAP. It introduces how the elements Stream, Dataset, Flow,
      and Procedure are used in a CDAP application.
  * - :ref:`WordCount<word-count>`
    - A simple application that counts words, and tracks word associations and unique words seen on the Stream.
      It demonstrates the power of using datasets and how they can be employed to simplify storing complex data.
  * - :ref:`CountRandom<count-random>`
    - An application that demonstrates the ``@Tick`` feature of Flows. It uses a tick method to generate random
      numbers which are then counted by downstream Flowlets.
  * - :ref:`Purchase<purchase>`
    - This example demonstrates use of each of the CDAP elements—Streams, Flows, Flowlets, Datasets, Queries,
      Procedures, MapReduce, Workflows, and Services—in a single Application.
      A Flow receives events from a Stream, each event describing a purchase ("John bought 5 apples for $2");
      the Flow processes the events and stores them in a Dataset. A Mapreduce Job reads the Dataset, compiles
      the purchases for each customer into a purchase history and stores the histories in a second Dataset.
      The purchase histories can then be queried either through a Procedure or an ad-hoc SQL query.
  * - :ref:`SparkKMeans<spark-k-means>`
    - An application that demonstrates streaming text analysis using a Spark program. It calculates the centers
      of points from an input stream using the KMeans Clustering method.
  * - :ref:`SparkPageRank<spark-page-rank>`
    - An application that demonstrates streaming text analysis using a Spark program. It computes the page rank
      of URLs from an input stream.
  * - :ref:`Web Analytics<web-analytics>`
    - An application to generate statistics and to provide insights about web usage through the analysis
      of web traffic.
.. include:: /_examples/conventions.rst
.. include:: /_examples/helloworld.rst
.. include:: /_examples/wordcount.rst
.. include:: /_examples/countrandom.rst
.. include:: /_examples/purchase.rst
.. include:: /_examples/sparkKMeans.rst
.. include:: /_examples/sparkPageRank.rst
