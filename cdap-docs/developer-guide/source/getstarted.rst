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

System Requirements and Dependencies
------------------------------------

The CDAP SDK runs on Linux, Unix, MacOS and Windows, and only has three requirements:

- `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__ (required to run CDAP;
  note that $JAVA_HOME should be set)
- `Node.js 0.8.16+ <http://nodejs.org>`__ (required to run the CDAP UI)
- `Apache Maven 3.0+ <http://maven.apache.org>`__ (required to build CDAP applications)

.. highlight:: console

Download and Setup
==================

There are three ways to download the CDAP SDK: as a binary zip file, as a Virtual Machine image,
or as a Docker image.

- The zip file is available on the Downloads section of the Cask Website at `<http://cask.co/downloads>`__.
  Once downloaded, unzip it to a directory on your machine::

    $ tar -zxvf cdap-sdk-2.5.0.zip

- To use the Virtual Machine image:

  + Download and install either `Oracle VirtualBox <https://www.virtualbox.org>`__ or
    `VMWare <http://www.vmware.com/products/player>`__ player to your environment.
  + Download the CDAP Standalone Virtual Machine (the .ova file) at `<http://cask.co/downloads>`__.
  + Import the Virtual Machine into VirtualBox or VMWare Player.
  + The CDAP Standalone Virtual Machine has been configured and setup so you can be productive immediately:

    * CDAP VM is configured with 4GB Default RAM (recommended).
    * The virtual machine has Ubuntu Desktop Linux installed as the operating system.
    * No password is required to enter the virtual machine; however, should you need to install or
      remove software, the admin user and password are both “cdap”.
    * 10GB of disk space is available for you to build your first CDAP project.
    * Both IntelliJ and Eclipse IDE are installed and will start when the virtual machine starts.
    * The CDAP SDK is installed under ``/Software/cdap-sdk-2.5.0``.
    * The Standalone CDAP will automatically start when the virtual machine starts.
    * The Firefox web browser starts when the machine starts. Its default home page is the CDAP Console
      (http://localhost:9999). You're welcome to install your favorite browser.
    * Maven is installed and configured to work for CDAP.
    * The Java JDK and Node JS are both installed.

Starting the Standalone CDAP
----------------------------

Use the ``cdap.sh`` script to start and stop the Standalone CDAP::

  $ cd cdap-sdk-2.5.0
  $ ./bin/cdap.sh start
  ...
  $ ./bin/cdap.sh stop

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

.. _examples:

First Steps
===========

Before you start developing your own applications, it is recommended to familiarize yourself with the
APIs and concepts of CDAP as well as the CDAP console using example applications that are provided together
with the SDK. Let's take a look at one of these:

[TODO: Insert tutorial #1 by Terence].

Other Example Applications
==========================

In addition to the previous example, the following examples are included in the SDK:

HelloWorld
----------

A Simple HelloWorld App that's written using CDAP. It introduces how Stream, Dataset, Flow and Procedure
are used in an CDAP application.

Purchase
--------

This example demonstrates use of each of the CDAP elements: Streams, Flows, Flowlets,
Datasets, Queries, Procedures, MapReduce Jobs, Workflows, and Custom Services in a single Application.

 - The PurchaseFlow receives Events from a stream, each describing a purchase by a given customer
   ("John bought 5 apples for $2"), processes and stores them it in a ``purchases`` dataset.
 - A Mapreduce Job reads the ``purchase`` dataset, compiles the purchases of each customer into a purchase
   history, and stores them in a ``history`` dataset.
 - The ``history`` dataset can then be queried through a procedure and also through Ad-hoc SQL queries.

Read more about this example :doc:`here <examples/purchase>`.

SparkKMeans
-----------

An application that demonstrates streaming text analysis using a Spark program. It calculates the centers of points
from an input stream using the KMeans Clustering method.

Read more about this example :doc:`here <examples/sparkKMeans>`.

SparkPageRank
-------------

An application that demonstrates streaming text analysis using a Spark program.
It computes the page rank of URLs from an input stream.

Read more about this example :doc:`here <examples/sparkPageRank>`.

WordCount
---------

A simple application that counts words and tracks word associations and unique words seen on the Stream.
It demonstrates the power of using datasets and how they can be used to simplify storing complex data.
