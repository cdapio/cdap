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

System Requirements and Dependencies
------------------------------------

The CDAP SDK runs on Linux, MacOS and Windows, and only has three requirements:

- `JDK 6 or JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__ 
  (required to run CDAP; note that $JAVA_HOME should be set)
- `Node.js 0.8.16+ <http://nodejs.org>`__ (required to run the CDAP Console UI)
- `Apache Maven 3.0+ <http://maven.apache.org>`__ (required to build CDAP applications)

.. highlight:: console

Download and Setup
==================

There are three ways to download the CDAP SDK: 

- as a `binary zip file`_;
- as a `Virtual Machine image <#cdap-standalone-virtual-machine-image>`__; or 
- as a `Docker image <#cdap-standalone-docker-image>`__.

If you already have a development environment setup, the zip file is your easiest solution.

If you don't have a development environment, the Virtual Machine offers a pre-configured
environment with CDAP pre-installed and that automatically starts applications so that you
can be productive immediately. You can build your own projects or follow the provided
example applications.

The Docker image is intended for those developing on Linux.

Binary Zip File
---------------
The **zip file** is available on the Downloads section of the Cask Website at `<http://cask.co/downloads>`__.
Click the link marked "SDK" of the *Software Development Kit (SDK).* 

Once downloaded, unzip it to a directory on your machine::

    $ tar -zxvf cdap-sdk-2.5.0.zip

CDAP Standalone Virtual Machine Image
-------------------------------------

To use the **Virtual Machine image**:

- Download and install either `Oracle VirtualBox <https://www.virtualbox.org>`__ or
  `VMWare <http://www.vmware.com/products/player>`__ player to your environment.
- Download the CDAP Standalone Virtual Machine (*Standalone VM*) at `<http://cask.co/downloads>`__.
- Import the downloaded ``.ova`` file into either the VirtualBox or VMWare Player.

The CDAP Standalone Virtual Machine is configured with the recommended settings for Standalone CDAP:

- 4 GB of RAM
- Ubuntu Desktop Linux
- 10 GB of disk space

It has pre-installed all the software that you need to run and develop CDAP applications:

- Java JDK 7 and Node.js are both installed.
- Maven is installed and configured to work for CDAP.
- The Standalone CDAP SDK is installed under ``/Software/cdap-sdk-2.5.0`` and will
  automatically start when the virtual machine starts.
- Both IntelliJ and Eclipse IDE are installed and will start when the virtual machine starts.
- The Firefox web browser starts when the machine starts. Its default home page is the CDAP Console,
  ``http://localhost:9999``.

No password is required to enter the virtual machine; however, should you need to install or
remove software, the admin user and password are both ``cdap``.

.. _docker:

CDAP Standalone Docker Image
-----------------------------

A Docker image with CDAP pre-installed is available on the Docker Hub for download.

To use the **Docker image**:

- Docker is available for a variety of platforms. Download and install Docker in your environment by
  following the `platform-specific installation instructions <https://docs.docker.com/installation>`__
  from `Docker.com <https://docker.com>`__ to verify that Docker is working and has
  started correctly.
  
  If you are not running on Linux, you need to start the Docker Virtual Machine (VM) before you
  can use containers. For example, on MacOS, use:: 
  
    $ boot2docker start
    $ boot2docker ip
    
  to determine the Docker VM's IP address. You will need to use that address as the host
  name when either connecting to the Console or making an HTTP request.
  
  When you run boot2docker start, it will print a message on the screen similar to::

    To connect the Docker client to the Docker daemon, please set:
    export DOCKER_HOST=tcp://192.168.59.103:2375

  It is essential to run this export command. Otherwise, subsequent Docker commands will
  fail because they can't tell how to connect to the Docker VM.

- Once Docker has started, pull down the *CDAP Docker Image* from the Docker hub using::

    $ docker pull caskdata/cdap-standalone
    
- Start the *Docker CDAP Virtual Machine* with::

    $ docker run -t -i -p 9999:9999 -p 10000:10000 caskdata/cdap-standalone
    
- CDAP will start automatically once the CDAP Virtual Machine starts. CDAP’s Software
  Directory is under ``/Software/cdap-sdk-2.5.0``.
  
- Once CDAP starts, it will instruct you to connect to the CDAP Console with a web browser
  at ``http://<virtual-hostname>:9999``, such as ``http://6f0162922c37:9999``. Replace
  ``<virtual-hostname>`` with the Docker VM's IP address you obtained earlier. Start a browser 
  and enter the address to access the CDAP Console.

- In order to begin building CDAP applications, have our `recommended software and tools
  <#system-requirements-and-dependencies>`__ installed in your environment.

- For a full list of Docker Commands, see the `Docker Command Line Documentation.
  <https://docs.docker.com/reference/commandline/cli/>`__

Starting and Stopping the Standalone CDAP
-----------------------------------------

Use the ``cdap.sh`` script to start and stop the Standalone CDAP 
(the location will vary depending on where the CDAP SDK is installed)::

  $ cd cdap-sdk-2.5.0
  $ ./bin/cdap.sh start
  ...
  $ ./bin/cdap.sh stop

Or, if you are using Windows, use the batch script ``cdap.bat`` to start and stop the SDK.

Note that starting CDAP is not necessary if you use either the Virtual Machine or the
Docker image, as they both start the Standalone CDAP automatically on startup.

Once CDAP is started successfully, in a web browser you will be able to see the CDAP
Console running at ``localhost:9999``, where you can deploy example applications and
interact with CDAP. Note that in the case of the Docker image, you will need to substitute 
the Docker VM's IP address for ``localhost`` in the web browser address bar.

Creating an Application
=======================

The best way to start developing a CDAP application is by using the Maven archetype::

  $ mvn archetype:generate \
    -DarchetypeGroupId=co.cask.cdap \
    -DarchetypeArtifactId=cdap-app-archetype \
    -DarchetypeVersion=2.5.0

This creates a Maven project with all required dependencies, Maven plugins, and a simple
application template for the development of your application. You can import this Maven project
into your preferred IDE—such as Eclipse or IntelliJ—and start developing your first
CDAP application.

First Steps
===========

Before you start developing your own applications, it is recommended that you familiarize yourself with the
APIs and concepts of CDAP as well as the CDAP Console using the example applications that are provided
with the SDK. Let's take a look at one of these:

.. include:: _examples/first-app.rst

.. _examples:

Other Example Applications
==========================

Congratulations on successfully building and running your first CDAP application.
The SDK includes these additional examples:

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


.. _convention:

Building and Running Applications
---------------------------------

.. highlight:: console

In the examples, we refer to the Standalone CDAP as "CDAP", and the
example code that is running on it as an "Application".


Building the Application
........................

From the example's project root, build an example with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

Starting CDAP
.............

Make sure an instance of CDAP is running and available.
From within the SDK root directory, this command will start the Standalone CDAP::

	$ ./bin/cdap.sh start

On Windows::

	> bin\cdap.bat start


Deploying an application
........................

Once CDAP is Started, you can deploy the example JAR by:

- Dragging and dropping the application JAR file (``example/target/<example>-<version>.jar``) onto the CDAP Console
  running at `http://localhost:9999/ <http://localhost:9999/>`__
- Use the *Load App* button found on the *Overview* of the CDAP Console to browse and upload the Jar.
- From the example's project root run the App Manager script:

   - Linux: ``$./bin/app-manager.sh --action deploy``
   - Windows: ``>bin\app-manager.bat deploy``

Starting an Application
.......................

Once the application is deployed,

- You can go to the Application view by clicking on the Application's name. Now you can *Start* or *Stop* the Process
  and Query of the application, or
- From the example's project root run the App Manager script:

    - Linux: ``$./bin/app-manager.sh --action start``
    - Windows: ``>bin\app-manager.bat start``

.. _stop-application:

Stopping an Application
.......................

- On the Application detail page of the CDAP Console, click the *Stop* button on both the Process and Query lists or
- From the example's project root run the App Manager script:

  - Linux: ``$./bin/app-manager.sh --action stop``
  - Windows: ``>bin\app-manager.bat stop``

.. include:: _examples/helloworld.rst
.. include:: _examples/wordcount.rst
.. include:: _examples/countrandom.rst
.. include:: _examples/purchase.rst
.. include:: _examples/sparkKMeans.rst
.. include:: _examples/sparkPageRank.rst

What's Next
===========

You can learn more about developing data application using CDAP by:

* Exploring the Web Analytics Application source code. It includes test cases that show unit-testing an
  application.
* Look at a CDAP case study: `Web Analytics using CDAP. <case-study.html>`__
* For a detailed understanding of what CDAP is capable of, read our `Programming Guide. <dev-guide.html>`__
