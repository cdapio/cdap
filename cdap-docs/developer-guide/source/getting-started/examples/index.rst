.. :author: Cask Data, Inc.
   :description: Examples
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Additional Examples
============================================


.. toctree::
   :maxdepth: 1
   :titlesonly:
   
   Hello World <hello-world>
   Word Count <word-count>
   Count Random <count-random>
   Purchase <purchase>
   Spark K-Means <spark-k-means>
   Spark Page Rank <spark-page-rank>
   Web Analytics <web-analytics>


The SDK includes these additional examples:

.. list-table::
  :widths: 15 60
  :header-rows: 1

  * - Example Name
    - Description
  * - :doc:`Hello World<hello-world>`
    - A simple HelloWorld App that's written using CDAP. It introduces how the elements Stream, Dataset, Flow,
      and Procedure are used in a CDAP application.
  * - :doc:`Word Count<word-count>`
    - A simple application that counts words, and tracks word associations and unique words seen on the Stream.
      It demonstrates the power of using datasets and how they can be employed to simplify storing complex data.
  * - :doc:`Count Random<count-random>`
    - An application that demonstrates the ``@Tick`` feature of Flows. It uses a tick method to generate random
      numbers which are then counted by downstream Flowlets.
  * - :doc:`Purchase<purchase>`
    - This example demonstrates use of each of the CDAP elements—Streams, Flows, Flowlets, Datasets, Queries,
      Procedures, MapReduce, Workflows, and Services—in a single Application.
      A Flow receives events from a Stream, each event describing a purchase ("John bought 5 apples for $2");
      the Flow processes the events and stores them in a Dataset. A Mapreduce Job reads the Dataset, compiles
      the purchases for each customer into a purchase history and stores the histories in a second Dataset.
      The purchase histories can then be queried either through a Procedure or an ad-hoc SQL query.
  * - :doc:`Spark K-Means<spark-k-means>`
    - An application that demonstrates streaming text analysis using a Spark program. It calculates the centers
      of points from an input stream using the K-Means Clustering method.
  * - :doc:`Spark Page Rank<spark-page-rank>`
    - An application that demonstrates streaming text analysis using a Spark program. It computes the page rank
      of URLs from an input stream.
  * - :doc:`Web Analytics<web-analytics>`
    - An application to generate statistics and to provide insights about web usage through the analysis
      of web traffic.


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

Before running the Example Applications, check that an instance of CDAP is running and available.
From within the SDK root directory, this command will start the Standalone CDAP::

	$ ./bin/cdap.sh start

On Windows::

	> bin\cdap.bat start

If you can reach the CDAP Console through a browser at `http://localhost:9999/ <http://localhost:9999/>`__, CDAP is running.

Deploying an application
........................

Once CDAP is Started, you can deploy the example JAR by any of these methds:

- Dragging and dropping the application JAR file (``example/target/<example>-<version>.jar``) onto the CDAP Console
  running at `http://localhost:9999/ <http://localhost:9999/>`__; or
- Use the *Load App* button found on the *Overview* of the CDAP Console to browse and upload the Jar; or
- From the example's project root run the App Manager script:

  - Linux: ``$./bin/app-manager.sh --action deploy``
  - Windows: ``>bin\app-manager.bat deploy``

Starting an Application
.......................

Once the application is deployed:

- You can go to the Application view by clicking on the Application's name. Now you can *Start* or *Stop* the Process
  and Query of the application; or
- From the example's project root run the App Manager script:

  - Linux: ``$./bin/app-manager.sh --action start``
  - Windows: ``>bin\app-manager.bat start``

Stopping an Application
.......................

Once the application is deployed:

- On the Application detail page of the CDAP Console, click the *Stop* button on both the Process and Query lists; or
- From the example's project root run the App Manager script:

  - Linux: ``$./bin/app-manager.sh --action stop``
  - Windows: ``>bin\app-manager.bat stop``


What's Next
===========

You can learn more about developing data application using CDAP by:

- Exploring the Web Analytics Application source code. It includes test cases that show unit-testing an
  application.
- Look at a CDAP case study: `Web Analytics using CDAP. <case-study.html>`__
- For a detailed understanding of what CDAP is capable of, read our `Programming Guide. <dev-guide.html>`__
