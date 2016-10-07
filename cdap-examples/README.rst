=======================================
Cask Data Application Platform Examples
=======================================

This /examples directory contains example apps for the Cask Data Application Platform
(CDAP). They are not compiled as part of the master build, and they should only depend on
the API jars (plus their dependencies). However, they may also be provided in their
compiled forms as JAR files in a release.

Additional information about these examples is available at the Cask documentation website:

http://docs.cask.co/cdap/@@project.version@@/en/examples-manual/examples/index.html


Building
========

Each example comes with a Maven pom.xml file. To build, install Maven, and from the
/examples directory prompt, enter::

  $ mvn clean package
  
To build while skipping tests, use::

  $ mvn clean package -DskipTests


List of Example Apps
====================

ClicksAndViews
--------------
- Demonstrates a reduce-side join across two streams using a MapReduce program.

CountRandom
-----------
- Generates random numbers between 0 and 9999.
- For each number *i*, generates i%10000, i%1000, i%100, i%10.
- Increments the counter for each number.
- Demonstrates the ``@Tick`` feature of flows.

DataCleansing
-------------
- Demonstrates incrementally consuming partitions of a partitioned fileset using MapReduce.
      
FileSetExample
--------------
- Illustrates how to use the FileSet dataset in applications.
- Uses a Service that uploads files in—or downloads files to—a FileSet.
- Includes a MapReduce that implements the classic WordCount example. The input and
  output paths of the MapReduce can be configured through runtime arguments.

HelloWorld
----------
- This is a simple HelloWorld example that uses a stream, a dataset, a flow, and a
  service.
- A dataset created using a KeyValueTable.
- A flow with a single flowlet that reads from the stream and stores each name in the KeyValueTable.
- A service that reads the name from the KeyValueTable and responds with "Hello [Name]!"

LogAnalysis
-----------
- Demonstrates Spark and MapReduce running in parallel inside a workflow.
- Shows the use of forks within workflows.

Purchase
--------
- An example application that reads purchase data from a stream, processes the data via a workflow,
  and makes it available via ad-hoc querying and the RESTful interface of a service. It
  uses a scheduled workflow to start a MapReduce program that reads from one ObjectStore dataset
  and writes to another. The app demonstrates using custom datasets and ad-hoc SQL queries.

  - Send sentences of the form "Tom bought 5 apples for $10" to the *purchaseStream*.
  - The ``PurchaseFlow`` reads the *purchaseStream* and converts every input String into a
    ``Purchase`` object and stores the object in the *purchases* dataset.
  - When scheduled by the *PurchaseHistoryWorkFlow*, the *PurchaseHistoryBuilder* MapReduce
    program reads the *purchases* dataset, creates a purchase history, and stores the purchase
    history in the *history* dataset every morning at 4:00 A.M. You can manually (in the
    application's pages in the CDAP-UI) or programmatically execute the 
    *PurchaseHistoryBuilder* MapReduce to store customers' purchase history in the
    *history* dataset.
  - Request the ``PurchaseHistoryService`` retrieve from the *history* dataset the
    purchase history of a user.
  - You can use SQL to formulate ad-hoc queries over the *history* dataset. This is done by
    a series of ``curl`` calls, as described in the RESTful API section of the Developer Guide.

  - Note: Because the *PurchaseHistoryWorkFlow* process is scheduled to run at 4:00 A.M.,
    you'll have to wait until the next day (or manually or programmatically execute the
    *PurcaseHistoryBuilder*) after entering the first customers' purchases or the *PurchaseQuery*
    will return a "not found" error.

SpamClassifier
--------------
- Demonstrates a Spark Streaming application.
- Classifies Kafka messages as either "spam" or "ham" (not "spam")
- Based on a trained Spark MLlib NaiveBayes model.

SparkKMeans
-----------
- An application that demonstrates streaming text analysis using a Spark program.
- It calculates the centers of points from an input stream using the KMeans Clustering
  method.

SparkPageRank
-------------
- An application that demonstrates text analysis using Spark and MapReduce programs.
- It computes the page rank of URLs from an input stream.

SportResults
------------
- An application that illustrates the use of partitioned File sets.
- It loads game results into a File set partitioned by league and season, and processes
  them with MapReduce.

StreamConversion
----------------
- An application that illustrates the use of time-partitioned File sets.
- It periodically converts a stream into partitions of a File set, which can be read by
  SQL queries.

UserProfiles
------------
- An application that demonstrates column-level conflict detection using the example of
  updating of user profiles in a dataset.

WebAnalytics
------------
- An application to generate statistics and to provide insights about web usage through
  the analysis of web traffic.

WikipediaPipeline
-----------------
- An application that performs analysis on Wikipedia data using MapReduce and Spark programs
  running within a CDAP workflow: *WikipediaPipelineWorkflow*.
      
WordCount
---------
- A simple application that counts words and tracks word associations and unique words
  seen on the stream. 
- It demonstrates the power of using datasets and how they can be used to simplify storing
  complex data.
- It uses a configuration class to configure the application at deployment time.


License and Trademarks
======================

Cask is a trademark of Cask Data, Inc. All rights reserved.

Copyright © 2014-2016 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions
and limitations under the License.
