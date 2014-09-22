# Cask Data Application Platform  Examples

This /examples directory contains example apps for the Cask Data Application Platform (CDAP). 
They are not compiled as part of the master build, and they should only depend 
on the API jars (plus their dependencies). However, they may also be provided 
in their compiled forms as JAR files in a release.

Additional information about selected examples is available at the Cask website:

>   http://docs.cask.co/cdap/current/en/examples/


# Building

Each example comes with a Maven pom.xml file. To build, install Maven, and from the
/examples directory prompt, enter:

>   mvn clean package


# List of Example Apps

## CountRandom

- Generates random numbers between 0 and 9999.
- For each number *i*, generates i%10000, i%1000, i%100, i%10.
- Increments the counter for each number.

## HelloWorld

- This is a simple HelloWorld example that uses one Stream, one Dataset, one Flow and one
  Procedure.
- A Stream, to send names to.
- A Flow, with a single Flowlet that reads the Stream and stores each name in a KeyValueTable.
- A Procedure, that reads the name from the KeyValueTable and prints "Hello [Name]!"

## Purchase

- An app that uses scheduled MapReduce Workflows to read from one ObjectStore Dataset
  and write to another and demonstrates using ad-hoc SQL queries.

  - Send sentences of the form "Tom bought 5 apples for $10" to the purchaseStream.
  - The PurchaseFlow reads the purchaseStream and converts every input String into a
    Purchase object and stores the object in the purchases Dataset.
  - When scheduled by the PurchaseHistoryWorkFlow, the PurchaseHistoryBuilder MapReduce
    job reads the purchases Dataset, creates a purchase history, and stores the purchase
    history in the history Dataset every morning at 4:00 A.M. You can manually (in the
    Process screen in the CDAP Console) or programmatically execute the 
    PurchaseHistoryBuilder MapReduce job to store customers' purchase history in the
    history Dataset.
  - Execute the PurchaseQuery procedure to query the history Dataset to discover the 
    purchase history of each user.
  - You can use SQL to formulate ad-hoc queries over the history Dataset. This is done by
    a series of ``curl`` calls, as described in the RESTful API section of the Developer Guide.

- Note: Because by default the PurchaseHistoryWorkFlow process doesn't run until 4:00 A.M.,
  you'll have to wait until the next day (or manually or programmatically execute the
  PurcaseHistoryBuilder) after entering the first customers' purchases or the PurchaseQuery
  will return a "not found" error.
- For more information, see http://cask.co/docs/cdap/current/en/examples/.

## SparkKMeans

- An application that demonstrates streaming text analysis using a Spark program.
  It calculates the centers of points from an input stream using the KMeans Clustering method.  

## SparkPageRank

- An application that demonstrates streaming text analysis using a Spark program.
- It computes the page rank of URLs from an input stream.

## WordCount

- A simple application that counts words and tracks word associations and unique words
  seen on the Stream. It demonstrates the power of using Datasets and how they can be used
  to simplify storing complex data.


Cask is a trademark of Cask Data, Inc. All rights reserved.

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions
and limitations under the License.
