.. :Author: Cask Data, Inc.
   :Description: Cask Data Application Platform Examples
   :copyright: Copyright © 2014 Cask Data, Inc.

=======================================
Cask Data Application Platform Examples
=======================================

**Example Applications Demonstrating Cask Data Application Platform (CDAP) Features**

Overview
========

We've selected three projects from our collection of examples to serve as
an introduction to the important features of the Cask Data Application Platform.

By reading, building and playing with these, you will look at these basic
elements of CDAP:

- Applications
- Streams
- Flows
- Flowlets
- Datasets
- Custom Datasets
- Batch Processing
- MapReduce

These examples (and more!) are included with our
`software development kit <http://cask.co/download>`__.

`ResponseCodeAnalytics <responseCodeAnalytics.html>`_
======================================================================
This is a simple application for real-time streaming log analysis—computing 
the number of occurrences of each HTTP status code by processing Apache access log data. 
The example introduces the basic constructs of the Cask Data Application Platform programming paradigm:
**Applications**, **Streams**, **Flows**, **Flowlets**, **Procedures** and **Datasets**.

`PageViewAnalytics <pageViewAnalytics.html>`_
==============================================================
This example demonstrates use of **custom Datasets** and **batch processing** in an Application.
It takes data from Apache access logs,
parses them and save the data in a custom Dataset. It then queries the results to find,
for a specific URI, pages that are requesting that page and the distribution of those requests.

The custom Dataset shows how you include business logic in the definition of a Dataset.
By doing so, the Dataset does more than just store or convert data—it
expresses methods that can perform valuable operations, such as counting and tabulating results
based on the Dataset's knowledge of its underlying data.

`TrafficAnalytics <trafficAnalytics.html>`_
=======================================================================
This example shows another application of streaming log analysis, but this time it
computes the aggregate number of HTTP requests on an hourly basis
in each hour of the last twenty-four hours, processing in real-time Apache access log data.
 
The application expands on the `ResponseCodeAnalytics`_ example to show how to use a **MapReduce** Job.

`Purchase <purchase.html>`_
=======================================================================
In this single example, we demonstrate all the different elements of CDAP:
Streams, Flow, Flowlets, Datasets, Queries, Procedures, MapReduce jobs, and Workflows 
in a single Application.
The application uses a scheduled MapReduce job and Workflow to read from one ObjectStore dataset
and write to another.

`SparkKMeans <sparkKMeans.html>`_
=======================================================================
This example shows another application of streaming analysis, computing the centers of points from an input stream using the KMeans Clustering method.

The application demonstrates using a **Spark** Program.

`SparkPageRank <sparkPageRank.html>`_
=======================================================================
This example shows another application of streaming analysis, computing the page rank based on information about backlink URLs.

The application demonstrates using a **Spark** Program.


Where to Go Next
================
Now that you've seen some examples using CDAP, take a look at:

- `Cask Data Application Platform Programming Guide <../programming.html>`__,
  an introduction to programming applications for CDAP.
