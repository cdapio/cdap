.. :Author: Continuuity, Inc.
   :Description: Continuuity Reactor Examples

============================
Examples
============================

Example Applications demonstrating Continuuity Reactor Features

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

Overview
========

We've selected three projects from our collection of examples to serve as
an introduction to the important features of Continuuity Reactor.

By reading, building and playing with these three, you will look at all the basic
elements of Continuuity Reactor:

- Applications
- Streams
- Flows
- Flowlets
- DataSets
- Custom DataSets
- Batch Processing
- MapReduce

Additional examples are included with our
`software development kit <http://continuuity.com/download>`__.

`ResponseCodeAnalytics <ResponseCodeAnalytics/index.html>`_
======================================================================
This is a simple application for real-time streaming log analysis—computing 
the number of occurrences of each HTTP status code by processing Apache access log data. 
The example introduces the basic constructs of the Continuuity Reactor programming paradigm:
**Applications**, **Streams**, **Flows**, **Flowlets**, **Procedures** and **DataSets**.

`PageViewAnalytics <PageViewAnalytics/index.html>`_
==============================================================
This example demonstrates use of **custom DataSets** and **batch processing** in an Application.
It takes data from Apache access logs,
parses them and save the data in a custom DataSet. It then queries the results to find,
for a specific URI, pages that are requesting that page and the distribution of those requests.

The custom DataSet shows how you include business logic in the definition of a DataSet.
By doing so, the DataSet does more than just store or convert data—it
expresses methods that can perform valuable operations, such as counting and tabulating results
based on the DataSet's knowledge of its underlying data.

`TrafficAnalytics <TrafficAnalytics/index.html>`_
=======================================================================
This example shows another application of streaming log analysis, but this time it
computes the aggregate number of HTTP requests on an hourly basis
in each hour of the last twenty-four hours, processing in real-time Apache access log data.
 
The application expands on the `ResponseCodeAnalytics`_ example to show how to use a **MapReduce** job.

Where to Go Next
================
Now that you've seen some examples using Continuuity Reactor, take a look at:

- :doc:`Continuuity Reactor Programming Guide </programming>`,
  an introduction to programming applications for the Continuuity Reactor.
