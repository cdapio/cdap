.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform SparkPageRank Application
    :copyright: Copyright © 2014-2016 Cask Data, Inc.

.. _examples-spark-page-rank:

=======================
Spark Page Rank Example
=======================

A Cask Data Application Platform (CDAP) example demonstrating Spark and MapReduce in a Workflow to compute page ranks.

Overview
========

This example demonstrates Spark and MapReduce performing streaming log analysis, computing the page rank based on information about backlink URLs

Data from a sample file is sent to CDAP by a CDAP CLI command
to the *backlinkURLStream*, which stores the URL pair event in its entirety.

After these events are streamed, they are taken up by the *SparkPageRankProgram*, which
goes through the entries, calculates page rank and tabulates results in an ObjectStore dataset, *ranks*.

A MapReduce job uses the output of the Spark program from the *ranks* dataset,
computes the total number of pages for every unique page rank, and then tabulates
the results in another ObjectStore dataset, *rankscount*.

The *PageRankWorkflow* ties the Spark and MapReduce to run sequentially in this application.

Once the application completes, you can query the *ranks* dataset by using the ``rank`` endpoint of the *SparkPageRankService*.
It will send back a string result with page rank based on the ``url`` query parameter. You can also query the
*rankscount* dataset by using ``total`` endpoint. It will send the total number of pages for the queried page rank as a string.

Let's look at some of these components, and then run the application and see the results.

The *SparkPageRank* Application
-------------------------------

As in the other `examples <index.html>`__, the components
of the application are tied together by the class ``SparkPageRankApp``:

.. literalinclude:: /../../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java
   :language: java
   :lines: 55-96
   :append: ...

The *ranks* and *rankscount* ObjectStore Data Storage
-----------------------------------------------------

The calculated page rank data is stored in an ObjectStore dataset, *ranks*,
with the total number of pages for a page rank stored in an additional ObjectStore dataset, *rankscount*.

The *SparkPageRankService* Service
----------------------------------

This ``SparkPageRankService`` service has a ``rank`` endpoint to obtain the page rank of a given URL.
It also has a ``total`` endpoint to obtain the total number of pages with a given page rank.

Memory Requirements
-------------------
When a Spark program is running inside a workflow, the memory requirements configured for the Spark program may need increasing beyond the defaults:

.. literalinclude:: /../../../cdap-examples/SparkPageRank/src/main/java/co/cask/cdap/examples/sparkpagerank/SparkPageRankApp.java
    :language: java
    :lines: 120-121
    :dedent: 6

.. Building and Starting
.. =====================
.. |example| replace:: SparkPageRank
.. |example-italic| replace:: *SparkPageRank*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <SparkPageRank>`

.. include:: _includes/_building-starting-running.txt


Running the Example
===================

.. Starting the Service

.. |example-service| replace:: SparkPageRankService
.. |example-service-italic| replace:: *SparkPageRankService*

.. include:: _includes/_starting-service.txt

Injecting URL Pairs
-------------------

Inject a file of URL pairs to the stream *backlinkURLStream* by running this command from the
Standalone CDAP SDK directory, using the Command Line Interface:
  
.. tabbed-parsed-literal::

  $ cdap-cli.sh load stream backlinkURLStream examples/SparkPageRank/resources/urlpairs.txt
  
  Successfully loaded file to stream 'backlinkURLStream'

Starting the Workflow
---------------------
.. |example-workflow| replace:: PageRankWorkflow
.. |example-workflow-italic| replace:: *PageRankWorkflow*

The workflow must be started with a runtime argument ``spark.SparkPageRankProgram.args``
that specifies the number of iterations. By default, this is 10; in this example, we'll
use ``3`` as the value.

- Using the CDAP UI, go to the |application-overview|,
  click |example-workflow-italic| to get to the workflow detail page, set the runtime
  arguments using ``spark.SparkPageRankProgram.args`` as the key and ``3`` as the value, then click
  the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. tabbed-parsed-literal::

    $ cdap-cli.sh start workflow |example|.\ |example-workflow| "spark.SparkPageRankProgram.args='3'"
    
    Successfully started workflow '|example-workflow|' of application '|example|' 
    with provided runtime arguments 'spark.SparkPageRankProgram.args=3'
      
- Or, send a query via an HTTP request using the ``curl`` command:

  .. tabbed-parsed-literal::

    $ curl -w"\n" -X POST -d "{spark.SparkPageRankProgram.args='3'}" \
    "http://localhost:10000/v3/namespaces/default/apps/|example|/workflows/|example-workflow|/start"
    

Querying the Results
--------------------

To query the *ranks* ObjectStore through the ``SparkPageRankService``, 
you can use the Command Line Interface:

.. tabbed-parsed-literal::

  $ cdap-cli.sh call service SparkPageRank.SparkPageRankService POST "rank" body "{'url':'http://example.com/page1'}"
  
  10

You can also send a query via an HTTP request using the ``curl`` command. For example:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X POST -d "{'url':'http://example.com/page1'}" \
  "http://localhost:10000/v3/namespaces/default/apps/SparkPageRank/services/SparkPageRankService/methods/rank"
  
  10  

Similarly, to query the *rankscount* ObjectStore using the ``SparkPageRankService`` and get the total number of
pages with a page rank of 10, you can do the following:

Using the Command Line Interface:

.. tabbed-parsed-literal::

  $ cdap-cli.sh call service SparkPageRank.SparkPageRankService GET 'total/10'
  
  48

Using ``curl``:

.. tabbed-parsed-literal::

  $ curl -w"\n" -X GET "http://localhost:10000/v3/namespaces/default/apps/SparkPageRank/services/SparkPageRankService/methods/total/10"
  
  48


.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-removing-application-title.txt

.. include:: _includes/_stopping-workflow.txt

.. include:: _includes/_stopping-service.txt

.. include:: _includes/_removing-application.txt
