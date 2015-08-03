.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform Wikipedia Pipeline Application
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _examples-wikipedia-data-pipeline:

==================
Wikipedia Pipeline
==================

A Cask Data Application Platform (CDAP) example demonstrating a typical data processing pipeline using CDAP Workflows.

Overview
========

This example demonstrates a CDAP application performing analysis on Wikipedia data using MapReduce and Spark programs
running within a CDAP Workflow: *WikipediaPipelineWorkflow*.

This example can be run in both online and offline modes.

- In the online mode, the MapReduceProgram *WikipediaDataDownloader* reads the stream *pageTitleStream*, each event of which
  is an element from the output of the
  `Facebook "Likes" API <https://developers.facebook.com/docs/graph-api/reference/v2.4/object/likes>`__.
  For each event, it tries to download Wikipedia data for the page using the
  `MediaWiki Wikipedia API <https://www.mediawiki.org/wiki/API:Main_page>`__. It stores the downloaded data in the
  KeyValueTable dataset *wikiData*.
  
- In the offline mode, it expects Wikipedia data formatted per the output of the MediaWiki API in the stream *wikiStream*.
  The MapReduce program *wikiDataToDataset* consumes this stream and stores it in the same KeyValueTable dataset
  *wikiData*. Data can be uploaded to the *wikiStream* using the CDAP CLI.

Once raw Wikipedia data is available from using either the online or offline modes, the
*WikipediaPipelineWorkflow* runs a MapReduce program *WikiContentValidatorAndNormalizer* that filters bad records from
the raw data, as well as normalizes it by converting the wikitext-formatted data to plain text. It then stores the
output in another KeyValueTable dataset *normalized*.

The *WikipediaPipelineWorkflow* then contains a fork, with two branches. One branch runs the Apache Spark program *SparkWikipediaAnalyzer*.
This program consumes normalized data and runs topic modeling on it using the Latent Dirichlet Allocation (LDA) algorithm. It stores its
output in the CDAP Table dataset *lda*, with one row for each iteration, and a column per topic containing the score for that topic.
The other branch contains a MapReduce program *TopNMapReduce* that consumes the normalized data and produces the top N words in the dataset *topn*.

Let's look at some of these components, and then run the application and see the results.

The WikipediaPipeline Application
---------------------------------

As in the other `examples <index.html>`__, the components
of the application are tied together by the class ``WikipediaPipelineApp``:

.. literalinclude:: /../../../cdap-examples/WikipediaPipeline/src/main/java/co/cask/cdap/examples/wikipedia/WikipediaPipelineApp.java
   :language: java
   :lines: 24-57

This application demonstrates:

- The use of assigning unique names, as the same MapReduce (*StreamToDataset*) is used twice in the workflow
  (*WikipediaPipelineWorkflow*) with two different names.
  
- The use of Workflow Tokens in:

    - Condition Predicates
    - Setting MapReduce program configuration (setting it based on values in the token)
    - ``map()``/``reduce()`` functions (read-only, no updates)
    - Spark Programs (reading from/writing to workflow token, adding Spark Accumulators to workflow token)
    - Assertions in application unit tests


.. |example| replace:: WikipediaPipeline
.. include:: building-starting-running-cdap.txt

Running the Example
===================

.. highlight:: console

Injecting data
--------------
The *pageTitleStream* consumes events in the format returned by the Facebook "Likes" Graph API.

- Inject a file of Facebook "Likes" data to the stream *pageTitleStream* by running this command from the Standalone
  CDAP SDK directory, using the CDAP Command Line Interface::

    $ cdap-cli.sh load stream pageTitleStream examples/WikipediaPipeline/resources/fb-likes.txt
    Successfully sent stream event to stream 'pageTitleStream'


The *wikiStream* consumes events in the format returned by the MediaWiki Wikipedia API.

- Inject a file of "Wikipedia" data to the stream *pageTitleStream* by running this command from the Standalone
  CDAP SDK directory, using the Command Line Interface::

    $ cdap-cli.sh load stream wikiStream examples/WikipediaPipeline/resources/wikipedia-data.txt
    Successfully sent stream event to stream 'wikiStream'

Starting the Workflow
---------------------

There are three ways to start the *WikipediaPipelineWorkflow* program:

#. Go to the *WikipediaPipeline* `application overview page
   <http://localhost:9999/ns/default/apps/WikipediaPipeline/overview/status>`__,
   click ``WikipediaPipelineWorkflow`` to get to the workflow detail page, then click the *Start* button; or

#. Send a query via an HTTP request using the ``curl`` command::

    curl -w'\n' -v \
      http://localhost:10000/v3/namespaces/default/apps/WikipediaPipeline/workflows/WikipediaPipelineWorkflow/start

#. From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start workflow WikipediaPipeline.WikipediaPipelineWorkflow
  
    Successfully started flow 'WikipediaPipelineWorkflow' of application 'WikipediaPipeline' with stored runtime arguments '{}'

You can also (optionally) specify these runtime arguments for the *WikipediaPipelineWorkflow*:

- *min.pages.threshold*: Threshold for the number of pages to exist in the *pageTitleStream* for the workflow to proceed.
  Defaults to 10.
- *mode*: Set this to 'online' when you wish to download Wikipedia data over the Internet.
  Defaults to offline, in which the workflow expects Wikipedia data to be in the *wikiStream*.
- *stopwords.file*: The path to the file containing stopwords to filter in the *SparkWikipediaAnalyzer* program.
  If unspecified, no words are considered as stopwords.
- *vocab.size*: The size of the vocabulary for the *SparkWikipediaAnalyzer* program. Defaults to 1000.
- *topn.rank*: The number of top words to produce in the *TopNMapReduce* program. Defaults to 10.
- *num.reduce.tasks*: The number of reduce tasks to set for the *TopNMapReduce* program. Defaults to 1.

Retrieving the Results
----------------------
The *WikipediaPipelineApp* contains a CDAP Service *WikipediaService* that can retrieve results from the analysis
performed by the *WikipediaPipelineWorkflow*.

To start the service:

- Go to the *WikipediaPipelineApp* `application overview page
  <http://localhost:9999/ns/default/apps/WikipediaPipelineApp/overview/status>`__,
  click ``WikipediaService`` to get to the service detail page, then click the *Start* button; or

- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start service WikipediaPipelineApp.WikipediaService

The *WikipediaService* exposes these REST APIs:

- Retrieve the list of topics generated by the *SparkWikipediaAnalyzer* program::

    $ curl -w'\n' localhost:10000/v3/namespaces/default/apps/WikipediaPipelineApp/services/WikipediaService/methods/v1/functions/lda/topics

- Retrieve the details (terms and term weights) for a given topic::

    $ curl -w'\n' localhost:10000/v3/namespaces/default/apps/WikipediaPipelineApp/services/WikipediaService/methods/v1/functions/lda/topics/{topic}

- Retrieve the output of the *TopNMapReduce* program::

    $ curl -w'\n' localhost:10000/v3/namespaces/default/apps/WikipediaPipelineApp/services/WikipediaService/methods/v1/functions/topn/words


Stopping and Removing the Application
=====================================
Once done, you can stop the application as described in :ref:`Stopping an Application 
<cdap-building-running-stopping>`. Here is an example-specific description of the steps:

**Stopping the Workflow**

- Go to the *WikipediaPipelineWorkflow* `application overview page
  <http://localhost:9999/ns/default/apps/WikipediaPipelineWorkflow/overview/status>`__,
  click ``WikipediaPipelineWorkflow`` to get to the workflow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop workflow WikipediaPipeline.WikipediaPipelineWorkflow


**Stopping the Service**

- Go to the *WikipediaService* `application overview page
  <http://localhost:9999/ns/default/apps/WikipediaService/overview/status>`__,
  click ``WikipediaService`` to get to the service detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop service WikipediaPipeline.WikipediaService


**Removing the Application**

You can now remove the application as described in :ref:`Removing an Application <cdap-building-running-removing>`, or:

- Go to the *WikipediaPipeline* `application overview page
  <http://localhost:9999/ns/default/apps/WikipediaPipeline/overview/status>`__,
  click the *Actions* menu on the right side and select *Manage* to go to the Management pane for the application,
  then click the *Actions* menu on the right side and select *Delete* to delete the application; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh delete app WikipediaPipeline

