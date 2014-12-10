.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform WordCount Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-word-count:

==========
Word Count
==========

A Cask Data Application Platform (CDAP) Example demonstrating Flows, Datasets and Services.

Overview
========

This application receives words and sentences from a stream and uses flowlets to process them and
store the results and statistics in datasets.

  - The ``wordStream`` receives sentences, one event at a time.
  - The ``splitter`` flowlet reads sentences from stream and splits them into words, writes global statistics of the
    received words like "total words received" and "total length of words received" and emits each word to the
    ``counter`` flowlet  and each sentence (list of words) to the ``associator`` flowlet.
  - The ``associator`` flowlet receives the set of words and writes word associations to the ``wordAssocs`` dataset.
    For example, if we receive a sentence "welcome to CDAP", the word associations are
    {"welcome","to"} , {"welcome", "CDAP"}, and {"to","CDAP"}.
  - The ``counter`` flowlet receives a word and increments the count for this word, maintained in a key-value table and
    forwards this word to the ``unique`` flowlet.
  - The ``unique`` flowlet receives a word and updates the ``uniqueCount`` table, if it sees this word for the first time.

Let's look at some of these components, and then run the Application and see the results.

The Word Count Application
--------------------------

As in the other :ref:`examples,<examples-index>` the components
of the Application are tied together by the class ``WordCount``:

.. literalinclude:: /../../../cdap-examples/WordCount/src/main/java/co/cask/cdap/examples/wordcount/WordCount.java
   :language: java
   :lines: 27-


Data Storage
--------------------------

- ``wordStats`` stores the global statistics of total count of words and the total length of words received.
- ``wordCounts`` stores the word and the corresponding count in a key value table.
- ``uniqueCount`` is a custom dataset that stores the total count of unique words received so far.
- ``wordAssocs`` is a custom dataset that stores the count for word associations.

RetrieveCounts Service
--------------------------

The Service serves read requests for calculated statistics, word counts and associations.
It exposes these endpoints:

- ``/stats`` returns the total number of words, the number of unique words, and the average word length;
- ``/count/{word}`` returns the word count of a specified word and its word associations,
  up to the specified limit or a pre-set limit of ten if not specified;
- ``/assoc/{word1}/{word2}`` returns the top associated words (those with the highest counts).


Building and Starting
=================================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application and its components as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the Flow and Service as described below.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

Running CDAP Applications
============================================

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 9

Running the Example
===================

Running the Example
===================

Starting the Flow
------------------------------

Once the application is deployed:

- Click on the *Process* button in the left sidebar of the CDAP Console,
  then click ``WordCounter`` in the *Process* page to get to the
  Flow detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start flow WordCount.WordCounter``
    * - On Windows:
      - ``> bin\cdap-cli.bat start flow WordCount.WordCounter``    

Starting the Service
------------------------------

Once the application is deployed:

- Click on ``WordCount`` in the Overview page of the CDAP Console to get to the
  Application detail page, click ``RetrieveCounts`` in the *Service* pane to get to the
  Service detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start service WordCount.RetrieveCounts``
    * - On Windows:
      - ``> bin\cdap-cli.bat start service WordCount.RetrieveCounts``    

Injecting Sentences
------------------------------

In the Application's detail page, under *Process*, click on the *WordCounter* flow. This takes you to the flow details page.
Now click on the *wordStream* stream on the left side of the flow visualization, which brings up a pop-up window.
Enter a sentence such as "Hello CDAP" and click on the *Inject* button. After you close the pop-up window, you will see that the counter
for the stream increases to 1, the counters for the flowlets *splitter*` and *associator* increase to 1 and
the counters for the flowlets *counter* and *unique* increase to 2.
You can repeat this step to enter additional sentences.

Querying the Results
------------------------------

.. highlight:: console

If the Service has not already been started, you start it either through the
CDAP Console or via an HTTP request using the ``curl`` command::

  curl -v -X POST 'http://localhost:10000/v2/apps/WordCount/services/RetrieveCounts/start'

To query the ``RetrieveCounts`` service,
send a query via an HTTP request using the ``curl`` command. For example::

  curl -w '\n' -v 'http://localhost:10000/v2/apps/WordCount/services/RetrieveCounts/methods/count/CDAP'

**Note:** A version of ``curl`` that works with Windows is included in the CDAP Standalone
SDK in ``libexec\bin\curl.exe``

The word count and top-10 associations words for that word will be displayed in JSON
format (example reformatted to fit)::

  {
    "assocs": {
      "Hello": 1,
      "BigData":3,
      "Cask":5,
    },
    "count": 6,
    "word": "CDAP"
  }

You can also request other endpoints available in this Service, which described above.

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the steps:

**Stopping the Flow**

- Click on the *Process* button in the left sidebar of the CDAP Console,
  then click ``WordCounter`` in the *Process* page to get to the
  Flow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop flow WordCount.WordCounter``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop flow WordCount.WordCounter``    

**Stopping the Service**

- Click on ``WordCount`` in the Overview page of the CDAP Console to get to the
  Application detail page, click ``RetrieveCounts`` in the *Service* pane to get to the
  Service detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh stop service WordCount.RetrieveCounts``
    * - On Windows:
      - ``> bin\cdap-cli.bat stop service WordCount.RetrieveCounts``    
