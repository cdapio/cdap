.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform WordCount Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-word-count:

==========
Word Count
==========

A Cask Data Application Platform (CDAP) example demonstrating flows, datasets and services.

Overview
========

This application receives words and sentences from a stream and uses flowlets in a flow to
process the sentences and store the results and statistics in datasets.

  - The ``wordStream`` receives sentences, one event at a time.
  - The ``splitter`` flowlet reads sentences from stream and splits them into words, writes global statistics of the
    received words such as "total words received" and "total length of words received" and emits each word to the
    ``counter`` flowlet  and each sentence (list of words) to the ``associator`` flowlet.
  - The ``associator`` flowlet receives the set of words and writes word associations to the ``wordAssocs`` dataset.
    For example, if we receive a sentence ``"Welcome to CDAP"``, the word associations are
    ``{"Welcome", "to"}`` , ``{"Welcome", "CDAP"}``, and ``{"to", "CDAP"}``.
  - The ``counter`` flowlet receives a word, increments the count for the word |---| maintained in a key-value table |---| and
    forwards the word to the ``unique`` flowlet.
  - The ``unique`` flowlet receives a word and updates the ``uniqueCount`` table, if it is seeing this word for the first time.

Let's look at some of these components, and then run the Application and see the results.

The Word Count Application
--------------------------

As in the other :ref:`examples <examples-index>`, the components
of the Application are tied together by the class ``WordCount``:

.. literalinclude:: /../../../cdap-examples/WordCount/src/main/java/co/cask/cdap/examples/wordcount/WordCount.java
   :language: java
   :lines: 27-


Data Storage
------------

- ``wordStats`` stores the global statistics of total count of words and the total length of words received.
- ``wordCounts`` stores the word and the corresponding count in a key value table.
- ``uniqueCount`` is a custom dataset that stores the total count of unique words received so far.
- ``wordAssocs`` is a custom dataset that stores the count for word associations.

.. _word-count-service-requests:

RetrieveCounts Service
----------------------

The service serves read requests for calculated statistics, word counts and associations.
It exposes these endpoints:

- ``/stats`` returns the total number of words, the number of unique words, and the average word length;
- ``/count/{word}`` returns the word count of a specified word and its word associations,
  up to the specified limit or a pre-set limit of ten if not specified;
- ``/assoc/{word1}/{word2}`` returns the top associated words (those with the highest counts).


Building and Starting
=====================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application and its components as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the flow and service as described below.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

Running CDAP Applications
=========================

.. |example| replace:: WordCount

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 11

Running the Example
===================

Starting the Flow
-----------------

Once the application is deployed:

- Click on the *Process* button in the left sidebar of the CDAP UI,
  then click ``WordCounter`` in the *Process* page to get to the
  flow detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start flow WordCount.WordCounter``
    * - On Windows:
      - ``> bin\cdap-cli.bat start flow WordCount.WordCounter``    

Starting the Service
--------------------

Once the application is deployed:

- Click on ``WordCount`` in the Overview page of the CDAP UI to get to the
  Application detail page, click ``RetrieveCounts`` in the *service* pane to get to the
  service detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. list-table::
    :widths: 20 80
    :stub-columns: 1

    * - On Linux:
      - ``$ ./bin/cdap-cli.sh start service WordCount.RetrieveCounts``
    * - On Windows:
      - ``> bin\cdap-cli.bat start service WordCount.RetrieveCounts``    

Injecting Sentences
-------------------

In the Application's detail page, under *Process*, click on the *WordCounter* flow. This takes you to the flow details page.
Now click on the *wordStream* stream on the left side of the flow visualization, which brings up a pop-up window.
Enter a sentence such as "Hello CDAP" and click on the *Inject* button. After you close the pop-up window, you will see that the counter
for the stream increases to 1, the counters for the flowlets *splitter*` and *associator* increase to 1 and
the counters for the flowlets *counter* and *unique* increase to 2.
You can repeat this step to enter additional sentences.

Querying the Results
--------------------

.. highlight:: console

If the service has not already been started, you start it either through the
CDAP UI or via an HTTP request using the ``curl`` command::

  curl -w'\n' -X POST 'http://localhost:10000/v3/namespaces/default/apps/WordCount/services/RetrieveCounts/start'

To query the ``RetrieveCounts`` service,
send a query via an HTTP request using the ``curl`` command. For example::

  curl -w'\n' 'http://localhost:10000/v3/namespaces/default/apps/WordCount/services/RetrieveCounts/methods/count/CDAP'

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

You can also make requests to the other endpoints available in this service, as 
:ref:`described above <word-count-service-requests>`.

Stopping and Removing the Application
-------------------------------------
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the steps:

**Stopping the Flow**

- Go to the *WordCount* `application overview page 
  <http://localhost:9999/ns/default/apps/WordCount/overview/status>`__,
  click ``WordCounter`` to get to the flow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop flow WordCount.WordCounter 

**Stopping the Service**

- Go to the *WordCount* `application overview page 
  <http://localhost:9999/ns/default/apps/WordCount/overview/status>`__,
  click ``RetrieveCounts`` to get to the flow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop service WordCount.RetrieveCounts 

**Removing the Application**

You can now remove the application as described above, `Removing an Application <#removing-an-application>`__, or:

- Go to the *WordCount* `application overview page 
  <http://localhost:9999/ns/default/apps/WordCount/overview/status>`__,
  click the *Actions* menu on the right side and select *Manage* to go to the Management pane for the application,
  then click the *Actions* menu on the right side and select *Delete* to delete the application; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh delete app WordCount

