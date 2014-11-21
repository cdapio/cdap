.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform WordCount Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-word-count:

==========
Word Count
==========

A Cask Data Application Platform (CDAP) Example demonstrating Flows, Datasets and Procedures.

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

Let's look at some of these elements, and then run the Application and see the results.

The Word Count Application
--------------------------

As in the other :ref:`examples,<examples-index>` the components
of the Application are tied together by the class ``WordCount``:

.. literalinclude:: /../../../cdap-examples/WordCount/src/main/java/co/cask/cdap/examples/wordcount/WordCount.java
   :language: java
   :lines: 26-


Data Storage
--------------------------

- ``wordStats`` stores the global statistics of total count of words and the total length of words received.
- ``wordCounts`` stores the word and the corresponding count in a key value table.
- ``uniqueCount`` is a custom dataset that stores the total count of unique words received so far.
- ``wordAssocs`` is a custom dataset that stores the count for word associations.

RetrieveCounts Procedure
--------------------------

This Procedure has three methods:

- ``getStats()``: Returns global statistics such as "total words received", "total length of words received" and "average length of words".
- ``getCount()``: Given a word, this returns the total count of occurrences and the top-10 associated words for this word.
- ``getAssoc()``: Given a pair, "word1" and "word2", this returns the association count for the pair.


Building and Starting
=================================

- You can either build the example (as described `below
  <#building-an-example-application>`__) or use the pre-built JAR file included in the CDAP SDK.
- Start CDAP, deploy and start the application as described below in 
  `Running CDAP Applications`_\ .
  Make sure you start the flow and procedure as described.
- Once the application has been deployed and started, you can `run the example. <#running-the-example>`__

Running CDAP Applications
============================================

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 9

Running the Example
===================

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

If the Procedure has not already been started, you start it either through the
CDAP Console or via an HTTP request using the ``curl`` command::

	curl -v -X POST 'http://localhost:10000/v2/apps/WordCount/procedures/RetrieveCounts/start'

There are two ways to query the  ``RetrieveCounts`` procedure:

1. Send a query via an HTTP request using the ``curl`` command. For example::

	curl -v -d '{"word": "CDAP"}' \
	  'http://localhost:10000/v2/apps/WordCount/procedures/RetrieveCounts/methods/getCount'; echo

  On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the project SDK.

#. Click on the ``RetrieveCounts`` in the Application page of the Console to get to the
   Procedure dialogue. Type in the method name ``getCount``, and enter a word in the parameters
   field, such as::

	  { "word" : "CDAP" }

   Then click the *Execute* button. 
   
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

From the Console dialog, you can try executing other methods available in this procedure:

- ``getStats`` - This returns statistics such as "average length", "total words received" and so on.
- ``getAssoc`` - You need to provide two words as parameters to retrieve their association
  count; example: ``{"word1":"Hello", "word2":"CDAP"}``.

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. <#stopping-an-application>`__
