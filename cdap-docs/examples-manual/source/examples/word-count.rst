.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform WordCount Application
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-word-count:

==========
Word Count
==========

A Cask Data Application Platform (CDAP) example demonstrating flows, datasets, services,
and configuring an application at deployment time.


Overview
========
This application receives words and sentences from a stream and uses flowlets in a flow to
process the sentences and store the results and statistics in datasets.

- The ``wordStream`` receives sentences, one event at a time.
- The ``splitter`` flowlet reads sentences from stream and splits them into words, writes 
  global statistics of the received words such as "total words received" and "total length
  of words received" and emits each word to the ``counter`` flowlet  and each sentence
  (list of words) to the ``associator`` flowlet.
- The ``associator`` flowlet receives the set of words and writes word associations to the 
  ``wordAssocs`` dataset. For example, if we receive a sentence ``"Welcome to CDAP"``, the
  word associations are ``{"Welcome", "to"}`` , ``{"Welcome", "CDAP"}``, and ``{"to",
  "CDAP"}``.
- The ``counter`` flowlet receives a word, increments the count for the word |---| 
  maintained in a key-value table |---| and forwards the word to the ``unique`` flowlet.
- The ``unique`` flowlet receives a word and updates the ``uniqueCount`` table, if it is 
  seeing this word for the first time.

Let's look at some of these components, and then run the application and see the results.

The Word Count Application
--------------------------
As in the other :ref:`examples <examples-index>`, the components
of the application are tied together by the class ``WordCount``:

.. literalinclude:: /../../../cdap-examples/WordCount/src/main/java/co/cask/cdap/examples/wordcount/WordCount.java
   :language: java
   :lines: 32-

Data Storage
------------
The application uses these datasets by default:

- ``wordStatsTable`` stores the global statistics of total count of words and the total length of words received.
- ``wordCountTable`` stores the word and the corresponding count in a key value table.
- ``uniqueCountTable`` is a custom dataset that stores the total count of unique words received so far.
- ``wordAssocTable`` is a custom dataset that stores the count for word associations.

However, the names of these datasets can be configured to be different from their defaults by providing a
configuration at application deployment time. All programs rely on this configuration to instantiate their
datasets at runtime.

.. _word-count-service-requests:

RetrieveCounts Service
----------------------
The service serves read requests for calculated statistics, word counts, and associations.
It exposes these endpoints:

- ``/stats`` returns the total number of words, the number of unique words, and the
  average word length.

- ``/count/{word}?limit={limit}`` returns the word count of a specified word and its word
  associations, up to a specified limit or, if not specified, the default limit of ten.

- ``/counts`` returns the counts for all words in the input, with the request body
  expected to contain a comma-separated list of words.

- ``/multicounts`` returns the counts for all words in the input, with the request body
  expected to contain a comma-separated list of words. It differs from the ``/counts``
  endpoint in that it uses a ``KeyValueTable`` to perform a batched read.

- ``/assoc/{word1}/{word2}`` returns the count of associations for a specific word pair.


.. Building and Starting
.. =====================
.. |example| replace:: WordCount
.. |example-italic| replace:: *WordCount*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <WordCount>`

.. include:: _includes/_building-starting-running.txt


Running the Example
===================

.. Starting the Flow
.. -----------------
.. |example-flow| replace:: WordCounter
.. |example-flow-italic| replace:: *WordCounter*
.. include:: _includes/_starting-flow.txt

.. Starting the Service
.. --------------------
.. |example-service| replace:: RetrieveCounts
.. |example-service-italic| replace:: *RetrieveCounts*
.. include:: _includes/_starting-service.txt

Injecting Sentences
-------------------
In the application's `detail page
<http://localhost:11011/ns/default/apps/WordCount/overview/programs>`__, click on the
*WordCounter* flow. This takes you to the flow details page. 

Now double-click on the *wordStream* stream on the left side of the flow visualization,
which brings up a pop-up window. Enter a sentence such as ``"Hello CDAP"`` (without the
enclosing quotes) and click on the *Inject* button. 

After you close the pop-up window (using the button in the window's upper-right), you will
see that the counter for the stream increases to 1, the counters for the flowlets
*splitter* and *associator* increase to 1 and the counters for the flowlets *counter* and
*unique* increase to 2. 

You can repeat these steps to enter additional sentences. In the dialog box is an *+Upload* button that will
send a file to the stream; you can use that to upload a text file if you wish.

Querying the Results
--------------------
.. highlight:: console

To query the ``RetrieveCounts`` service, either:

- Use the CDAP CLI:

  .. tabbed-parsed-literal::

    $ cdap-cli.sh call service WordCount.RetrieveCounts GET /count/CDAP

- Send a query via an HTTP request using the ``curl`` command:

  .. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "http://localhost:11015/v3/namespaces/default/apps/WordCount/services/RetrieveCounts/methods/count/CDAP"

.. highlight:: json

The word count and top-10 associations words for that word will be displayed in JSON
format (example reformatted to fit; results will depend on what you have submitted)::

  {
    "assocs": {
      "Hello":1,
      "BigData":3,
      "Cask":5,
    },
    "count":6,
    "word":"CDAP"
  }

.. highlight:: console

You can also make requests to the other endpoints available in this service, as 
:ref:`described above <word-count-service-requests>`.


.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-flow-service-removing-application.txt
