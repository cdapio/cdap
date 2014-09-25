:orphan:

.. :Author: Cask Data, Inc.
   :Description: Cask Data Application Platform WordCount Application
     :copyright: Copyright © 2014 Cask Data, Inc.

.. _word-count:

Word Count
----------

A Cask Data Application Platform (CDAP) Example demonstrating Flows, Datasets and Procedures.

Overview
........

This application receives words and sentences from a stream and uses flowlets to process them and
store the results and statistics in datasets.

  - The ``wordStream`` receives sentences, one event at a time
  - The ``splitter`` flowlet reads sentences from stream and splits them into words, writes global statistics of the received words like "total words received"
    and "total length of words received" and emits each word to the ``counter`` flowlet  and the sentence (list of words) to the  ``associator`` flowlet
  - The ``associator`` flowlet receives the set of words and writes word association to the dataset.
    For example, If we recevie a sentence "welcome to CDAP", there word associations are
    {"welcome","to"} , {"welcome", "CDAP"}, and {"welcome","to"}.
  - The ``counter`` flowlet receives a word and increments the count for this word, maintained in a Key-Value table and forwards this word to ``unique`` flowlet
  - The ``unique`` flowlet receives a word and updates the uniqueCount table, if it sees this word for the first time

Let's look at some of these elements, and then run the Application and see the results.

The Word Count Application
..........................

As in the other :ref:`examples.<examples>`, the components
of the Application are tied together by the class ``WordCount``::

  public class WordCount extends AbstractApplication {
    @Override
    public void configure() {
      setName("WordCount");
      setDescription("Example Word Count Application");

      // Ingest data into the Application via Streams
      addStream(new Stream("wordStream"));

       // Store processed data in Datasets
      createDataset("wordStats", Table.class);
      createDataset("wordCounts", KeyValueTable.class);
      createDataset("uniqueCount", UniqueCountTable.class);
      createDataset("wordAssocs", AssociationTable.class);

      // Process events in real-time using Flows
      addFlow(new WordCounter());

      // Query the processed data using a Procedure
      addProcedure(new RetrieveCounts());
    }
  }


Data Storage
++++++++++++

- wordStats table stores the global statistics of total count of words and the total length of words received
- wordCounts table stores the word and the corresponding count in a key value table
- uniqueCount is a custom dataset, that stores the total count of unique words received so far
- wordAssocs is a custom dataset, that stores the count for word associations

RetrieveCounts Procedure
++++++++++++++++++++++++

This Procedure has three methods:
- getStats(): Returns global statistics like  "total words received", "total length of words received" and "average length of words"
- getCount(): Given a word, this returns the total count of occurrences and the top-10 associated words for this word
- getAssoc(): Given a pair, "word1" and "word2", this returns the association count for this pair

Deploy and start the application as described in  :ref:`Build, Deploy and start <convention>`

Running the Example
+++++++++++++++++++

Injecting Sentences
###################

In the Application's detail page, under Process, click on WordCounter flow. This takes you to the flow details page.
Now click on the "wordStream" stream on the left side of the flow visualization, which brings up a pop-up window.
Enter a sentence "Hello CDAP" and click on the Inject button. After you close the pop-up window, you will see that the counter
for the stream increases to 1, whereas the counters for the flowlets ``splitter`` and ``associator`` increases to 1 and
the counters for the flowlets ``counter``  and ``unique`` increases to 2.
You can repeat this step to enter more sentences.

Querying the Results
####################

If the Procedure has not already been started, you start it either through the
CDAP Console or via an HTTP request using the ``curl`` command::

	curl -v -X POST 'http://localhost:10000/v2/apps/WordCount/procedures/RetrieveCounts/start'

There are two ways to query the  ``RetrieveCounts`` procedure:

1. Send a query via an HTTP request using the ``curl`` command. For example::

	curl -v -d '{"word": "CDAP"}' \
	  -X POST 'http://localhost:10000/v2/apps/WordCount/procedures/RetrieveCounts/methods/getCount'

  On Windows, a copy of ``curl`` is located in the ``libexec`` directory of the project SDK::

	  libexec\curl...

2. Click on the ``RetrieveCounts`` in the Application page of the Console to get to the
   Procedure dialogue. Type in the method name ``getCount``, and enter a word in the parameters
   field, such as::

	  { "word" : "CDAP" }

Then click the *Execute* button. The word count and top-10 associations words for that word will be displayed in the
Console in JSON format, for example (reformatted to fit)::

  {
    "assocs": {
        "Hello": 1,
        "BigData":3,
        "Cask":5,
    },
    "count": 6,
    "word": "CDAP"
  }

3. You can try executing other methods available in this procedure,
    - getStats
    - getAssoc - For getAssoc you need to provide two words to get their association count, example: {"word1":"Hello", "word2":"CDAP"}

Once done, You can stop the application as described in :ref:`Stop Application <stop-application>`
.. highlight:: java


