.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform SpamClassifier Application
    :copyright: Copyright © 2016 Cask Data, Inc.

.. _examples-spam-classifier:

===============
Spam Classifier
===============

A Cask Data Application Platform (CDAP) example demonstrating Spark Streaming.


Overview
========
This example demonstrates a Spark Streaming application that classifies Kafka messages as either "spam" or "ham" (not spam)
based on a trained Spark MLlib NaiveBayes model.

Training data from a sample file is sent to CDAP by a CDAP CLI command to the *trainingDataStream*. This training data
is from the `SMS Spam Collection Dataset <https://archive.ics.uci.edu/ml/datasets/SMS+Spam+Collection>`__, 
which consists of a label (``spam``, ``ham``) followed by the message.

This training data is used by the *SpamClassifierProgram* to train a Spark MLlib NaiveBayes model, which is then used
to classify realtime messages coming through Kafka.

Once the application completes, you can query the *messageClassificationStore* dataset by using the
``classification/<message-id>`` endpoint of the *MessageClassification*. It will respond with ``Spam`` or ``Ham``
depending upon on the classification of the ``message``.

Let's look at some of these components, and then run the application and see the results.

The *SpamClassifier* Application
--------------------------------
As in the other `examples <index.html>`__, the components
of the application are tied together by the class ``SpamClassifier``:

.. literalinclude:: /../../../cdap-examples/SpamClassifier/src/main/java/co/cask/cdap/examples/sparkstreaming/SpamClassifier.java
   :language: java
   :lines: 40-66
   :append: . . .

The *messageClassificationStore* ObjectStore Data Storage
---------------------------------------------------------
The classified messages are stored in an ObjectStore dataset, *messageClassificationStore*, keyed by the ``message-id``.

The *MessageClassification* Service
-----------------------------------
This service has a ``classification/<message-id>`` endpoint to obtain the classification of a given message through its
``message-id``.


.. Building and Starting
.. =====================
.. |example| replace:: SpamClassifier
.. |example-italic| replace:: *SpamClassifier*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <SpamClassifier>`

.. |example-spark| replace:: SpamClassifierProgram
.. |example-spark-italic| replace:: *SpamClassifierProgram*
.. |example-spark-literal| replace:: ``SpamClassifierProgram``

.. include:: _includes/_building-starting-running.txt


Running the Example
===================

.. Starting the Service
.. --------------------
.. |example-service| replace:: MessageClassification
.. |example-service-italic| replace:: *MessageClassification*

.. include:: _includes/_starting-service.txt

Injecting Training Data
-----------------------
Inject a file of training data to the stream *trainingDataStream* by running this command from the
Standalone CDAP SDK directory, using the Command Line Interface:
  
.. tabbed-parsed-literal::
  
  $ cdap cli load stream trainingDataStream examples/SpamClassifier/src/test/resources/trainingData.txt
  
  Successfully loaded file to stream 'trainingDataStream'

Running the Spark Program
-------------------------
There are three ways to start the Spark program:

1. Go to the |example-italic| :cdap-ui-apps-programs:`application overview page, programs
   tab <SpamClassifier>`, click |example-spark-literal| to get to the Spark program detail
   page, and add these runtime arguments/preferences::

     kafka.brokers:broker1-host:port
     kafka.topics:topic1,topic2

   then click the *Start* button;
   
#. Use the Command Line Interface:

   .. tabbed-parsed-literal::

     $ cdap cli start spark |example|.\ |example-spark| "kafka.brokers=broker1-host:port kafka.topics=topic1,topic2"

#. Send a query via an HTTP request using a ``curl`` command:

   .. tabbed-parsed-literal::

     $ curl -w"\n" -X POST -d '{"kafka.brokers":"broker1-host:port", "kafka.topics":"topic1,topic2"}' \
     "http://localhost:11015/v3/namespaces/default/apps/SpamClassifier/spark/SpamClassifierProgram/start"


Injecting Prediction Data
-------------------------
You can publish the messages which you want to be classified as "spam/ham" to the Kafka topic configured in the
above Spark program. Kafka command line tools can be used to create topic and send  messages. The message must be in
the format::

    message-id:message

For example::

    2:I will call you later
    

Querying the Results
--------------------
To query the *messageClassificationStore* ObjectStore using the ``MessageClassification``, you can either:

- Use the Command Line Interface:

  .. tabbed-parsed-literal::

    $ cdap cli call service SpamClassifier.MessageClassification GET status/1
    
    Ham

- Send a query via an HTTP request using a ``curl`` command. For example:

  .. tabbed-parsed-literal::

    $ curl -w"\n" -X GET "http://localhost:11015/v3/namespaces/default/apps/SpamClassifier/services/MessageClassification/methods/status/1"
    
    Ham

.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-spark-service-removing-application.txt
