.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-count-random:

====================
Count Random
====================

A Cask Data Application Platform (CDAP) Example demonstrating the ``@Tick`` feature of Flows.

Overview
====================

This application does not have a Stream, instead it uses a Tick annotation in the ``source`` flowlet to generate data:

- The ``generate`` method of the  ``source`` flowlet has a ``@Tick`` annotation which specifies how frequently the method will be called.
- The ``source`` flowlet generates a random integer in the range {1..10000} and emits it to the next flowlet ``splitter``.
- The ``splitter`` flowlet splits the number into digits, and emits these digits to the next stage.
- The ``counter`` increments the count of the received number in the KeyValueTable.

Let's look at some of these elements, and then run the Application and see the results.

The Count Random Application
------------------------------

.. highlight:: java

As in the other `examples <index.html>`__, the components
of the Application are tied together by the class ``CountRandom``:

.. literalinclude:: /../../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/CountRandom.java
   :language: java
   :lines: 24-

The Flow contains three flowlets:

.. literalinclude:: /../../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/CountRandomFlow.java
   :language: java
   :lines: 25-

The Flowlet (*source*) that generates random numbers every 1 millisecond:

.. literalinclude:: /../../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/RandomSource.java
   :language: java
   :lines: 28-

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

Once you start the flow, the *source* flowlet will continuously generate data. You can see this by observing the counters
for each flowlet in the flow visualization. Even though you are not injecting any data into the flow, the counters increase steadily.

Stopping the Application
-------------------------------
Once done, you can stop the application as described above in `Stopping an Application. <#stopping-an-application>`__

