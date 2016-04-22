.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _examples-count-random:

============
Count Random
============

A Cask Data Application Platform (CDAP) example demonstrating the ``@Tick`` feature of flows.

Overview
========

This application does not have a stream; instead, it uses a tick annotation in the ``source`` flowlet to generate data:

- The ``generate`` method of the  ``source`` flowlet has a ``@Tick`` annotation which specifies how frequently the method will be called.
- The *source* flowlet generates a random integer in the range {1..10000} and emits it to the next flowlet *splitter*.
- The *splitter* flowlet splits the number into digits, and emits these digits to the next stage.
- The *counter* flowlet increments the count of the received number in the ``KeyValueTable``.

Let's look at some of these components, and then run the application and see the results.

The Count Random Application
----------------------------

.. highlight:: java

As in the other `examples <index.html>`__, the components
of the application are tied together by the class ``CountRandom``:

.. literalinclude:: /../../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/CountRandom.java
   :language: java
   :lines: 25-

The flow contains three flowlets:

.. literalinclude:: /../../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/CountRandomFlow.java
   :language: java
   :lines: 25-
   :dedent: 2

The *source* flowlet generates random numbers every 1 millisecond. It can also be configured through runtime
arguments:

- whether to emit events or not; and
- whether to sleep for an additional delay after each event.

.. literalinclude:: /../../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/RandomSource.java
   :language: java
   :lines: 29-

The *splitter* flowlet emits four numbers for every number that it receives.

.. literalinclude:: /../../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/NumberSplitter.java
   :language: java
   :lines: 25-

Note that this flowlet associates a hash value named *n* with every number that it emits. That allows the *counter*
flowlet to use a hash partitioning strategy when consuming the numbers it receives. This ensures that there are no
transaction conflicts if the flowlet is scaled to multiple instances:

.. literalinclude:: /../../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/NumberCounter.java
   :language: java
   :lines: 27-


.. Building and Starting
.. =====================
.. |example| replace:: CountRandom
.. |example-italic| replace:: *CountRandom*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <CountRandom>`

.. include:: _includes/_building-starting-running.txt


Running the Example
===================

.. Starting the Flow

.. |example-flow| replace:: CountRandom
.. |example-flow-italic| replace:: *CountRandom*

.. include:: _includes/_starting-flow.txt

Once you start the flow, the *source* flowlet will continuously generate data. You can see
this by observing the counters for each flowlet in the flow visualization. Even though you
are not injecting any data into the flow, the counters increase steadily.

Querying the Results
--------------------
You can see the results by executing a SQL query using the CDAP UI. Go to the *randomTable* 
:cdap-ui-datasets-explore:`dataset overview page, explore tab <randomTable>` and click the 
*Execute SQL* button. When the query has finished and is hi-lighted in color, you can view
the results by clicking a middle *Action* button in the right-side of the *Results* table.
A pop-up window will show you the different keys and their values.


.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-flow-removing-application.txt
