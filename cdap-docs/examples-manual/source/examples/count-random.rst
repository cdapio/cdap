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
- The ``source`` flowlet generates a random integer in the range {1..10000} and emits it to the next flowlet ``splitter``.
- The ``splitter`` flowlet splits the number into digits, and emits these digits to the next stage.
- The ``counter`` increments the count of the received number in the KeyValueTable.

Let's look at some of these components, and then run the application and see the results.

The Count Random Application
----------------------------

.. highlight:: java

As in the other `examples <index.html>`__, the components
of the application are tied together by the class ``CountRandom``:

.. literalinclude:: /../../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/CountRandom.java
   :language: java
   :lines: 24-

The flow contains three flowlets:

.. literalinclude:: /../../../cdap-examples/CountRandom/src/main/java/co/cask/cdap/examples/countrandom/CountRandomFlow.java
   :language: java
   :lines: 25-

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


Building and Starting
=====================

.. include:: building-and-starting.txt


Running CDAP Applications
=========================

.. |example| replace:: CountRandom

.. include:: /../../developers-manual/source/getting-started/building-apps.rst
   :start-line: 11


Running the Example
===================

Starting the Flow
-----------------

Once the application is deployed:

- Go to the *CountRandom* `application overview page 
  <http://localhost:9999/ns/default/apps/CountRandom/overview/status>`__,
  click ``CountRandom`` to get to the flow detail page, then click the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh start flow CountRandom.CountRandom 
    Successfully started flow 'CountRandom' of application 'CountRandom' with stored runtime arguments '{}'

Once you start the flow, the *source* flowlet will continuously generate data. You can see
this by observing the counters for each flowlet in the flow visualization. Even though you
are not injecting any data into the flow, the counters increase steadily.


Stopping and Removing the Application
=====================================
Once done, you can stop the application as described above in `Stopping an Application. 
<#stopping-an-application>`__ Here is an example-specific description of the step:

**Stopping the Flow**

- Go to the *CountRandom* `application overview page 
  <http://localhost:9999/ns/default/apps/CountRandom/overview/status>`__,
  click ``CountRandom`` to get to the flow detail page, then click the *Stop* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh stop flow CountRandom.CountRandom
    Successfully stopped flow 'CountRandom' of application 'CountRandom'

**Removing the Application**

You can now remove the application as described above, `Removing an Application <#removing-an-application>`__, or:

- Go to the *CountRandom* `application overview page 
  <http://localhost:9999/ns/default/apps/CountRandom/overview/status>`__,
  click the *Actions* menu on the right side and select *Manage* to go to the Management pane for the application,
  then click the *Actions* menu on the right side and select *Delete* to delete the application; or
- From the Standalone CDAP SDK directory, use the Command Line Interface::

    $ cdap-cli.sh delete app CountRandom
