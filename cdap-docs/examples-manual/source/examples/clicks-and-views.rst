.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform Clicks and Views Application
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _examples-clicks-and-views:

================
Clicks and Views
================

A Cask Data Application Platform (CDAP) example demonstrating a reduce-side join
across two streams using a MapReduce program.


Overview
========
This application has a MapReduce which processes records in two streams and outputs to a PartitionedFileSet.
The ``ClicksAndViewsMapReduce`` processes the records from a ``views`` stream and a ``clicks`` stream,
then joining on the ``viewId`` of the records and writing the joined records to a ``joined`` PartitionedFileSet.

Let's look at some of these components, and then run the application and see the results.

The Clicks and Views Application
--------------------------------
As in the other :ref:`examples <examples-index>`, the components
of the application are tied together by the class ``ClicksAndViews``:

.. literalinclude:: /../../../cdap-examples/ClicksAndViews/src/main/java/co/cask/cdap/examples/clicksandviews/ClicksAndViews.java
   :language: java
   :lines: 29-

Data Storage
------------
- *views* input ``Stream`` contains ad views, representing an advertisement displayed on a viewer's screen.
- *clicks* input ``Stream`` contains ad clicks, representing when a viewer clicks on an advertisement.
- *joined* output ``PartitionedFileSetFileSet`` contains the joined ad views, which additionally contains the
  count of clicks each ad view has. This Dataset is partitioned on the logical start time of the MapReduce.

MapReduce over Multiple Inputs
------------------------------
``ClicksAndViewsMapReduce`` is a MapReduce that reads from the *clicks* and *views* streams and writes to
the *joined* ``PartitionedFileSet``. The ``initialize()`` method prepares the MapReduce program. It sets up the
two streams as input and sets up the ``PartitionedFileSet`` as output, with the appropriate ``PartitionKey``:

.. literalinclude:: /../../../cdap-examples/ClicksAndViews/src/main/java/co/cask/cdap/examples/clicksandviews/ClicksAndViewsMapReduce.java
   :language: java
   :lines: 58-79
   :dedent: 2

The Mapper class then keys each of the records based upon the ``viewId``. That results in all clicks and views
for a particular ``viewId`` going to a single Reducer for joining. In order to do the join properly, the Reducer needs
to know which source each record came from. This is possible by calling the ``getInputName`` method of the
``MapReduceTaskContext`` that is passed to ``initialize``. Note that the Mapper needs to implement ``ProgramLifeCycle``:

.. literalinclude:: /../../../cdap-examples/ClicksAndViews/src/main/java/co/cask/cdap/examples/clicksandviews/ClicksAndViewsMapReduce.java
   :language: java
   :lines: 86-96
   :dedent: 2
   :append: ...


.. Building and Starting
.. =====================
.. |example| replace:: ClicksAndViews
.. |example-italic| replace:: *ClicksAndViews*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <ClicksAndViews>`

.. include:: _includes/_building-running.txt

- Once the application has been deployed, `run the example <#running-the-example>`__.

- When finished, you can `remove the application <#removing-the-application>`__.

Running the Example
===================

Ingesting Records
-----------------
Begin by uploading a file containing newline-separated records representing view events into the *views* stream:

.. tabbed-parsed-literal::

  $ cdap cli load stream views examples/ClicksAndViews/resources/views.txt

  Successfully loaded file to stream 'views'

Then, upload records representing click events into the *clicks* stream:

.. tabbed-parsed-literal::

  $ cdap cli load stream clicks examples/ClicksAndViews/resources/clicks.txt

  Successfully loaded file to stream 'clicks'


.. |example-mapreduce| replace:: ClicksAndViewsMapReduce
.. |example-mapreduce-italic| replace:: *ClicksAndViewsMapReduce*

Starting the MapReduce
----------------------
The MapReduce will write to a partition based upon its logical start time when it is run.

- Using the CDAP UI, go to the |application-overview|,
  click |example-mapreduce-italic| to get to the MapReduce detail page, then click
  the *Start* button; or
- From the CDAP Sandbox home directory, use the Command Line Interface:

  .. tabbed-parsed-literal::

      $ cdap cli start mapreduce |example|.\ |example-mapreduce|

      Successfully started mapreduce '|example-mapreduce|' of application '|example|'
      with stored runtime arguments '{}'

Querying the Results
--------------------
.. highlight:: console

Once the MapReduce job has completed, you can sample the *joined* ``PartitionedFileSet``,
by executing an explore query using the CDAP CLI:

.. tabbed-parsed-literal::

  $ cdap cli execute "\"SELECT * FROM dataset_joined\""

- Alternatively, go to the *rawRecords*
  :cdap-ui-datasets-explore:`dataset overview page, explore tab <rawRecords>`
  and execute the query from there.

The view records along with their click count will be displayed::

  +======================================================================================================================================================================================================================+
  | dataset_joined.viewid:  | dataset_joined.requestb | dataset_joined.adid: BI | dataset_joined.referrer | dataset_joined.usercook | dataset_joined.ip: STRI | dataset_joined.numclick | dataset_joined.runtime: BIGINT |
  | BIGINT                  | egintime: BIGINT        | GINT                    | : STRING                | ie: STRING              | NG                      | s: INT                  |                                |
  +======================================================================================================================================================================================================================+
  | 0                       | 1461219010              | 2157                    | http://www.google.com   | lu=fQ9qHjLjFg3qi3bZiuz  | 62.128.93.36            | 0                       | 1461284201475                  |
  | 1                       | 1461265001              | 2157                    | http://www.google.co.uk | lu=8fsdggknea@ASJHlz    | 21.612.39.63            | 1                       | 1461284201475                  |
  | 2                       | 1461281958              | 2157                    | http://www.yahoo.com    | name=Mike               | 212.193.252.52          | 1                       | 1461284201475                  |
  | 3                       | 1461331879              | 2157                    | http://www.amazon.com   | name=Matt               | 1.116.135.146           | 0                       | 1461284201475                  |
  | 4                       | 1461348738              | 2157                    | http://www.t.co         | name=Nicholas; Httponly | 89.141.94.158           | 0                       | 1461284201475                  |
  | 5                       | 1461349158              | 2157                    | http://www.linkedin.com | lo=Npa0jbIHGloMnx75     | 69.75.87.114            | 1                       | 1461284201475                  |
  +======================================================================================================================================================================================================================+
  Fetched 6 rows


To calculate a click-through rate from this data, you could divide the number of clicks by the number of total views:

.. tabbed-parsed-literal::

  $ cdap cli execute "\"SELECT SUM(numclicks)/COUNT(*) AS CTR FROM dataset_joined\""

With our sample data, the click through rate is ``0.5``::

  +=============+
  | ctr: DOUBLE |
  +=============+
  | 0.5         |
  +=============+
  Fetched 1 rows

Removing the Application
========================
.. include:: _includes/_removing-application.txt
  :start-after: **Removing the Application**
