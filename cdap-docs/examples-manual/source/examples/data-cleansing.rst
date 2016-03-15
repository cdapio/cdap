.. meta::
    :author: Cask Data, Inc.
    :description: Cask Data Application Platform Data Cleansing Application
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _examples-data-cleansing:

==============
Data Cleansing
==============

A Cask Data Application Platform (CDAP) example demonstrating incrementally consuming
partitions of a partitioned file set using MapReduce.


Overview
========
This application has a MapReduce which processes records from one partitioned file set into another partitioned file set,
while filtering records that do not match a particular schema.

- The ``DataCleansingService`` writes to the *rawRecords* partitioned file set.
- The ``DataCleansingMapReduce`` processes the records, while filtering 'unclean' or invalid records.

Let's look at some of these components, and then run the application and see the results.

The Data Cleansing Application
------------------------------
As in the other :ref:`examples <examples-index>`, the components
of the application are tied together by the class ``DataCleansing``:

.. literalinclude:: /../../../cdap-examples/DataCleansing/src/main/java/co/cask/cdap/examples/datacleansing/DataCleansing.java
   :language: java
   :lines: 30-

Data Storage
------------
- *rawRecords* input ``PartitionedFileSet`` of the ``DataCleansingMapReduce``, contains any ingested records.
- *cleanRecords* output ``PartitionedFileSet``, contains only the filtered records.
- *consumingState* stores the state of the ``DataCleansingMapReduce``, such that in each run,
  it processes only new partitions.

DataCleansingService
--------------------
The service allows writing to the *rawRecords* ``PartitionedFileSet``.
It exposes this endpoint:

- POST ``/records/raw`` allows for writing to a partition of the *rawRecords* dataset;

MapReduce over PartitionedFileSet
---------------------------------
``DataCleansingMapReduce`` is a simple MapReduce that reads from the *rawRecords* ``PartitionedFileSet`` and writes to
the *cleanRecords* ``PartitionedFileSet``. The ``beforeSubmit`` method prepares the MapReduce program:

- It uses the ``PartitionBatchInput`` to specify the partitions to process as input, in order to only process new
  partitions since its last run.
- It specifies the output partition that is written to, based upon the supplied runtime arguments.


.. Building and Starting
.. =====================
.. |example| replace:: DataCleansing
.. |example-italic| replace:: *DataCleansing*
.. |application-overview-page| replace:: :cdap-ui-apps-programs:`application overview page, programs tab <DataCleansing>`

.. include:: _includes/_building-starting-running.txt


Running the Example
===================

.. Starting the Service
.. --------------------
.. |example-service| replace:: DataCleansingService
.. |example-service-italic| replace:: *DataCleansingService*

.. include:: _includes/_starting-service.txt

Ingesting Records
-----------------
Begin by uploading a file containing some newline-separated JSON records into the *rawRecords* dataset::

  $ cdap-cli.sh call service DataCleansing.DataCleansingService POST v1/records/raw body:file examples/DataCleansing/resources/person.json

.. |example-mapreduce| replace:: DataCleansingMapReduce
.. |example-mapreduce-italic| replace:: *DataCleansingMapReduce*

Starting the MapReduce
----------------------
The MapReduce must be started with a runtime argument ``output.partition.key`` that
specifies the output partition of the *cleanRecords* dataset to write to. In this
example, we'll simply use ``1`` as the value.

- Using the CDAP-UI, go to the |application-overview|,
  click |example-mapreduce-italic| to get to the MapReduce detail page, set the runtime
  arguments using ``output.partition.key`` as the key and ``1`` as the value, then click
  the *Start* button; or
- From the Standalone CDAP SDK directory, use the Command Line Interface:

  .. container:: highlight

    .. parsed-literal::
      |$| cdap-cli.sh start mapreduce |example|.\ |example-mapreduce| output.partition.key=1
      Successfully started mapreduce '|example-mapreduce|' of application '|example|' 
      with provided runtime arguments 'output.partition.key=1'

Optionally, to specify a custom schema to match records against, the JSON of the schema can be
specified as an additional runtime argument to the MapReduce with the key ``'schema.key'``.
Otherwise, this is the default schema that is matched against the records:

.. literalinclude:: /../../../cdap-examples/DataCleansing/src/main/java/co/cask/cdap/examples/datacleansing/DataCleansingMapReduce.java
    :language: java
    :lines: 127-131
    :dedent: 2

Querying the Results
--------------------
.. highlight:: console

To sample the *cleanRecords* ``PartitionedFileSet``, execute an explore query using the CDAP CLI::

  $ cdap-cli.sh execute \'SELECT record FROM dataset_cleanRecords where TIME = 1 LIMIT 5\'

- Alternatively, go to the *rawRecords*
  :cdap-ui-datasets-explore:`dataset overview page, explore tab <rawRecords>`
  and execute the query from there.

The records that are not filtered out (those that adhere to the given schema) will be displayed::

  +======================================================================+
  | record: STRING                                                       |
  +======================================================================+
  | {"pid":223986723,"name":"bob","dob":"02-12-1983","zip":"84125"}      |
  | {"pid":001058370,"name":"jill","dob":"12-12-1963","zip":"84125"}     |
  | {"pid":000150018,"name":"wendy","dob":"06-19-1987","zip":"84125"}    |
  | {"pid":013587810,"name":"john","dob":"10-10-1991","zip":"84125"}     |
  | {"pid":811638015,"name":"samantha","dob":"04-20-1965","zip":"84125"} |
  +======================================================================+
  Fetched 5 rows

This process |---| ingesting records, running the MapReduce job with a **different** output partition key,
and requesting the filtered data |---| can be repeated. Each time, the MapReduce job will pickup and process
only the newly ingested set of records.


.. Stopping and Removing the Application
.. =====================================
.. include:: _includes/_stopping-service-removing-application.txt
