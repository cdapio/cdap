.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _users-etl-components:

==========================
ETL Application Components
==========================

Introduction
============

Sources
=======

Batch
-----
 
StreamSource
............
PartitionFileSource
...................

TableSource
...........

DBInputSource (MySQLBatchSource?)
.................................

DatasetSource
.............

KafkaSource  0.7/0.8
....................


Realtime
--------

Kafka 0.7/0.8
.............

Java Message Service (JMS)
..........................

Twitter (Twitter4J library)
...........................

Database (stream new records)
.............................


Sinks
=====
Batch
-----
PartitionFileSink
.................

TableSink
.........

DBSink
......

Realtime
--------
StreamSink
..........

TableSink
.........

KeyValueTableSink (KVTableSink)
...............................

CubeSink
........

Sink as Source for additional Applications/pipelines   
-------------------------------------------------------

Transformations
===============

Filters
-------
- in UI as "IdentityTransform"
- Filter based on a criteria [tbd]

Projection
----------
- in UI as "ProjectionTransform"
- Dropping Columns
- Renaming Columns
- Converting Columns

Custom Transforms
-----------------
- Uses Javascript?
- Link to Dev Manual?
