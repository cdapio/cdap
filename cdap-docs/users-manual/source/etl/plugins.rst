.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _users-etl-components:

==========================
ETL Plugins
==========================

Introduction
============

Shipped with CDAP in the ETL Batch and ETL Realtime, the plugins listed below are 
available for creating ETL Adapters.

Batch
=======

Source
-----

Stream
......

Sets a CDAP Stream as the source of the Batch Adapter. 
Users provide the stream name and the 
duration of Stream data to read. 

Properties expected: 

- ``name``: Name of the Stream
- ``duration``: Size of the time window to read with each run of pipeline

Optional properties:

- ``delay``: Delay for reading stream events
- ``format``: Format of the stream
- ``schema``: Schema for the body of stream events

Table
.....

KVTable
.......

Database
........

BatchReadable
.............

Sink
----

Table
.....

KVTable (Key Value Table)
.........................

Database
........

TimePartitionedFileSetAvro
..........................

BatchWritable
.............


Realtime
========

Source
-----

Kafka 0.7/0.8
.............

Java Message Service (JMS)
..........................

Twitter (Twitter4J library)
...........................

Sink
----

Stream
......

Table
.....

CubeSink
........


Transformations
===============

Filters
-------
- Filter based on a criteria [tbd]
- in CAP UI as "IdentityTransform"

Projection
----------
- Dropping Columns
- Renaming Columns (``rename``)
- Converting Columns
- in CDAP UI as "ProjectionTransform"
