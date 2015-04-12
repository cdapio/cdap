.. meta::
    :author: Cask Data, Inc.
    :description: Introduction to the Cask Data Application Platform
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-relations: true
:hide-global-toc: true

.. _introduction-to-cdap:

==================================================
Introduction to CDAP
==================================================

Simple Access to Powerful Technology
====================================

The idea of CDAP is captured in our phrase, "Simple Access to Powerful Technology". Our
goal is to provide access to the powerful technology of Apache Hadoop through a unified
big data platform for both the cloud and on-premises.

In this introduction to CDAP, we're going to show how CDAP provides that access,
demonstrated through a comparison between using the current technologies available from
the Hadoop ecosystem and using CDAP |---| a new paradigm.

We'll look at these areas:

- Data Ingestion
- Data Exploration
- Advanced Data Exploration
- Transforming Your Data
- Building Real World Applications


Data Ingestion
==============



- Streams are abstractions over HDFS with an HTTP endpoint
- Data in a Stream are ordered and time-partitioned]
- Support easy exploration and processing in realtime and batch
- Ingest using RESTful, Flume, language-specific APIs, or Tools
- The abstraction of Streams lets you disconnect how you ingest from how you process

.. list-table::
   :widths: 45 45 10
   :header-rows: 1

   * - New Paradigm With CDAP
     - Current Approach
     - Technologies
     
   * - ``$ create stream logEventStream``
     - - Create a Time partitioned file in HDFS
       - Configure Kafka or Flume to write to time partitions
     - - HDFS
       - Kafka
       
   * - ``$ load stream logEventStream data/accesslog.txt``
     - - Write a custom consumer for Kafka that reads from source
       - Write the data to HDFS
       - Create external table in Hive called cdap_stream_logeventstream
     - - HDFS
       - Kafka

Data Exploration
================
- Immediately start with exploration of your ingested data
- Introspect raw data, view data within a time range
- Easily inspect the quality of data by generating data stats
- Easily associate schema once you know your data: "schema on read"
- Support different data formats; extensible to support custom formats
- Supported data formats include AVRO, Text, CSV, TSV, and CLF
- Query using SQL

.. list-table::
   :widths: 45 45 10
   :header-rows: 1

   * - New Paradigm With CDAP
     - Current Approach
     - Technologies
     
   * - ``$ execute 'describe cdap_stream_logEventStream'``
     - - Run Hive command "DESCRIBE cdap_stream_logeventstream" using Hive CLI
     - - HiveServer
       - Beeline
     
   * - ``$ execute 'select * from cdap_stream_logEventStream limit 2'``
     - - Run Hive command "SELECT * FROM cdap_stream_logeventstream LIMIT 2" using Hive CLI
     - - HiveServer
       - Beeline
     

Advanced Data Exploration
=========================



Transforming Your Data
======================



Building Real World Applications
================================



Applications
============

