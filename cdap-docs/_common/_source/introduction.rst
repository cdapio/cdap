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
     - Current Approach and Required Technologies
     - 
     
   * - ``$ create stream logEventStream``
     - - Create a Time partitioned file in HDFS
       - Configure Kafka or Flume to write to time partitions
     - - HDFS
       - Kafka
       
   * - ``$ load stream logEventStream data/accesslog.txt``
     - - Write a custom consumer for Kafka that reads from source
       - Write the data to HDFS
       - Create external table in Hive called ``cdap_stream_logeventstream``
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
     - Current Approach and Required Technologies
     - 
     
   * - ``$ execute 'describe cdap_stream_logEventStream'``
     - - Run Hive command using Hive CLI
       - ``DESCRIBE cdap_stream_logeventstream``
     - - HiveServer
       - Beeline
     
   * - ``$ execute 'select * from cdap_stream_logEventStream limit 2'``
     - - Run Hive command using Hive CLI
       - ``SELECT * FROM cdap_stream_logeventstream LIMIT 2``
     - - HiveServer
       - Beeline

Data Exploration: Attaching schema
==================================
- Apply Combined log format schema to data in stream
- Basic Stream stats

.. list-table::
   :widths: 45 45 10
   :header-rows: 1

   * - New Paradigm With CDAP
     - Current Approach and Required Technologies
     - 
     
   * - ``$ set stream format logEventStream clf``
     - Drop the external Hive table
     - - HiveServer
       - Beeline
   
   * - ``$ execute 'describe cdap_stream_logEventStream'``
     - - Run Hive command 
       - ``DESCRIBE cdap_stream_logeventsetream``
     - - Hive CLI
       - Beeline
   
   * - ``$ execute 'select * from cdap_stream_logEventStream limit 2'``
     - - Run Hive command 
       - ``SELECT * FROM cdap_stream_logeventsetream LIMIT 2``
     - - Hive CLI
       - Beeline
   
   * - ``$ get stream-stats logEventStream limit 1000``
     - Write a code to compute the various stats: Unique, Histograms, etc.   
     - - HiveServer
       - Beeline

Advanced Data Exploration
=========================
- Ability to join multiple Streams using SQL
- Data in Stream can be ingested in Realtime or Batch
- Supports joining with other streams using Hive SQL

.. list-table::
   :widths: 45 45 10
   :header-rows: 1

   * - New Paradigm With CDAP
     - Current Approach and Required Technologies
     - 
     
   * - ``$ create stream ip2geo``
     - - Create a Time partitioned file in HDFS
       - Configure Flume or Kafka to write to time partitions
     - - HDFS
       - Kafka
       - Hive
  
   * - ``$ load stream ip2geo data/ip2geo-maps.csv``
     - - Creating a file in Hadoop file system called ip2geo
       - Write a custom consumer that reads from source (Example: Kafka)
       - Write the data to HDFS
       - Create external table in Hive called cdap_stream_ip2geo
     - - HDFS
       - Kafka
       - Hive

   * - ``$ send stream ip2geo '69.181.160.120, Los Angeles, CA'``
     - - Write data to Kafka or append directly to HDFS
     - - HDFS
       - Kafka

   * - ``$ execute 'select * from cdap_stream_ip2geo'``
     - - Run Hive command using Hive CLI
       - ``SELECT * FROM cdap_stream_ip2geo``
     - - Hive CLI
       - Beeline

   * - ``$ set stream format ip2geo csv "ip string, city string, state string"``
     - - Drop the external Hive table
       - Recreate the Hive table with new schema
     - - HDFS
       - Kafka

   * - ``$ execute 'select * from cdap_stream_ip2geo'``
     - - Run Hive command using Hive CLI
       - ``SELECT * FROM cdap_stream_ip2geo``
     - - Hive CLI
       - Beeline

   * - ``$execute 'select remote_host, city, state, request from cdap_stream_logEventStream join cdap_stream_ip2geo on (cdap_stream_logEventStream.remote_host = cdap_stream_ip2geo.ip) limit 10'``
     - - Run Hive command using Hive CLI
       - ``SELECT remote_host, city, state, request from cdap_stream_logEventStream join cdap_stream_ip2geo on (cdap_stream_logEventStream.remote_host = cdap_stream_ip2geo.ip) limit 10``
     - - Hive CLI
       - Beeline


Transforming Your Data
======================
- Adapters are high order compositions of programs that includes MapReduce, Workflow, Services
- Adapters provide pre-defined transformations to be applied on Stream or other datasets
- Adapters are re-usable and extenable
- Easily configure and manage
- Build your own adapters using simple APIs
- In the following example we will apply pre-defined transformation of converting data in streams to writing to TimePartitionedDatasets (in Avro format) that can be queried using Hive or Impala

.. list-table::
   :widths: 45 45 10
   :header-rows: 1

   * - New Paradigm With CDAP
     - Current Approach and Required Technologies
     - 
     
   * - ``$ list adapters``
     - 
     - 

   * - ``$ create stream-conversion adapter logEventStreamConverter on logEventStream frequency 1m format clf schema "remotehost string, remotelogname string, authuser string, date string, request string, status int, contentlength int, referrer string, useragent string"``
     - - Write a custom consumer that reads from source (Example: Kafka)
       - Write the data to HDFS
       - Create external table in Hive called ``cdap_stream_ip2geo``
       - Orchestrate running the job periodically using Oozie
       - Keep track of last processed times
     - - HDFS
       - Kafka
       - Hive
       - Oozie

   * - ``$ load stream logEventStream data/accesslog.txt``
     - - Write a custom consumer that reads from source (Example: Kafka)
       - Write the data to HDFS
       - Create external table in Hive called ``cdap_stream_ip2geo``
     - - HDFS

   * - ``$ list dataset instances``
       - Dataset that is time paritioned
     - - Run this command using hbase shell:
       - ``hbase shell> list``
       - ``hbase shell> hdfs fs -ls /path/to/my/files``
     - - HDFS

   * - ``$ execute 'describe cdap_user_logEventStream_converted'``
     - - Run Hive query using CLI 
       - ``'describe cdap_user_logEventStream_converted'``
     - - Hive CLI
       - Beeline


Building Real World Applications
================================
- Build Data Applications using simple-to-use CDAP APIs
- Compose complex applications consisting of Workflow, MapReduce, Realtime DAGs (Tigon) and Services
- Build using a collection of pre-defined data pattern libraries
- Deploy and Manage complex data applications such as Web Applications
- **Let's see how we would build a real-world application using CDAP:**

  - *Wise App* performs Web Analytics on access logs
  - *WiseFlow*, parses and computes pageview count per IP in realtime
  - MapReduce job that computes bounce counts: percentage of page that goes to the page before exiting
  - Service to expose the data 
  - Unified platform for different processing paradigms

.. list-table::
   :widths: 45 45 10
   :header-rows: 1

   * - New Paradigm With CDAP
     - Current Approach and Required Technologies
     - 
     
   * - ``$ deploy app apps/cdap-wise-0.3.0-SNAPSHOT.jar``
     - - Write and execute MR job
       - Separate environment for processing in real-time setup stack
       - Add ability to periodically copy datasets into SQL using Sqoop
       - Orchestrate the Mapreduce job using Oozie
       - Write an application to serve the data
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - Sqoop

   * - ``$ describe app Wise``
     - - Check Oozie
       - Check YARN Console
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
   
   * - ``$ start flow Wise.WiseFlow``
     - - Set classpath in environment variable 
       - ``CLASSPATH=/my/classpath``
       - Run the command to start the yarn application
       - ``yarn jar /path/to/myprogram.jar``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
   
   * - ``$ get flow status Wise.WiseFlow``
     - - Run the following commands
       - Get the application Id from the command: 
       - ``yarn application -list | grep "Wise.WiseFlow"``
       - Get the status using the command: 
       - ``yarn application -status <APP ID>``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
   
   * - ``$ get flow logs Wise.WiseFlow``
     - - Navigate to the resouce manager UI
       - Find the Wise.WiseFlow on UI
       - Click to the see application logs
       - Find all the node managers for the application containers
       - Navigate to all the containers in separate tabs 
       - Click on container logs
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN

.. rubric:: Program Lifecycle

.. list-table::
   :widths: 45 45 10
   :header-rows: 1

   * - New Paradigm With CDAP
     - Current Approach and Required Technologies
     - 
     
   * - ``$ start workflow Wise.WiseWorkflow``
     - - Start the job using oozie
       - ``oozie job -start <arguments>``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN

   * - ``$ get workflow status Wise.WiseWorkflow``
     - - Get the workflow status from oozie
       - ``oozie job -info <jobid>``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
   
   * - ``$ start service Wise.WiseService``
     - - Set classpath in environment variable 
       - ``CLASSPATH=/my/classpath``
       - Run the command to start the yarn application
       - ``yarn jar /path/to/myprogram.jar``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
   
   * - ``$ get service status Wise.WiseService``
     - - Run these commands
       - Get the application Id from the command: 
       - ``yarn application -list | grep "Wise.WiseService"``
       - Get the status using the following command: 
       - ``yarn application -status <APP ID>``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN

.. rubric:: Serve the processed data in real time

.. list-table::
   :widths: 45 45 10
   :header-rows: 1

   * - New Paradigm With CDAP
     - Current Approach and Required Technologies
     - 
     
   * - ``$ get endpoints service Wise.WiseService``
     - - Navigate to the resouce manager UI
       - Find the Wise.WiseService on UI
       - Click to the see application logs
       - Find all the node managers for the application containers
       - Navigate to all the containers in sepearate tabs 
       - Click on container logs
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
   
   * - ``$ call service Wise.WiseService GET /ip/69.181.160.120/count``
     - - Discover the host and port where the service is running on by looking at the host 
         and port in the YARN logs or by writing a discovery client that is co-ordinated using Zookeeper
       - Run ``curl http://hostname:port/ip/69.181.160.120/count``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
   
   * - ``$ list dataset instances``
         - ``cdap.user.bounceCountStore``
         - ``cdap.user.pageViewStore``
     - - Run the following command in Hbase shell
       - ``hbase shell> list "cdap.user.*"``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
       - HBase

.. rubric:: View bounce count results 

.. list-table::
   :widths: 45 45 10
   :header-rows: 1

   * - New Paradigm With CDAP
     - Current Approach and Required Technologies
     - 
     
   * - ``$ execute 'SELECT * FROM cdap_user_bouncecountstore LIMIT 5'``
     - - Run the folllowing command in Hive CLI
       - ``"SELECT * FROM cdap_user_bouncecountstore LIMIT 5"``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
       - HBase
   
   * - ``$ stop service Wise.WiseService``
     - - Find the yarn application Id from the following command
       - ``yarn application -list | grep "Wise.WiseService"``
       - Stop the application by running the following command
       - ``yarn application -kill <Application ID>``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
       - HBase
   
   * - ``$ stop flow Wise.WiseFlow``
     - - Find the yarn application Id from the following command
       - ``yarn application -list | grep "Wise.WiseFlow"``
       - Stop the application by running the following command
       - ``yarn application -kill <Application ID>``
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
       - HBase
  
   * - ``$ delete app Wise``
     - - Delete the workflow from oozie
       - Remove the service jars and flow jars
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
       - HBase

Summary
=======

.. list-table::
   :widths: 45 45 10
   :header-rows: 1

   * - New Paradigm With CDAP
     - Current Approach and Required Technologies
     - 

   * - CDAP
     - - Bringing in different open source technologies that have different design principles
       - Familiarize and learn how to operationalize the different technologies
       - Design specific architecture to wire in the the various different components
       - Revisit everything when technology changes
     - - HDFS
       - Kafka
       - Hive
       - Oozie
       - YARN
       - HBase


