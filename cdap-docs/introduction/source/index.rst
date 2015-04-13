.. meta::
    :author: Cask Data, Inc.
    :description: Introduction to the Cask Data Application Platform
    :copyright: Copyright © 2015 Cask Data, Inc.


.. _introduction-to-cdap:

==================================================
Introduction to CDAP
==================================================

Simple Access to Powerful Technology
====================================

The idea of CDAP is captured in our phrase, *Simple Access to Powerful Technology*. Our
goal is to provide access to the powerful technology of Apache Hadoop |(R)| through a
unified big data platform for both the cloud and on-premises.

In this introduction to CDAP, we're going to show how CDAP provides that access,
demonstrated through a comparison between using the current technologies available from
the Hadoop ecosystem and using CDAP |---| a new paradigm.

For each example action, we describe a current approach, the steps involved and the
technologies required, and then show an equivalent CDAP command with the resulting output
from the CDAP Command Line Interface.

To try this yourself, :ref:`download a copy of CDAP SDK <standalone-index>`, install it
and then use the resources in its ``examples`` directory as you follow along.

We'll look at these areas:
  - `Data Ingestion`_
  - `Data Exploration`_
  - `Advanced Data Exploration`_
  - `Transforming Your Data`_
  - `Building Real World Applications`_

.. highlight:: console

Data Ingestion
==============
- Streams are abstractions over HDFS with an HTTP endpoint
- Data in a Stream are ordered and time-partitioned
- CDAP supports easy exploration and processing in both realtime and batch
- Ingest using RESTful, Flume, language-specific APIs, or Tools
- The abstraction of Streams lets you disconnect how you ingest from how you process

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - - Create a Time partitioned file in HDFS
       - Configure Kafka or Flume to write to time partitions
     - HDFS, Kafka
       
.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ create stream logEventStream``
       ::
       
        Successfully created stream with ID 'logEventStream'

|non-breaking-space|

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - - Write a custom consumer for Kafka that reads from source
       - Write the data to HDFS
       - Create external table in Hive called ``cdap_stream_logeventstream``
     - HDFS, Kafka

.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ load stream logEventStream examples/resources/accesslog.txt``
       ::
       
        Successfully send stream event to stream 'logEventStream'

Data Exploration
================
- Immediately start with the exploration of your ingested data
- Introspect raw data or view data within a time range
- Easily inspect the quality of data by generating data stats
- Easily associate a schema once you know your data: "schema on read"
- Support different data formats; extensible to support custom formats
- Supported data formats include AVRO, Text, CSV, TSV, and CLF
- Query using SQL

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - Run Hive command using Hive CLI: ``DESCRIBE stream_logeventstream``
     - HiveServer, Beeline
       
.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ execute 'describe stream_logEventStream'``
       ::
    
        +=========================================================================================================+
        | col_name: STRING                 | data_type: STRING                | comment: STRING                   |
        +=========================================================================================================+
        | ts                               | bigint                           | from deserializer                 |
        | headers                          | map<string,string>               | from deserializer                 |
        | body                             | string                           | from deserializer                 |
        +=========================================================================================================+

|non-breaking-space|

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - Run Hive command using Hive CLI: ``SELECT * FROM stream_logeventstream LIMIT 2``
     - HiveServer, Beeline

.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ execute 'select * from stream_logEventStream limit 2'``
       ::

        +=========================================================================================================+
        | stream_logeventstream.ts: BIGINT  | stream_logeventstream.headers: | stream_logeventstream.body: STRING |
        |                                   | map<string,string>             |                                    |
        +=========================================================================================================+
        | 1428100343436                     | {}                             | 255.255.255.185 - - [23/Sep/2014:1 |
        |                                   |                                | 1:45:38 -0400]  "GET /cdap.html HT |
        |                                   |                                | TP/1.0" 401 2969 " " "Mozilla/4.0  |
        |                                   |                                | (compatible; MSIE 7.0; Windows NT  |
        |                                   |                                | 5.1)"                              |
        |---------------------------------------------------------------------------------------------------------|
        | 1428100483106                     | {}                             | 255.255.255.185 - - [23/Sep/2014:1 |
        |                                   |                                | 1:45:38 -0400] "GET /cdap.html HTT |
        |                                   |                                | P/1.0" 401 2969 " " "Mozilla/4.0 ( |
        |                                   |                                | compatible; MSIE 7.0; Windows NT 5 |
        |                                   |                                | .1)"                               |
        +=========================================================================================================+


Data Exploration: Attaching schema
==================================
- Apply an *Combined log format* schema to data in the Stream
- Retrieve basic Stream stats

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - Drop the external Hive table
     - HiveServer, Beeline

.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ set stream format logEventStream clf``
       ::

        Successfully set format of stream 'logEventStream'

|non-breaking-space|

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - Run Hive command using Hive CLI: `DESCRIBE cdap_stream_logeventsetream``
     - HiveServer, Beeline

.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ execute 'describe stream_logEventStream'``
       ::

        +=============================================================================+
        | col_name: STRING          | data_type: STRING       | comment: STRING       |
        +=============================================================================+
        | ts                        | bigint                  | from deserializer     |
        | headers                   | map<string,string>      | from deserializer     |
        | remote_host               | string                  | from deserializer     |
        | remote_login              | string                  | from deserializer     |
        | auth_user                 | string                  | from deserializer     |
        | date                      | string                  | from deserializer     |
        | request                   | string                  | from deserializer     |
        | status                    | int                     | from deserializer     |
        | content_length            | int                     | from deserializer     |
        | referrer                  | string                  | from deserializer     |
        | user_agent                | string                  | from deserializer     |
        +=============================================================================+

|non-breaking-space|

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - Run Hive command using Hive CLI: ``SELECT * FROM cdap_stream_logeventsetream LIMIT 2``
     - HiveCLI, Beeline

.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ execute 'select * from stream_logEventStream limit 2'``
       ::

        +===================================================================================================================+
        | stream_ | stream_ | stream_ | stream_ | stream_ | stream_ | stream_ | stream_ | stream_ | stream_ | stream_logeve |
        | logeven | logeven | logeven | logeven | logeven | logeven | logeven | logeven | logeven | logeven | ntstream.user |
        | tstream | tstream | tstream | tstream | tstream | tstream | tstream | tstream | tstream | tstream | _agent: STRIN |
        | .ts: BI | .header | .remote | .remote | .auth_u | .date:  | .reques | .status | .conten | .referr | G             |
        | GINT    | s: map< | _host:  | _login: | ser: ST | STRING  | t: STRI | : INT   | t_lengt | er: STR |               |
        |         | string, | STRING  |  STRING | RING    |         | NG      |         | h: INT  | ING     |               |
        |         | string> |         |         |         |         |         |         |         |         |               |
        +===================================================================================================================+
        | 1428100 | {}      | 255.255 |         |         | 23/Sep/ | GET /cd | 401     | 2969    |         | Mozilla/4.0 ( |
        | 343436  |         | .255.18 |         |         | 2014:11 | ap.html |         |         |         | compatible; M |
        |         |         | 5       |         |         | :45:38  |  HTTP/1 |         |         |         | SIE 7.0; Wind |
        |         |         |         |         |         | -0400   | .0      |         |         |         | ows NT 5.1)   |
        |-------------------------------------------------------------------------------------------------------------------|
        | 1428100 | {}      | 255.255 |         |         | 23/Sep/ | GET /cd | 401     | 2969    |         | Mozilla/4.0 ( |
        | 483106  |         | .255.18 |         |         | 2014:11 | ap.html |         |         |         | compatible; M |
        |         |         | 5       |         |         | :45:38  |  HTTP/1 |         |         |         | SIE 7.0; Wind |
        |         |         |         |         |         | -0400   | .0      |         |         |         | ows NT 5.1)   |
        +===================================================================================================================+

|non-breaking-space|

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - Write a code to compute the various stats: Unique, Histograms, etc.
     - HiveServer, Beeline

.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ get stream-stats logEventStream limit 1000``
       ::

        Analyzing 1000 Stream events in the time range [0, 9223372036854775807]...

        column: stream_logeventstream.remote_host, type: STRING
        Unique elements: 6

        column: stream_logeventstream.remote_login, type: STRING
        Unique elements: 0

        column: stream_logeventstream.auth_user, type: STRING
        Unique elements: 0

        column: stream_logeventstream.date, type: STRING
        Unique elements: 750

        column: stream_logeventstream.request, type: STRING
        Unique elements: 972

        column: stream_logeventstream.status, type: INT
        Unique elements: 4
        Histogram:
          [200, 299]: 977  |+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
          [300, 399]: 17   |
          [400, 499]: 6    |

        column: stream_logeventstream.content_length, type: INT
        Unique elements: 142
        Histogram:
          [0, 99]: 205           |+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
          [100, 199]: 1          |
          [200, 299]: 9          |+
          [300, 399]: 9          |+
          [400, 499]: 3          |
          [500, 599]: 300        |+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
          [600, 699]: 4          |
          [800, 899]: 2          |
          [900, 999]: 1          |
          [1300, 1399]: 10       |++
          [1400, 1499]: 206      |++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
          [1500, 1599]: 2        |
          [1600, 1699]: 2        |
          [2500, 2599]: 1        |
          [2700, 2799]: 1        |
          [2800, 2899]: 1        |
          [4200, 4299]: 1        |
          [5700, 5799]: 5        |
          [7100, 7199]: 1        |
          [7300, 7399]: 4        |
          [7800, 7899]: 1        |
          [8200, 8299]: 5        |
          [8700, 8799]: 3        |
          [8800, 8899]: 12       |++
          [8900, 8999]: 22       |+++++
          [9000, 9099]: 16       |+++
          [9100, 9199]: 9        |+
          [9200, 9299]: 4        |
          [9300, 9399]: 3        |
          [9400, 9499]: 5        |
          [9600, 9699]: 1        |
          [9700, 9799]: 2        |
          [9800, 9899]: 39       |++++++++++
          [9900, 9999]: 4        |
          [10000, 10099]: 1      |
          [10100, 10199]: 8      |+
          [10200, 10299]: 1      |
          [10300, 10399]: 3      |
          [10400, 10499]: 1      |
          [10500, 10599]: 1      |
          [10600, 10699]: 9      |+
          [10700, 10799]: 32     |++++++++
          [10800, 10899]: 5      |
          [10900, 10999]: 3      |
          [11000, 11099]: 4      |
          [11100, 11199]: 1      |
          [11200, 11299]: 4      |
          [11300, 11399]: 2      |
          [11500, 11599]: 1      |
          [11800, 11899]: 3      |
          [17900, 17999]: 2      |
          [36500, 36599]: 1      |
          [105800, 105899]: 1    |
          [397900, 397999]: 2    |
          [1343400, 1343499]: 1  |
          [1351600, 1351699]: 1  |

        column: stream_logeventstream.referrer, type: STRING
        Unique elements: 8

        column: stream_logeventstream.user_agent, type: STRING
        Unique elements: 4


Advanced Data Exploration
=========================
- CDAP has the ability to join multiple Streams using SQL
- Data in a Stream can be ingested in Realtime or Batch
- CDAP supports joining with other Streams using Hive SQL


.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - - Create a Time partitioned file in HDFS
       - Configure Flume or Kafka to write to time partitions
     - HDFS, Kafka, Hive

.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ create stream ip2geo``
       ::

        Successfully created stream with ID 'ip2geo'

|non-breaking-space|

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - - Creating a file in Hadoop file system called ip2geo
       - Write a custom consumer that reads from source (Example: Kafka)
       - Write the data to HDFS
       - Create external table in Hive called ``cdap_stream_ip2geo``
     - HDFS, Kafka, Hive

.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ load stream ip2geo examples/resources/ip2geo-maps.csv``
       ::

        Successfully send stream event to stream 'ip2geo'
        
|non-breaking-space|

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - Write data to Kafka or append directly to HDFS
     - HDFS, Kafka

.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ Successfully send stream event to stream 'ip2geo'``
       ::

        Successfully send stream event to stream 'ip2geo'
        

|non-breaking-space|

.. list-table::
   :widths: 15 65 20

   * - 
     - *Action / CDAP Command and Output*
     - *Required Technologies*
   * - **Current Approach**
     - Write data to Kafka or append directly to HDFS
     - HDFS, Kafka

.. list-table::
   :widths: 15 85

   * - **CDAP**
     - ``$ Successfully send stream event to stream 'ip2geo'``
       ::

        Successfully send stream event to stream 'ip2geo'


OLD

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
  
   * - ``$ load stream ip2geo examples/resources/ip2geo-maps.csv``
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

   * - ``$ execute 'select remote_host, city, state, request from cdap_stream_logEventStream join cdap_stream_ip2geo on (cdap_stream_logEventStream.remote_host = cdap_stream_ip2geo.ip) limit 10'``
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

   * - ``$ load stream logEventStream examples/resources/accesslog.txt``
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


