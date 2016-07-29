.. meta::
    :author: Cask Data, Inc.
    :description: Introduction to the Cask Data Application Platform
    :copyright: Copyright © 2015-2016 Cask Data, Inc.


:hide-relations: true

.. _introduction-to-cdap:

====================
Introduction to CDAP
====================


Simple Access to Powerful Technology
====================================

The idea of CDAP is captured in the phrase, *Simple Access to Powerful Technology*. Our
goal is to provide access to the powerful technology of Apache Hadoop |(R)| through a
unified big data platform for both the cloud and on-premises.

In this introduction to CDAP, we're going to show how CDAP provides that access,
demonstrated through a comparison between using the current technologies available from
the Hadoop ecosystem and using CDAP |---| a new paradigm.

For each example action, we describe a current approach, the steps involved and the
technologies required, and then show an equivalent CDAP command with the resulting output
from the CDAP Command Line Interface. (Output has been reformatted to fit the webpage
as required.)

To try this yourself, :ref:`download a copy of the CDAP SDK <standalone-index>`, 
:ref:`install it <standalone-index>`,
and then use the resources in its ``examples`` directory as you follow along.

We'll look at these areas:
  - `Installation`_
  - `Data Ingestion`_
  - `Data Exploration`_
  - `Data Exploration: Attaching A Schema`_
  - `Advanced Data Exploration`_
  - `Transforming Your Data`_
  - `Building Real World Applications`_


.. highlight:: console

Installation
============
- Download and install CDAP to run in standalone mode on a laptop
- Startup CDAP
- Startup CDAP Command Line Interface

.. container:: table-block

  .. list-table::
     :widths: 99 1
     :stub-columns: 1

     * - Install and start Servers
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Install and startup **Hadoop** and related technologies, as required
         
     * - Using CDAP
       - - Install **CDAP** by `downloading <http://cask.co/downloads/>`_ a zipfile, unzipping, and starting the **CDAP Server**
      
     * -  
       - .. tabbed-parsed-literal::
       
            .. Linux
      
            $ unzip cdap-sdk-|release|.zip
            $ cd cdap-sdk-|release|
            $ ./bin/cdap.sh start
          
            Starting Standalone CDAP ................
            Standalone CDAP started successfully.
            Connect to the CDAP UI at http://localhost:9999
            
            .. Windows
            
            > jar xf cdap-sdk-|release|.zip
            > cd cdap-sdk-|release|
            > bin\cdap.bat start
          
            Starting Standalone CDAP ................
            Standalone CDAP started successfully.
            Connect to the CDAP UI at http://localhost:9999
            

.. container:: table-block

  .. list-table::
     :widths: 99 1
     :stub-columns: 1

     * - Start command line interface
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1
     
     * - Without CDAP
       - - Run Hive commands using **Hive CLI** (HiveServer and Beeline)
       
     * - Using CDAP
       - - Start **CDAP CLI** (Command Line Interface)

     * -  
       - .. tabbed-parsed-literal::

            $ ./bin/cdap-cli.sh
            
            Successfully connected to CDAP instance at \http://localhost:10000/default
            |cdap >| 


Data Ingestion
==============
- Data is ingested into CDAP using :ref:`streams <streams>`
- Streams are abstractions over HDFS with an HTTP endpoint
- Data in a stream are ordered and time-partitioned
- CDAP supports easy exploration and processing in both real time and batch
- Ingest using RESTful, Flume, language-specific APIs, or Tools
- The abstraction of streams lets you disconnect how you ingest from how you process

.. container:: table-block

  .. list-table::
     :widths: 99 1
     :stub-columns: 1

     * - Create a stream
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Create a Time partitioned file in **HDFS**
         - Configure **Kafka** or **Flume** to write to time partitions
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
       
            |cdap >| create stream logEventStream

            Successfully created stream with ID 'logEventStream'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1

     * - Send data to the stream
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Write a custom consumer for **Kafka** that reads from source
         - Write the data to **HDFS**
         - Create external table in **Hive** called ``stream_logeventstream``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
        
            |cdap >| load stream logEventStream examples/resources/accesslog.txt
 
            Successfully loaded file to stream 'logEventStream'


Data Exploration
================
- Immediately start with the exploration of your ingested data
- Introspect raw data or view data within a time range
- Easily inspect the quality of data by generating data stats
- Easily associate a schema once you know your data: "schema on read"
- Support different data formats; extensible to support custom formats
- Supported data formats include Avro, Text, CSV, TSV, CLF, and Custom
- Query using SQL

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Describe ingested Data
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run Hive command using **Hive CLI**
         - ``DESCRIBE stream_logeventstream``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
        
            |cdap >| execute 'describe stream_logEventStream'
 
            +===========================================================+
            | col_name: STRING | data_type: STRING  | comment: STRING   |
            +===========================================================+
            | ts               | bigint             | from deserializer |
            | headers          | map<string,string> | from deserializer |
            | body             | string             | from deserializer |
            +===========================================================+
            Fetched 3 rows

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Retrieve first two events from the stream
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run Hive command using **Hive CLI**
         - ``SELECT * FROM stream_logeventstream LIMIT 2``

     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| execute 'select * from stream_logEventStream limit 2'
           
            +==============================================================================================================+
            | stream_logeventstream.ts: | stream_logeventstream.hea | stream_logeventstream.body: STRING                   |
            | BIGINT                    | ders: map<string,string>  |                                                      |
            +==============================================================================================================+
            | 1428969220987             | {"content.type":"text/pla | 69.181.160.120 - - [08/Feb/2015:04:36:40 +0000] "GET |
            |                           | in"}                      |  /ajax/planStatusHistoryNeighbouringSummaries.action |
            |                           |                           | ?planKey=COOP-DBT&buildNumber=284&_=1423341312519 HT |
            |                           |                           | TP/1.1" 200 508 "http://builds.cask.co/browse/COOP-D |
            |                           |                           | BT-284/log" "Mozilla/5.0 (Macintosh; Intel Mac OS X  |
            |                           |                           | 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chro |
            |                           |                           | me/38.0.2125.122 Safari/537.36"                      |
            |--------------------------------------------------------------------------------------------------------------|
            | 1428969220987             | {"content.type":"text/pla | 69.181.160.120 - - [08/Feb/2015:04:36:47 +0000] "GET |
            |                           | in"}                      |  /rest/api/latest/server?_=1423341312520 HTTP/1.1" 2 |
            |                           |                           | 00 45 "http://builds.cask.co/browse/COOP-DBT-284/log |
            |                           |                           | " "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) A |
            |                           |                           | ppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.21 |
            |                           |                           | 25.122 Safari/537.36"                                |
            +==============================================================================================================+
            Fetched 2 rows


Data Exploration: Attaching a Schema
====================================

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Apply a *Combined log format* schema to data in the stream
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Drop the external **Hive** table
         - Recreate the **Hive** table with new schema
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| set stream format logEventStream clf
  
            Successfully set format of stream 'logEventStream'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Describe new format of the ingested data
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run Hive command using **Hive CLI**
         - ``DESCRIBE stream_logeventsetream``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| execute 'describe stream_logEventStream'
 
            +=============================================================================+
            | col_name: STRING          | data_type: STRING       | comment: STRING       |
            +=============================================================================+
            | ts                        | bigint                  | from deserializer     |
            | headers                   | map<string,string>      | from deserializer     |
            | remote_host               | string                  | from deserializer     |
            | remote_login              | string                  | from deserializer     |
            | auth_user                 | string                  | from deserializer     |
            | request_time              | string                  | from deserializer     |
            | request                   | string                  | from deserializer     |
            | status                    | int                     | from deserializer     |
            | content_length            | int                     | from deserializer     |
            | referrer                  | string                  | from deserializer     |
            | user_agent                | string                  | from deserializer     |
            +=============================================================================+
            Fetched 11 rows

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Retrieve first two events from the stream, in new format
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run Hive command using **Hive CLI**
         - ``SELECT * FROM stream_logeventsetream LIMIT 2``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| execute 'select * from stream_logEventStream limit 2'

            +========================================================================================================================+
            | stream_l | stream_l | stream_l | stream_l | stream_l | stream_l | stream_l | stream_l | stream_l | stream_l | stream_l |
            | ogevents | ogevents | ogevents | ogevents | ogevents | ogevents | ogevents | ogevents | ogevents | ogevents | ogevents |
            | tream.ts | tream.he | tream.re | tream.re | tream.au | tream.re | tream.re | tream.st | tream.co | tream.re | tream.us |
            | : BIGINT | aders: m | mote_hos | mote_log | th_user: | quest_ti | quest: S | atus: IN | ntent_le | ferrer:  | er_agent |
            |          | ap<strin | t: STRIN | in: STRI |  STRING  | me: STRI | TRING    | T        | ngth: IN | STRING   | : STRING |
            |          | g,string | G        | NG       |          | NG       |          |          | T        |          |          |
            |          | >        |          |          |          |          |          |          |          |          |          |
            +========================================================================================================================+
            | 14437238 | {"conten | 69.181.1 |          |          | 08/Feb/2 | GET /aja | 200      | 508      | http://b | Mozilla/ |
            | 45737    | t.type": | 60.120   |          |          | 015:04:3 | x/planSt |          |          | uilds.ca | 5.0 (Mac |
            |          | "text/pl |          |          |          | 6:40 +00 | atusHist |          |          | sk.co/br | intosh;  |
            |          | ain"}    |          |          |          | 00       | oryNeigh |          |          | owse/COO | Intel Ma |
            |          |          |          |          |          |          | bouringS |          |          | P-DBT-28 | c OS X 1 |
            |          |          |          |          |          |          | ummaries |          |          | 4/log    | 0_10_1)  |
            |          |          |          |          |          |          | .action? |          |          |          | AppleWeb |
            |          |          |          |          |          |          | planKey= |          |          |          | Kit/537. |
            |          |          |          |          |          |          | COOP-DBT |          |          |          | 36 (KHTM |
            |          |          |          |          |          |          | &buildNu |          |          |          | L, like  |
            |          |          |          |          |          |          | mber=284 |          |          |          | Gecko) C |
            |          |          |          |          |          |          | &_=14233 |          |          |          | hrome/38 |
            |          |          |          |          |          |          | 41312519 |          |          |          | .0.2125. |
            |          |          |          |          |          |          |  HTTP/1. |          |          |          | 122 Safa |
            |          |          |          |          |          |          | 1        |          |          |          | ri/537.3 |
            |          |          |          |          |          |          |          |          |          |          | 6        |
            |------------------------------------------------------------------------------------------------------------------------|
            | 14437238 | {"conten | 69.181.1 |          |          | 08/Feb/2 | GET /res | 200      | 45       | http://b | Mozilla/ |
            | 45737    | t.type": | 60.120   |          |          | 015:04:3 | t/api/la |          |          | uilds.ca | 5.0 (Mac |
            |          | "text/pl |          |          |          | 6:47 +00 | test/ser |          |          | sk.co/br | intosh;  |
            |          | ain"}    |          |          |          | 00       | ver?_=14 |          |          | owse/COO | Intel Ma |
            |          |          |          |          |          |          | 23341312 |          |          | P-DBT-28 | c OS X 1 |
            |          |          |          |          |          |          | 520 HTTP |          |          | 4/log    | 0_10_1)  |
            |          |          |          |          |          |          | /1.1     |          |          |          | AppleWeb |
            |          |          |          |          |          |          |          |          |          |          | Kit/537. |
            |          |          |          |          |          |          |          |          |          |          | 36 (KHTM |
            |          |          |          |          |          |          |          |          |          |          | L, like  |
            |          |          |          |          |          |          |          |          |          |          | Gecko) C |
            |          |          |          |          |          |          |          |          |          |          | hrome/38 |
            |          |          |          |          |          |          |          |          |          |          | .0.2125. |
            |          |          |          |          |          |          |          |          |          |          | 122 Safa |
            |          |          |          |          |          |          |          |          |          |          | ri/537.3 |
            |          |          |          |          |          |          |          |          |          |          | 6        |
            +========================================================================================================================+
            Fetched 2 rows
          
.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Retrieve basic stream statistics
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - Write code to compute the various stats: number of unique elements, histograms, etc.
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| get stream-stats logEventStream limit 1000

            column: stream_logeventstream.remote_host, type: STRING
            Unique elements: 6
 
            column: stream_logeventstream.remote_login, type: STRING
            Unique elements: 0
 
            column: stream_logeventstream.auth_user, type: STRING
            Unique elements: 0
 
            column: stream_logeventstream.request_time, type: STRING
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
 
            Analyzing 1000 stream events in the time range [0, 9223372036854775807]...


Advanced Data Exploration
=========================
- CDAP has the ability to join multiple streams using SQL
- Data in a stream can be ingested in real time or batch
- CDAP supports joining with other streams using Hive SQL

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Create an additional stream
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Create a file in **Hadoop** file system called ``ip2geo``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| create stream ip2geo
 
            Successfully created stream with ID 'ip2geo'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Ingest CSV-formatted "IP-to-geo location" data into stream
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Write a custom consumer that reads from source (example: **Kafka**)
         - Write the data to **HDFS**
         - Create external table in **Hive** called ``stream_ip2geo``

     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| load stream ip2geo examples/resources/ip2geo-maps.csv
 
            Successfully loaded file to stream 'ip2geo'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Send individual event to stream
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - Write data to **Kafka** or append directly to **HDFS**
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| send stream ip2geo '69.181.160.120, Los Angeles, CA'
          
            Successfully sent stream event to stream 'ip2geo'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Retrieve events from the stream
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run **Hive** command using **Hive CLI**
         - ``SELECT * FROM stream_ip2geo``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| execute 'select * from stream_ip2geo'
 
            +===========================================================================================================+
            | stream_ip2geo.ts: BIGINT | stream_ip2geo.headers: map<string,string> | stream_ip2geo.body: STRING         |
            +===========================================================================================================+
            | 1428892912060            | {"content.type":"text/csv"}               | 108.206.32.124, Santa Clara, CA    |
            | 1428892912060            | {"content.type":"text/csv"}               | 109.63.206.34, San Jose, CA        |
            | 1428892912060            | {"content.type":"text/csv"}               | 113.72.144.115, New York, New York |
            | 1428892912060            | {"content.type":"text/csv"}               | 123.125.71.19, Palo Alto, CA       |
            | 1428892912060            | {"content.type":"text/csv"}               | 123.125.71.27, Redwood, CA         |
            | 1428892912060            | {"content.type":"text/csv"}               | 123.125.71.28, Los Altos, CA       |
            | 1428892912060            | {"content.type":"text/csv"}               | 123.125.71.58, Mountain View, CA   |
            | 1428892912060            | {"content.type":"text/csv"}               | 142.54.173.19, Houston, TX         |
            | 1428892912060            | {"content.type":"text/csv"}               | 144.76.137.226, Dallas, TX         |
            | 1428892912060            | {"content.type":"text/csv"}               | 144.76.201.175, Bedminister, NJ    |
            | 1428892912060            | {"content.type":"text/csv"}               | 162.210.196.97, Milipitas, CA      |
            | 1428892912060            | {"content.type":"text/csv"}               | 188.138.17.205, Santa Barbara, CA  |
            | 1428892912060            | {"content.type":"text/csv"}               | 195.110.40.7, Orlando, FL          |
            | 1428892912060            | {"content.type":"text/csv"}               | 201.91.5.170, Tampa, FL            |
            | 1428892912060            | {"content.type":"text/csv"}               | 220.181.108.158, Miami, FL         |
            | 1428892912060            | {"content.type":"text/csv"}               | 220.181.108.161, Chicago, IL       |
            | 1428892912060            | {"content.type":"text/csv"}               | 220.181.108.184, Philadelphia, PA  |
            | 1428892912060            | {"content.type":"text/csv"}               | 222.205.101.211, Indianpolis, IN   |
            | 1428892912060            | {"content.type":"text/csv"}               | 24.4.216.155, Denver, CO           |
            | 1428892912060            | {"content.type":"text/csv"}               | 66.249.75.153, San Diego, CA       |
            | 1428892912060            | {"content.type":"text/csv"}               | 77.75.77.11, Austin, TX            |
            | 1428892981049            | {}                                        | 69.181.160.120, Los Angeles, CA    |
            +===========================================================================================================+
            Fetched 22 rows

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Having reviewed data, set a new format for the stream
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Drop the external **Hive** table
         - Recreate the **Hive** table with new schema
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| set stream format ip2geo csv "ip string, city string, state string"
          
            Successfully set format of stream 'ip2geo'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Retrieve events from the stream, in new format
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run **Hive** command using **Hive CLI**
         - ``SELECT * FROM stream_ip2geo``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| execute 'select * from stream_ip2geo'
                    
            +================================================================================================================+
            | stream_ip2geo.ts:| stream_ip2geo.headers:      | stream_ip2geo.ip:| stream_ip2geo.city: | stream_ip2geo.state: |
            | BIGINT           | map<string,string>          | STRING           | STRING              | STRING               |
            +================================================================================================================+
            | 1428892912060    | {"content.type":"text/csv"} | 108.206.32.124   |  Santa Clara        |  CA                  |
            | 1428892912060    | {"content.type":"text/csv"} | 109.63.206.34    |  San Jose           |  CA                  |
            | 1428892912060    | {"content.type":"text/csv"} | 113.72.144.115   |  New York           |  New York            |
            | 1428892912060    | {"content.type":"text/csv"} | 123.125.71.19    |  Palo Alto          |  CA                  |
            | 1428892912060    | {"content.type":"text/csv"} | 123.125.71.27    |  Redwood            |  CA                  |
            | 1428892912060    | {"content.type":"text/csv"} | 123.125.71.28    |  Los Altos          |  CA                  |
            | 1428892912060    | {"content.type":"text/csv"} | 123.125.71.58    |  Mountain View      |  CA                  |
            | 1428892912060    | {"content.type":"text/csv"} | 142.54.173.19    |  Houston            |  TX                  |
            | 1428892912060    | {"content.type":"text/csv"} | 144.76.137.226   |  Dallas             |  TX                  |
            | 1428892912060    | {"content.type":"text/csv"} | 144.76.201.175   |  Bedminister        |  NJ                  |
            | 1428892912060    | {"content.type":"text/csv"} | 162.210.196.97   |  Milipitas          |  CA                  |
            | 1428892912060    | {"content.type":"text/csv"} | 188.138.17.205   |  Santa Barbara      |  CA                  |
            | 1428892912060    | {"content.type":"text/csv"} | 195.110.40.7     |  Orlando            |  FL                  |
            | 1428892912060    | {"content.type":"text/csv"} | 201.91.5.170     |  Tampa              |  FL                  |
            | 1428892912060    | {"content.type":"text/csv"} | 220.181.108.158  |  Miami              |  FL                  |
            | 1428892912060    | {"content.type":"text/csv"} | 220.181.108.161  |  Chicago            |  IL                  |
            | 1428892912060    | {"content.type":"text/csv"} | 220.181.108.184  |  Philadelphia       |  PA                  |
            | 1428892912060    | {"content.type":"text/csv"} | 222.205.101.211  |  Indianpolis        |  IN                  |
            | 1428892912060    | {"content.type":"text/csv"} | 24.4.216.155     |  Denver             |  CO                  |
            | 1428892912060    | {"content.type":"text/csv"} | 66.249.75.153    |  San Diego          |  CA                  |
            | 1428892912060    | {"content.type":"text/csv"} | 77.75.77.11      |  Austin             |  TX                  |
            | 1428892981049    | {}                          | 69.181.160.120   |  Los Angeles        |  CA                  |
            +================================================================================================================+
            Fetched 22 rows

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Join data in the two streams and retrieve selected events
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run **Hive** command using **Hive CLI**
         - ``SELECT remote_host, city, state, request from stream_logEventStream join stream_ip2geo on (stream_logEventStream.remote_host = stream_ip2geo.ip) limit 10``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| execute 'select remote_host, city, state, request from stream_logEventStream join stream_ip2geo on (stream_logEventStream.remote_host = stream_ip2geo.ip) limit 10'
 
            +======================================================================================================================+
            | remote_host: STRING | city: STRING | state: STRING | request: STRING                                                 |
            +======================================================================================================================+
            | 108.206.32.124      |  Santa Clara |  CA           | GET /browse/CDAP-DUT725-8 HTTP/1.1                              |
            |----------------------------------------------------------------------------------------------------------------------|
            | 108.206.32.124      |  Santa Clara |  CA           | GET /s/d41d8cd98f00b204e9800998ecf8427e-CDN/en_US/4411/1/1.0/_/ |
            |                     |              |               | download/batch/bamboo.web.resources:base-model/bamboo.web.resou |
            |                     |              |               | rces:base-model.js HTTP/1.1                                     |
            |----------------------------------------------------------------------------------------------------------------------|
            | 108.206.32.124      |  Santa Clara |  CA           | GET /s/d41d8cd98f00b204e9800998ecf8427e-CDN/en_US/4411/1/1.0/_/ |
            |                     |              |               | download/batch/bamboo.web.resources:model-deployment-version/ba |
            |                     |              |               | mboo.web.resources:model-deployment-version.js HTTP/1.1         |
            |----------------------------------------------------------------------------------------------------------------------|
            | 108.206.32.124      |  Santa Clara |  CA           | GET /s/d41d8cd98f00b204e9800998ecf8427e-CDN/en_US/4411/1/1.0/_/ |
            |                     |              |               | download/batch/bamboo.web.resources:model-deployment-result/bam |
            |                     |              |               | boo.web.resources:model-deployment-result.js HTTP/1.1           |
            |----------------------------------------------------------------------------------------------------------------------|
            | 108.206.32.124      |  Santa Clara |  CA           | GET /s/d41d8cd98f00b204e9800998ecf8427e-T/en_US/4411/1/3.5.7/_/ |
            |                     |              |               | download/batch/com.atlassian.support.stp:stp-license-status-res |
            |                     |              |               | ources/com.atlassian.support.stp:stp-license-status-resources.c |
            |                     |              |               | ss HTTP/1.1                                                     |
            |----------------------------------------------------------------------------------------------------------------------|
            | 108.206.32.124      |  Santa Clara |  CA           | GET /s/d41d8cd98f00b204e9800998ecf8427e-CDN/en_US/4411/1/1.0/_/ |
            |                     |              |               | download/batch/bamboo.web.resources:model-deployment-operations |
            |                     |              |               | /bamboo.web.resources:model-deployment-operations.js HTTP/1.1   |
            |----------------------------------------------------------------------------------------------------------------------|
            | 108.206.32.124      |  Santa Clara |  CA           | GET /s/d41d8cd98f00b204e9800998ecf8427e-CDN/en_US/4411/1/1.0/_/ |
            |                     |              |               | download/batch/bamboo.web.resources:model-deployment-environmen |
            |                     |              |               | t/bamboo.web.resources:model-deployment-environment.js HTTP/1.1 |
            |----------------------------------------------------------------------------------------------------------------------|
            | 108.206.32.124      |  Santa Clara |  CA           | GET /s/d41d8cd98f00b204e9800998ecf8427e-CDN/en_US/4411/1/1.0/_/ |
            |                     |              |               | download/batch/bamboo.web.resources:model-deployment-project/ba |
            |                     |              |               | mboo.web.resources:model-deployment-project.js HTTP/1.1         |
            |----------------------------------------------------------------------------------------------------------------------|
            | 108.206.32.124      |  Santa Clara |  CA           | GET /s/71095c56c641f2c4a4f189b9dfcd7a38-CDN/en_US/4411/1/5.6.2/ |
            |                     |              |               | _/download/batch/bamboo.deployments:deployment-project-list/bam |
            |                     |              |               | boo.deployments:deployment-project-list.js?locale=en-US HTTP/1. |
            |                     |              |               | 1                                                               |
            |----------------------------------------------------------------------------------------------------------------------|
            | 108.206.32.124      |  Santa Clara |  CA           | GET /s/d41d8cd98f00b204e9800998ecf8427e-CDN/en_US/4411/1/5dddb6 |
            |                     |              |               | ea4dc4fd5569d992cf603f31e5/_/download/contextbatch2/css/atl.gen |
            |                     |              |               | eral,bamboo.result/batch.css HTTP/1.1                           |
            +======================================================================================================================+
            Fetched 10 rows


.. _introduction-to-cdap-transforming-your-data:

Transforming Your Data
======================
- CDAP Extensions such as :ref:`Cask Hydrator <cask-hydrator>` create applications that are
  reusable through the configuration of artifacts and can be used to create an application
  without writing any code at all
- Built-in ETL (Extract, Transform, Load) and data pipeline applications
- Hydrator includes over 30 plugins to build applications merely through configuration of parameters
- Build your own custom plugins, using simple APIs
- Hydrator Transformations provide pre-defined transformations to be applied on streams or other datasets
- In this example, we will use the data pipeline system artifact to create a batch application to convert data in a stream to
  Avro-formatted files in a ``TimePartitionedFileSet`` that can be queried using either Hive or Impala

..        - .. code:: json
..            :class: copyable copyable-text

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Create a stream-conversion application using the batch ``cdap-data-pipeline`` system artifact
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Write a custom consumer that reads from source (example: **Kafka**)
         - Write the data to **HDFS**
         - Create an external table in **Hive** called ``stream_ip2geo``
         - Orchestrate running the custom consumer periodically using **Oozie**
         - Keep track of last processed times
         
     * - Using CDAP
       - - Write a configuration file, saving it to ``examples/resources/app-config.json``, with these contents:

     * - 
       - .. parsed-literal::
           :class: copyable copyable-text
       
           {
             "description": "Batch Data Pipeline Application",
             "artifact": {
               "name": "cdap-data-pipeline",
               "scope": "system",
               "version": "|release|"
             },
             "config": {
               "schedule": "\*/5 \* \* \* \*",
               "engine": "mapreduce",
               "stages": [
                 {
                   "name": "Stream",
                   "plugin": {
                     "name": "Stream",
                     "type": "batchsource",
                     "properties": {
                       "format": "clf",
                       "name": "logEventStream",
                       "duration": "5m"
                     }
                   }
                 },
                 {
                   "name": "TPFSAvro",
                   "plugin": {
                     "name": "TPFSAvro",
                     "type": "batchsink",
                     "properties": {
                       "schema": "{
                         \\"type\\":\\"record\\",
                         \\"name\\":\\"etlSchemaBody\\",
                         \\"fields\\":[
                           {\\"name\\":\\"ts\\",\\"type\\":\\"long\\"},
                           {\\"name\\":\\"remote_host\\",\\"type\\":[\\"string\\",\\"null\\"]},
                           {\\"name\\":\\"remote_login\\",\\"type\\":[\\"string\\",\\"null\\"]},
                           {\\"name\\":\\"auth_user\\",\\"type\\":[\\"string\\",\\"null\\"]},
                           {\\"name\\":\\"date\\",\\"type\\":[\\"string\\",\\"null\\"]},
                           {\\"name\\":\\"request\\",\\"type\\":[\\"string\\",\\"null\\"]},
                           {\\"name\\":\\"status\\",\\"type\\":[\\"int\\",\\"null\\"]},
                           {\\"name\\":\\"content_length\\",\\"type\\":[\\"int\\",\\"null\\"]},
                           {\\"name\\":\\"referrer\\",\\"type\\":[\\"string\\",\\"null\\"]},
                           {\\"name\\":\\"user_agent\\",\\"type\\":[\\"string\\",\\"null\\"]}]}",
                       "name": "logEventStream_converted",
                       "basePath": "logEventStream_converted"
                     }
                   }
                 },
                 {
                   "name": "Projection",
                   "plugin": {
                     "name": "Projection",
                     "type": "transform",
                     "properties": {
                       "drop": "headers"
                     }
                   }
                 }
               ],
              "connections": [
                {
                  "from": "Stream",
                  "to": "Projection"
                },
                {
                  "from": "Projection",
                  "to": "TPFSAvro"
                }
              ]
            }
           }

            
     * - 
       - - Create an application using that configuration through the CLI:

     * - 
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| create app logEventStreamConverter cdap-data-pipeline |release| system examples/resources/app-config.json
            Successfully created application
          
            |cdap >| resume schedule logEventStreamConverter.dataPipelineSchedule
            Successfully resumed schedule 'dataPipelineSchedule' in app 'logEventStreamConverter'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - List the applications available in the CDAP instance
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Not available
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| list apps
 
            +====================================================================================================+
            | id                      | description       | artifactName       | artifactVersion | artifactScope |
            +====================================================================================================+
            | logEventStreamConverter | Data Pipeline App | cdap-data-pipeline | |version|           | SYSTEM        |
            |                         | lication          |                    |                 |               |
            +====================================================================================================+
 
         .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"

            |cdap >| describe app logEventStreamConverter
 
            +====================================================================================================+
            | type      | id                   | description                                                     |
            +====================================================================================================+
            | MapReduce | phase-1              | MapReduce phase executor. Sources 'Stream' to sinks 'TPFSAvro'. |
            | Workflow  | DataPipelineWorkflow | Data Pipeline Workflow                                          |
            +====================================================================================================+
 
         .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"

            |cdap >| describe stream logEventStream
 
            +===============================================================================================+
            | ttl              | format | schema                  | notification.threshold.mb | description |
            +===============================================================================================+
            | 9223372036854775 | clf    | {"type":"record","name" | 1024                      |             |
            |                  |        | :"streamEvent","fields" |                           |             |
            |                  |        | :[{"name":"remote_host" |                           |             |
            |                  |        | ,"type":["string","null |                           |             |
            |                  |        | "]},{"name":"remote_log |                           |             |
            |                  |        | in","type":["string","n |                           |             |
            |                  |        | ull"]},{"name":"auth_us |                           |             |
            |                  |        | er","type":["string","n |                           |             |
            |                  |        | ull"]},{"name":"request |                           |             |
            |                  |        | _time","type":["string" |                           |             |
            |                  |        | ,"null"]},{"name":"requ |                           |             |
            |                  |        | est","type":["string"," |                           |             |
            |                  |        | null"]},{"name":"status |                           |             |
            |                  |        | ","type":["int","null"] |                           |             |
            |                  |        | },{"name":"content_leng |                           |             |
            |                  |        | th","type":["int","null |                           |             |
            |                  |        | "]},{"name":"referrer", |                           |             |
            |                  |        | "type":["string","null" |                           |             |
            |                  |        | ]},{"name":"user_agent" |                           |             |
            |                  |        | ,"type":["string","null |                           |             |
            |                  |        | "]}]}                   |                           |             |
            +===============================================================================================+
 
         .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"

            |cdap >| get workflow schedules logEventStreamConverter.DataPipelineWorkflow
 
            +===========================================================================================================+
            | applicatio | program    | program type | name       | type       | descriptio | properties | runtime args |
            | n          |            |              |            |            | n          |            |              |
            +===========================================================================================================+
            | logEventSt | DataPipeli | WORKFLOW     | dataPipeli | co.cask.cd | Data pipel | cron entry | {}           |
            | reamConver | neWorkflow |              | neSchedule | ap.interna | ine schedu | : */5 * *  |              |
            | ter        |            |              |            | l.schedule | le         | * *        |              |
            |            |            |              |            | .TimeSched |            |            |              |
            |            |            |              |            | ule        |            |            |              |
            +===========================================================================================================+

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Load data into the stream; it will automatically be converted  
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Write a custom consumer that reads from source (example: **Kafka**)
         - Write the data to **HDFS**
         - Create external table in **Hive** called ``stream_ip2geo``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| load stream logEventStream examples/resources/accesslog.txt
          
            Successfully loaded file to stream 'logEventStream'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - List available datasets
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run **HDFS** commands using **HBase** shell
         - ``hbase shell> list``
         
     * - Using CDAP
       - Dataset that is time partitioned

         .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| list dataset instances
 
            +=================================================================================+
            | name                      | type                                                |
            +=================================================================================+
            | logEventStream_converted  | co.cask.cdap.api.dataset.lib.TimePartitionedFileSet |
            +=================================================================================+

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Describe the converted dataset
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run **Hive** query using **Hive CLI**
         - ``'describe user_logEventStream_converted'`` 
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| execute 'describe dataset_logEventStream_converted'
          
            +=======================================================================+
            | col_name: STRING        | data_type: STRING    | comment: STRING      |
            +=======================================================================+
            | ts                      | bigint               | from deserializer    |
            | remote_host             | string               | from deserializer    |
            | remote_login            | string               | from deserializer    |
            | auth_user               | string               | from deserializer    |
            | request_time            | string               | from deserializer    |
            | request                 | string               | from deserializer    |
            | status                  | int                  | from deserializer    |
            | content_length          | int                  | from deserializer    |
            | referrer                | string               | from deserializer    |
            | user_agent              | string               | from deserializer    |
            | year                    | int                  |                      |
            | month                   | int                  |                      |
            | day                     | int                  |                      |
            | hour                    | int                  |                      |
            | minute                  | int                  |                      |
            |                         |                      |                      |
            | # Partition Information |                      |                      |
            | # col_name              | data_type            | comment              |
            |                         |                      |                      |
            | year                    | int                  |                      |
            | month                   | int                  |                      |
            | day                     | int                  |                      |
            | hour                    | int                  |                      |
            | minute                  | int                  |                      |
            +=======================================================================+
            Fetched 24 rows

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Retrieve the first two events from the converted data
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run **Hive** query using **Hive CLI**
         - ``SELECT ts, request, status FROM dataset_logEventStream_converted LIMIT 2``
         
     * - Using CDAP
       - - Instead of waiting for the schedule to run, you can directly start the workflow and check its status:

     * - 
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| start workflow logEventStreamConverter.DataPipelineWorkflow
            
            Successfully started workflow 'DataPipelineWorkflow' of application 'logEventStreamConverter'
            with stored runtime arguments '{}'            
            
            |cdap >| get workflow status logEventStreamConverter.DataPipelineWorkflow
            
            RUNNING
 
            ...
            
            |cdap >| get workflow status logEventStreamConverter.DataPipelineWorkflow
            
            STOPPED

     * - 
       - - Once the workflow has stopped, retrieve the first two events from the converted data: 

     * - 
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| execute 'SELECT ts, request, status FROM dataset_logEventStream_converted LIMIT 2'
          
            +=====================================================================+
            | ts: BIGINT    | request: STRING                       | status: INT |
            +=====================================================================+
            | 1430769459594 | GET /ajax/planStatusHistoryNeighbouri | 200         |
            |               | ngSummaries.action?planKey=COOP-DBT&b |             |
            |               | uildNumber=284&_=1423341312519 HTTP/1 |             |
            |               | .1                                    |             |
            |---------------------------------------------------------------------|
            | 1430769459594 | GET /rest/api/latest/server?_=1423341 | 200         |
            |               | 312520 HTTP/1.1                       |             |
            +=====================================================================+
            Fetched 2 rows


Building Real World Applications
================================
- Build Data Applications using simple-to-use CDAP APIs
- Compose complex applications consisting of workflow, MapReduce, real-time DAGs (Tigon) and services
- Build using a collection of pre-defined data pattern libraries
- Deploy and manage complex data applications such as Web Applications

**Let's see how we would build a real-world application using CDAP:**

- *Wise App* performs Web analytics on access logs
- *WiseFlow* parses and computes pageview count per IP in real time
- A MapReduce computes bounce counts: percentage of pages that *don’t* go to another page before exiting
- Service to expose the data 
- Unified platform for different processing paradigms

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Deploy a pre-built CDAP application: Wise App
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Write and execute **MapReduce** using **Hadoop**
         - Separate environment for processing in real-time setup stack
         - Add ability to periodically copy datasets into **SQL** using **Sqoop**
         - Orchestrate the **MapReduce** job using **Oozie**
         - Write an application to serve the data
         
     * - Using CDAP
       - Download the Wise app and unzip into the ``examples`` directory of your CDAP SDK:
       
         .. tabbed-parsed-literal::
      
            $ cd cdap-sdk-|release|/examples
            $ curl -O http://repository.cask.co/downloads/co/cask/cdap/apps/|cdap-apps-version|/cdap-wise-|cdap-apps-version|.zip
            $ unzip cdap-wise-|cdap-apps-version|.zip

         From within the CDAP CLI:

         .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| deploy app examples/cdap-wise-|cdap-apps-version|/target/cdap-wise-|cdap-apps-version|.jar
          
            Successfully deployed application

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Describe application components
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Check **Oozie**
         - Check **YARN** Console
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| describe app Wise
 
            +=====================================================================+
            | type      | id                    | description                     |
            +=====================================================================+
            | Flow      | WiseFlow              | Wise Flow                       |
            | MapReduce | BounceCountsMapReduce | Bounce Counts MapReduce Program |
            | Service   | WiseService           |                                 |
            | workflow  | WiseWorkflow          | Wise Workflow                   |
            +=====================================================================+

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Start the application's flow (for processing events)
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Set classpath in environment variable 
         - ``CLASSPATH=/my/classpath``
         - Run the command to start the **YARN** application
         - ``yarn jar /path/to/myprogram.jar``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| start flow Wise.WiseFlow
          
            Successfully started flow 'WiseFlow' of application 'Wise'
            with stored runtime arguments '{}'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Check the status of the flow
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Retrieve the application ID
         - ``yarn application -list | grep "Wise.WiseFlow"``
         - Retrieve the status
         - ``yarn application -status <APP ID>``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| get flow status Wise.WiseFlow
          
            RUNNING

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Ingest access log data into the Wise App stream
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Write a custom consumer for **Kafka** that reads from source
         - Write the data to **HDFS**
         - Create external table in **Hive** called ``cdap_stream_logeventstream``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| load stream logEventStream examples/resources/accesslog.txt
 
            Successfully loaded file to stream 'logEventStream'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Retrieve logs
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Navigate to the **Resource Manager UI**
         - Find the *Wise.WiseFlow* on UI
         - Click to see application logs
         - Find all the node managers for the application containers
         - Navigate to all the containers in separate tabs 
         - Click on container logs
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| get flow logs Wise.WiseFlow
 
            2015-04-15 09:22:53,775 - INFO  [FlowletRuntimeService
            STARTING:c.c.c.i.a.r.f.FlowletRuntimeService$1@110] - Initializing flowlet:
            flowlet=pageViewCount, instance=0, groupsize=1, namespaceId=default, applicationId=Wise,
            program=WiseFlow, runid=aae85671-e38b-11e4-bd5e-3ee74a48f4aa
            2015-04-15 09:22:53,779 - INFO  [FlowletRuntimeService
            STARTING:c.c.c.i.a.r.f.FlowletRuntimeService$1@117] - Flowlet initialized:
            flowlet=pageViewCount, instance=0, groupsize=1, namespaceId=default, applicationId=Wise,
            program=WiseFlow, runid=aae85671-e38b-11e4-bd5e-3ee74a48f4aa
            ...
            2015-04-15 10:07:54,708 - INFO  [FlowletRuntimeService
            STARTING:c.c.c.i.a.r.f.FlowletRuntimeService$1@117] - Flowlet initialized: flowlet=parser,
            instance=0, groupsize=1, namespaceId=default, applicationId=Wise, program=WiseFlow,
            runid=f4e0e52a-e391-11e4-a467-3ee74a48f4aa
            2015-04-15 10:07:54,709 - DEBUG [FlowletRuntimeService
            STARTING:c.c.c.i.a.r.AbstractProgramController@230] - Program started: WiseFlow:parser
            f4e0e52a-e391-11e4-a467-3ee74a48f4aa

.. highlight:: console

.. rubric:: Program Lifecycle

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Start the Wise application workflow to process ingested data
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Start the job using **Oozie**
         - ``oozie job -start <arguments>``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| start workflow Wise.WiseWorkflow
          
            Successfully started workflow 'WiseWorkflow' of application 'Wise' 
            with stored runtime arguments '{}'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Check the status of the workflow 
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Get the workflow status from **Oozie**
         - ``oozie job -info <jobid>``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| get workflow status Wise.WiseWorkflow
          
            RUNNING

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Start the WiseService that will be used to retrieve results
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Set classpath in environment variable 
         - ``CLASSPATH=/my/classpath``
         - Run the command to start the yarn application
         - ``yarn jar /path/to/myprogram.jar``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| start service Wise.WiseService
          
            Successfully started service 'WiseService' of application 'Wise' 
            with stored runtime arguments '{}'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Check the status of the service
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Get the application ID
         - ``yarn application -list | grep "Wise.WiseService"``
         - Get the status
         - ``yarn application -status <APP ID>``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| get service status Wise.WiseService
          
            RUNNING

.. rubric:: Serve the processed data in real time

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Discover the WiseService's available endpoints for retrieving results
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Navigate to the **Resource Manager UI**
         - Find the *Wise.WiseService* on UI
         - Click to the see application logs
         - Find all the node managers for the application containers
         - Navigate to all the containers in separate tabs 
         - Click on container logs
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| get endpoints service Wise.WiseService
          
            +=========================+
            | method | path           |
            +=========================+
            | GET    | /ip/{ip}/count |
            | POST   | /ip/{ip}/count |
            +=========================+

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Retrieve the count of a particular IP address (``69.181.160.120``)
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Discover the host and port where the service is running on by looking at the host 
           and port in the **YARN** logs or by writing a discovery client that is co-ordinated using **ZooKeeper**
         - Run ``curl http://hostname:port/v3/namespaces/default/apps/Wise/services/WiseService/methods/ip/69.181.160.120/count``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
 
            |cdap >| call service Wise.WiseService GET /ip/69.181.160.120/count
          
            < 200 OK
            < Content-Length: 5
            < Connection: keep-alive
            < Content-Type: application/json
            20097

..             +=================================================+
..             | status | headers            | body size | body  |
..             +=================================================+
..             | 200    | Content-Length : 5 | 5         | 20097 |
..             |        | Connection : keep- |           |       |
..             |        | alive              |           |       |
..             |        | Content-Type : app |           |       |
..             |        | lication/json      |           |       |
..             +=================================================+

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - List the dataset instances
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run a command in **HBase shell** 
         - ``hbase shell> list "cdap.user.*"``
         
     * - Using CDAP
       - - The listing returned will depend on whether you have run all of the previous examples

     * -  
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"
       
            |cdap >| list dataset instances
 
            +================================================================================+
            | name                     | type                                                |
            +================================================================================+
            | pageViewStore            | co.cask.cdap.apps.wise.PageViewStore                |
            | bounceCountStore         | co.cask.cdap.apps.wise.BounceCountStore             |
            | logEventStream.converted | co.cask.cdap.api.dataset.lib.TimePartitionedFileSet |
            +================================================================================+

.. rubric:: View bounce count results 

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Retrieve the first five pages with bounce counts and their statistics 
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Run a command in the **Hive CLI**
         - ``"SELECT * FROM dataset_bouncecountstore LIMIT 5"``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"

            |cdap >| execute 'SELECT * FROM dataset_bouncecountstore LIMIT 5'
          
            +===============================================================================================+
            | dataset_bouncecountstore.uri: STRING   | dataset_bouncecountstore  | dataset_bouncecountstore |
            |                                        | .totalvisits: BIGINT      | .bounces: BIGINT         |
            +===============================================================================================+
            | /CDAP-DUT-50/index.php                 | 2                         | 2                        |
            |-----------------------------------------------------------------------------------------------|
            | /ajax/planStatusHistoryNeighbouringSum | 2                         | 2                        |
            | maries.action?planKey=CDAP-DUT&buildNu |                           |                          |
            | mber=50&_=1423398146659                |                           |                          |
            |-----------------------------------------------------------------------------------------------|
            | /ajax/planStatusHistoryNeighbouringSum | 2                         | 0                        |
            | maries.action?planKey=COOP-DBT&buildNu |                           |                          |
            | mber=284&_=1423341312519               |                           |                          |
            |-----------------------------------------------------------------------------------------------|
            | /ajax/planStatusHistoryNeighbouringSum | 2                         | 0                        |
            | maries.action?planKey=COOP-DBT&buildNu |                           |                          |
            | mber=284&_=1423341312521               |                           |                          |
            |-----------------------------------------------------------------------------------------------|
            | /ajax/planStatusHistoryNeighbouringSum | 2                         | 0                        |
            | maries.action?planKey=COOP-DBT&buildNu |                           |                          |
            | mber=284&_=1423341312522               |                           |                          |
            +===============================================================================================+
            Fetched 5 rows

.. rubric:: Stop Application and Delete From Server

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Stop the WiseService
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Find the **YARN** application ID from the command
         - ``yarn application -list | grep "Wise.WiseService"``
         - Stop the application by running the command
         - ``yarn application -kill <application ID>``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"

            |cdap >| stop service Wise.WiseService
          
            Successfully stopped service 'WiseService' of application 'Wise'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Stop the Wise flow
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Find the **YARN** application ID from the command
         - ``yarn application -list | grep "Wise.WiseFlow"``
         - Stop the application by running the command
         - ``yarn application -kill <application ID>``
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"

            |cdap >| stop flow Wise.WiseFlow
          
            Successfully stopped flow 'WiseFlow' of application 'Wise'

.. container:: table-block

  .. list-table::
     :widths: 80 20
     :stub-columns: 1
     
     * - Delete the application from the Server
       - 
       
  .. list-table::
     :widths: 15 85
     :class: triple-table
     :stub-columns: 1

     * - Without CDAP
       - - Delete the workflow from **Oozie**
         - Remove the service jars and flow jars
         
     * - Using CDAP
       - .. tabbed-parsed-literal::
            :tabs: "CDAP CLI"

            |cdap >| delete app Wise
          
            Successfully deleted application 'Wise'


Summary
=======

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - Without CDAP
     - With CDAP 

   * - - Bring in different open source technologies, each with different design principles
       - Familiarize and learn how to operate the different technologies
       - Design specific architectures to wire in different components
       - Revisit everything whenever the technologies change
     - - Learn a single framework that works with multiple technologies
       - Abstraction of data in the Hadoop environment through logical representations of underlying data
       - Portability of applications through decoupling underlying infrastructures
       - Services and tools that enable faster application development
       - Higher degrees of operational control in production through enterprise best practices
