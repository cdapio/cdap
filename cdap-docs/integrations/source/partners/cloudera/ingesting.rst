.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cloudera-ingesting:

=========================
Ingestion and Exploration
=========================

.. _integrations-impala:

Integrating CDAP with Impala
============================

When using CDAP with Impala:

.. |cdap-apps| replace:: **CDAP Applications:**
.. _cdap-apps: ../../../cdap-apps/hydrator/index.html

- |cdap-apps|_ Using **ETL** and **custom ETL plugins**


.. |stream| replace:: **Stream Exploration:**
.. _stream: ../../../developers-manual/data-exploration/streams.html

- |stream|_ It is often useful to be able to **examine data in a stream** in an ad-hoc manner through SQL-like queries


.. |fileset| replace:: **Fileset Exploration:**
.. _fileset: ../../../developers-manual/data-exploration/filesets.html

- |fileset|_ The **CDAP FileSet Datasets** can be explored through ad-hoc SQL-like queries.


Ingesting and Exploring Data with Impala
========================================

Streams are the primary means of bringing data from external systems into the CDAP in
real time. They are ordered, time-partitioned sequences of data, usable for real-time
collection and consumption of data.

They can easily be created by using the CDAP Command Line Interface (CLI).
First, connect to your CDAP instance using the CLI::

  cdap > connect <hostname>:10000

Next, create a stream::

  cdap > create stream trades

You can then add events to a stream, one-by-one::

  cdap > send stream trades 'NFLX,441.07,50'
  cdap > send stream trades 'AAPL,118.63,100'
  cdap > send stream trades 'GOOG,528.48,10'

Or, you can add the entire contents of a file::

  cdap > load stream trades <my_path>/trades.csv

Or, you can use the other tools and APIs available to ingest data in real time or batch.
For more information on what are other ways of ingesting data into CDAP, see the links at
the top of this page.

You can now examine the contents of your stream by executing a SQL query::

  cdap > execute 'select * from stream_trades limit 5'
  +===================================================================================================+
  | stream_trades.ts: BIGINT | stream_trades.headers: map<string,string> | stream_trades.body: STRING |
  +===================================================================================================+
  | 1422493022983            | {}                                        | NFLX,441.07,50             |
  | 1422493027358            | {}                                        | AAPL,118.63,100            |
  | 1422493031802            | {}                                        | GOOG,528.48,10             |
  | 1422493036080            | {"content.type":"text/csv"}               | GOOG,538.53,18230          |
  | 1422493036080            | {"content.type":"text/csv"}               | GOOG,538.17,100            |
  +===================================================================================================+

You can also attach a schema to your stream to enable more powerful queries::

  cdap > set stream format trades csv 'ticker string, price double, trades int'
  cdap > execute 'select ticker, sum(price * trades) / 1000000 as millions from stream_trades group by ticker order by millions desc'
  +=====================================+
  | ticker: STRING | millions: DOUBLE   |
  +=====================================+
  | AAPL           | 3121.8966341143905 |
  | NFLX           | 866.0789117408007  |
  | GOOG           | 469.01340359839986 |
  +=====================================+

On one of our test clusters, the above query took just about two minutes to complete.
Data in CDAP is integrated with Apache Hive, and the above query above translates to a Hive query.
As such, it will launch two MapReduce jobs in order to calculate the query results, which
is why it takes minutes instead of seconds. 

To reduce query time, you can use Impala to query the data instead of Hive. Since streams
are written in a custom format, they cannot be directly queried through Impala. Instead,
you can create an ETL batch application that regularly reads
stream events and writes those events into files on HDFS that can then be queried by Impala.

To do this, write the following JSON to a config file::

  {
    "description": "Periodically reads stream data and writes it to a TimePartitionedFileSet",
    "config": {
      "schedule": "*/10 * * * *",
      "source": {
        "name": "tradeStream",
        "plugin": {
          "name": "Stream",
          "properties": {
            "name": "trades",
            "duration": "10m",
            "format": "csv",
            "schema": "{
              \"type\":\"record\",
              \"name\":\"purchase\",
              \"fields\":[
                {\"name\":\"ticker\",\"type\":\"string\"},
                {\"name\":\"price\",\"type\":\"double\"},
                {\"name\":\"trades\",\"type\":\"int\"}
              ]
            }",
            "format.setting.delimiter":","
          }
        }
      },
      "transforms": [
        {
          "name": "dropHeadersTransform",
          "plugin": {
            "name": "Projection",
            "properties": {
              "drop": "headers"
            }
          }
        }
      ],
      "sinks": [
        {
          "name": "tpfsAvroSink",
          "plugin": {
            "name": "TPFSAvro",
            "properties": {
              "name": "trades_converted",
              "schema": "{
                \"type\":\"record\",
                \"name\":\"purchase\",
                \"fields\":[
                  {\"name\":\"ts\",\"type\":\"long\"},
                  {\"name\":\"ticker\",\"type\":\"string\"},
                  {\"name\":\"price\",\"type\":\"double\"},
                  {\"name\":\"trades\",\"type\":\"int\"}
                ]
              }",
              "basePath": "trades_converted"
            }
          }
        }
      ],
      "connections": [
        {
          "from": "tradeStream",
          "to": "dropHeadersTransform"
        },
        {
          "from": "dropHeadersTransform",
          "to": "tpfsAvroSink"
        }
      ]
    }
  }

**Note:** The above JSON has been re-formatted to fit and requires editing (remove the line endings added to
the ``schema`` values) to be a conforming JSON file. 

Then use your config file with the ``cdap-etl-batch`` artifact to create an application through the CLI.
For example, if you wrote the above JSON to a file named ``conversion.json``::

  .. container:: highlight

    .. parsed-literal::
      cdap > create app trades_conversion cdap-etl-batch |version| system <path-to-conversion.json>


This will create and configure an application. The application's schedule (named, by default, to ``etlWorkflow``)
will not run until you resume it::

  cdap > resume schedule trades_conversion.etlWorkflow

This will start a schedule that will run the workflow every ten minutes. 
The next time the workflow runs, it will spawn a MapReduce job that reads all events added
in the past ten minutes, writes each event to Avro encoded files, and registers a new
partition in the Hive Metastore. We can then query the contents using Impala. On a
cluster, use the Impala shell to connect to Impala::

  $ impala-shell -i <impala-host>
  > invalidate metadata
  > select ticker, sum(price * trades) / 1000000 as millions from dataset_trades_converted group by ticker order by millions desc
  +--------+-------------------+
  | ticker | millions          |
  +--------+-------------------+
  | AAPL   | 3121.88477111439  |
  | NFLX   | 866.0568582408006 |
  | GOOG   | 469.0081187983999 |
  +--------+-------------------+
  Fetched 3 row(s) in 1.03s

Since we are using Impala, no MapReduce jobs are launched, and the query comes back in
about one second.

Now that you have data in CDAP and are able to explore your data, you can use CDAP's many
useful and powerful services, such as the ability to dynamically scale processing units,
distributed transactions, and service discovery, to write applications that meet your
business needs.
