.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _cloudera-ingesting:

==================================================
Ingestion and Exploration
==================================================

.. _integrations-impala:

Integrating CDAP with Impala
============================

When using CDAP with Impala:

.. |adapters| replace:: **CDAP Adapters:**
.. _adapters: ../../../developers-manual/advanced/adapters.html

- |adapters|_ **Stream-conversion** and **custom adapter** types


.. |stream| replace:: **Stream Exploration:**
.. _stream: ../../../developers-manual/data-exploration/streams.html

- |stream|_ It is often useful to be able to **examine data in a Stream** in an ad-hoc manner through SQL-like queries


.. |fileset| replace:: **Fileset Exploration:**
.. _fileset: ../../../developers-manual/data-exploration/filesets.html

- |fileset|_ The **CDAP FileSet Datasets** can be explored through ad-hoc SQL-like queries.


Ingesting and Exploring Data with Impala
===========================================

Streams are the primary means of bringing data from external systems into the CDAP in
realtime. They are ordered, time-partitioned sequences of data, usable for realtime
collection and consumption of data.

They can easily be created by using the CDAP Command Line Interface (CLI).
First, connect to your CDAP instance using the CLI::

  > connect <hostname>:11015

Next, create a Stream::

  > create stream trades

You can then add events to a Stream, one-by-one::

  > send stream trades 'NFLX,441.07,50'
  > send stream trades 'AAPL,118.63,100'
  > send stream trades 'GOOG,528.48,10'

Or, you can add the entire contents of a file::

  > load stream trades /my/path/trades.csv

Or you can use the other tools and APIs available to ingest data in real-time or batch.
For more information on what are other ways of ingesting data into CDAP, see the links at
the top of this page.

You can now examine the contents of your stream by executing a SQL query::

  > execute 'select * from cdap_stream_trades limit 5'
  +==================================================================================================================+
  | cdap_stream_trades.ts: BIGINT | cdap_stream_trades.headers: map<string,string> | cdap_stream_trades.body: STRING |
  +==================================================================================================================+
  | 1423165506100                 | {}                                             | NFLX,441.07,50                  |
  | 1423165511026                 | {}                                             | AAPL,118.63,100                 |
  | 1423165516222                 | {}                                             | GOOG,528.48,10                  |
  +==================================================================================================================+

You can also attach a schema to your stream to enable more powerful queries::

  > set stream format trades csv 'ticker string, price double, trades int'
  > execute 'select ticker, sum(price * trades) / 1000000 as millions from cdap_stream_trades group by ticker order by millions desc'
  +===================================+
  | ticker: STRING | millions: DOUBLE |
  +===================================+
  | NFLX           | 0.0220535        |
  | AAPL           | 0.011863         |
  | GOOG           | 0.0052848        |
  +===================================+

On one of our test clusters, the above query took just about two minutes to complete.
Data in CDAP is integrated with Apache Hive, and the above query above translates to a Hive query.
As such, it will launch two MapReduce jobs in order to calculate the query results, which
is why it takes minutes instead of seconds. 

To reduce query time, you can use Impala to query the data instead of Hive. Since Streams
are written in a custom format, they cannot be directly queried through Impala. Instead,
you can create an Adapter that regularly reads Stream events and writes those events into
files on HDFS that can then be queried by Impala. You can also do this through the CLI::

  > create stream-conversion adapter ticks_adapter on trades frequency 10m format csv schema "ticker string, price double, trades int"

This will create an Adapter that runs every ten minutes, reads the last ten minutes of
events from the Stream, and writes them to a file set that can be queried through Impala.

The next time the Adapter runs, it will spawn a MapReduce job that reads all events added
in the past ten minutes, writes each event to Avro encoded files, and registers a new
partition in the Hive Metastore. We can then query the contents using Impala. On a
cluster, use the Impala shell to connect to Impala::

  $ impala-shell -i <impala-host>
  > invalidate metadata
  > select ticker, sum(price * trades) / 1000000 as millions from cdap_user_trades_converted group by ticker order by millions desc
  +--------+-------------------+
  | ticker | millions          |
  +--------+-------------------+
  | NFLX   | 0.0220535         |
  | AAPL   | 0.011863          |
  | GOOG   | 0.0052848         |
  +--------+-------------------+
  Fetched 3 row(s) in 1.03s

Since we are using Impala, no MapReduce jobs are launched, and the query comes back in
about one second.

Now that you have data in CDAP and are able to explore your data, you can use CDAP's many
useful and powerful services, such as the ability to dynamically scale processing units,
distributed transactions, and service discovery, to write Applications that meet your
business needs.
