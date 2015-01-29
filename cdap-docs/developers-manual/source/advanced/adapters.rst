.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _advanced-adapters:

================
Adapters (Beta)
================

Adapters read data from one or more data sources, perform a transformation, and then
write to one or more data sinks. They may be used for ETL (extract, transform, and load)
purposes, or any situation where you want to pipe data from one data source to another.

CDAP currently provides a stream-conversion adapter that regularly reads data from
a Stream and then writes the data to a ``TimePartitionedFileSet`` in
`Avro format <http://avro.apache.org>`__. The output
can then be explored through Hive or Impala, assuming a compatible schema is used. 

Each Adapter type is implemented as a CDAP Application, with an Adapter as
a schedule for running the program in the application. For example, the stream-conversion
Adapter type is an application named StreamConversion. Each Adapter is a schedule
that periodically runs the Workflow defined in the StreamConversion application.
Custom adapters can be added to CDAP, though the feature is experimental and will
likely change and improve in future releases.

Stream-conversion Adapter
=========================
A stream-conversion Adapter regularly reads data from a source Stream and writes
it to a ``TimePartitionedFileSet`` as Avro files. You may want to use this Adapter if you
want to run partitioned Hive or Impala queries on data in a Stream. You may also want
to use the Adapter if you have set a TTL on a Stream for real-time processing,
but want to keep all Stream data around in another format for archiving purposes.
Each Stream event is translated as a line in the output fileset.
Stream format and schema must be given as adapter properties.
The output schema will add a required ``ts`` field of type long for the timestamp of the
event, as well as nullable string fields for any headers given in the adapter properties. 

Stream conversion Adapters require these properties:

- ``frequency``: Defines how often the Adapter should run, as a number followed by a letter.
  For example, 5m means the Adapter will run every five minutes, and 2h means the Adapter
  will run every two hours. For minutes, the number given must be an even divisor of an hour.
  For example, 30m is valid, but 13m is not because an hour does not evenly divide into 13 minute chunks.
  Similarly, for hours, the number given must be an even divisor of a day.
  For example, 3h is valid, but 7h is not.
- ``source.name``: The name of the Stream to read from.
- ``source.format.name``: The name of the format of the Stream body.
- ``source.format.settings``: Settings of the format as a JSON Object of string to string.
- ``source.schema``: The schema of the Stream body.
- ``sink.name``: The name of the ``TimePartitionedFileSet`` to write to.

These Adapter properties are optional:

- ``mapper.resources.memory.mb``: Amount of memory each mapper of the Mapreduce program should use.
- ``mapper.resources.vcores``: Number of virtual cores each mapper of the Mapreduce program should use.
- ``headers``: Any stream headers that should be included in the output.

This sink property is required:

- ``explore.table.property.avro.schema.literal``: The output schema including timestamp and headers. 

Each time the Adapter is run, it will read stream events from a time range, write to the fileset, and add those
files as a partition. The time range is calculated based on the frequency of the Adapter and the logical
start time of the Adapter run. For example, if the Adapter is configured to run every hour, each run of the
Adapter will read events starting from the logical start time minus an hour and ending at the logical start time.
The logical start time is the time at which the schedule is triggered. For an hour frequency, this means
the logical start time will be on the hour, every hour. Even if the run actually begins at 5:03:13, the logical
start will be 5:00:00 and the run will read Stream events that were added from 4:00:00 inclusive to 5:00:00 exclusive.

As there are so many properties, and because properties such as schema must be string values of
escaped JSON Objects, it is highly recommended that you use the CLI to create stream-conversion Adapters.
For example::

  cdap (http://127.0.0.1:10000)> create stream-conversion adapter convert_tickers on tickers frequency 10m format csv schema "ticker string, trades int, price double"

The command above will create a stream-conversion Adapter named ``convert_tickers`` that runs every ten minutes.
It will read from the *tickers* Stream and write to the *tickers_converted* ``TimePartitionedFileSet``.
It will require stream events to be comma-separated values of ticker, trades, and price, and will write
events as partitioned Avro files. With this adapter running, you will be able to run Hive or Impala queries
over the output fileset, which will contain the ``ts``, ``ticker``, ``trades``, and ``price`` fields as well
as the partition fields::

  cdap (http://127.0.0.1:10000)> execute 'describe cdap_user_tickers_converted'
  +=======================================================================+
  | col_name: STRING        | data_type: STRING    | comment: STRING      |
  +=======================================================================+
  | tickers                 | string               | from deserializer    |
  | trades                  | int                  | from deserializer    |
  | price                   | double               | from deserializer    |
  | ts                      | bigint               | from deserializer    |
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


Adding a Custom Adapter Type
============================
Adapter types are CDAP Applications that contain a special manifest file and are placed
in an adapter plugins directory. If you wish to write a custom Adapter type, make sure
your manifest file contains these properties:

- ``CDAP-Adapter-Type``: Name by which your adapter type will be referenced.
- ``CDAP-Adapter-Program-Type``: Schedulable program type of your adapter.
  Currently, only ``WORKFLOW`` is supported.
- ``CDAP-Adapter-Properties``: Default properties to use for your adapter, formatted
  as a JSON Object of strings to strings. Properties set when creating an Adapter
  will be applied on top of the default properties.
  Adapter properties are passed to the Adapter program as runtime arguments.
- ``CDAP-Source-Type``: The source type. Currently only ``STREAM`` is supported.
- ``CDAP-Sink-Type``: The sink type. Currently only ``DATASET`` is supported.
- ``CDAP-Sink-Properties``: Default properties to use for sinks created when an
  Adapter is created, formatted as a JSON Object of strings to strings. Sink properties
  set when creating an Adapter will be applied on top of the default properties.
  When an Adapter is created, if the sink given does not already exist it will be
  created with the sink properties. The dataset.class properties must be present
  for dataset sinks.

You can look at the cdap-adapters module in the CDAP project as an example.
Once you have built your application jar, you must place it in the directory specified
by the ``app.adapter.dir`` configuration setting.
This setting defaults to ``/opt/cdap/master/plugins`` for distributed CDAP, and ``plugins``
for CDAP Standalone. Note that the jar must be present on all CDAP masters. 

Adapters are an experimental feature in CDAP. As such, they are subject to change
and improvement in future releases. For example, much of the information placed 
in the manifest file will likely be exposed for programmatic manipulation.
When creating a custom adapter, keep in mind the following limitations:

- The RESTful API for adapters only supports a single source and single sink. 
- Adapter properties are passed to Adapter programs as runtime arguments.
  Source and Sink properties are not passed.
- Only Workflows can be scheduled by the Adapter framework.
