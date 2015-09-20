.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _included-apps-etl-plugins-batch-sources-stream:

======================
Batch Sources: Stream 
======================

.. rubric:: Description

Reads a batch of data from a stream. The source will read a configurable chunk of data from
a stream, based on the logical start time of the workflow it runs in and the configured
duration and delay of the source. If the format and schema of the stream are known,
they can be specified as well. The source will return a record for each stream event it reads.
Records will always contain a 'ts' field of type 'long' that contains the timestamp of the event,
as well as a 'headers' field of type 'map<string, string>' that contains the headers for
the event. Other fields output records are determined by the configured format and schema.
  
.. rubric:: Use Case

The source is used whenever you need to read from a stream in batch. For example,
you may want to read from a stream every hour, perform some data cleansing, then write
the cleansed data for that hour as Avro files.

.. rubric:: Properties

**name:** Name of the stream. Must be a valid stream name. If the stream does not exist,
it will be created.
    
**duration:** Size of the time window to read with each run of the pipeline. The format is
expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with
's' for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, a value of
'5m' means each run of the pipeline will read 5 minutes of events from the stream.

**delay:** Optional delay for reading stream events. The value must be of the same format
as the duration value. For example, a duration of '5m' and a delay of '10m' means each run
of the pipeline will read events from 15 minutes before its logical start time to 10
minutes before its logical start time. The default value is 0.

**format:** Optional format of the stream. Any format supported by CDAP is also supported.
For example, a value of 'csv' will attempt to parse stream events as comma separated
values. If no format is given, event bodies will be treated as bytes, resulting in a 
three-field schema: 'ts' of type long, 'headers' of type map of string to string, and 'body' of
type bytes.

**schema:** Optional schema for the body of stream events. Schema is used in conjunction
with format to parse stream events. Some formats like the avro format require schema,
while others do not. The schema given is for the body of the stream, so the final schema
of records output by the source will contain an additional field named 'ts' for the
timestamp and a field named 'headers' for the headers as the first and second fields of
the schema.

.. rubric:: Example

::

  {
    "name": "Stream",
    "properties": {
      "name": "accesslogs",
      "duration": "10m",
      "delay": "5m",
      "format": "clf"
    }
  }

This example reads from a stream named 'accesslogs'. With the 'duration' property set to
'10m', the source will read ten minutes-worth of data. As the 'delay' property is set to
'5m', the source will read data up to five minutes before the logical start time of the
run. The 'end time' of the data will then be five minutes before that logical start time. 
Combining the duration and the delay, the 'start time' of the data will be 15 minutes
before the logical start time of the run. For example, if the pipeline was scheduled to
run at 10:00am, the source will read data from 9:45am to 9:55am. The stream contents will
be parsed as 'clf' (Combined Log Format), which will output records with this schema::

  +======================================+
  | field name     | type                |
  +======================================+
  | ts             | long                |
  | headers        | map<string, string> |
  | remote_host    | nullable string     |
  | remote_login   | nullable string     |
  | auth_user      | nullable string     |
  | date           | nullable string     |
  | request        | nullable string     |
  | status         | nullable string     |
  | content_length | nullable string     |
  | referrer       | nullable string     |
  | user_agent     | nullable string     |
  +======================================+

The 'ts' and 'headers' fields will be always be present regardless of the stream format.
All other fields in this example come from the default schema for the 'clf' format.
