.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

======================
Sources: Batch: Stream 
======================

.. rubric:: Description: Batch source for a Stream

**Name:** Name of the stream. Must be a valid stream name.  If it doesn't exist, it will be created.
    
**Duration:** Size of the time window to read with each run of the pipeline. The format is
expected to be a number followed by a 's', 'm', 'h', or 'd' specifying the time unit, with
's' for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, a value of
'5m' means each run of the pipeline will read 5 minutes of events from the stream.

**Delay:** Optional delay for reading stream events. The value must be of the same format
as the duration value. For example, a duration of '5m' and a delay of '10m' means each run
of the pipeline will read events from 15 minutes before its logical start time to 10
minutes before its logical start time. The default value is 0.

**Format:** Optional format of the stream. Any format supported by CDAP is also supported.
For example, a value of 'csv' will attempt to parse stream events as comma separated
values. If no format is given, event bodies will be treated as bytes, resulting in a three
field schema: 'ts' of type long, 'headers' of type map of string to string, and 'body' of
type bytes.

**Schema:** Optional schema for the body of stream events. Schema is used in conjunction
with format to parse stream events. Some formats like the avro format require schema,
while others do not. The schema given is for the body of the stream, so the final schema
of records output by the source will contain an additional field named 'ts' for the
timestamp and a field named 'headers' for the headers as as the first and second fields of
the schema.
