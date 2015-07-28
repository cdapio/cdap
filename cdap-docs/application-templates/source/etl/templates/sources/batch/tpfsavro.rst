.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

========================
Sources: Batch: TPFSAvro 
========================

.. rubric:: Description: Avro Source with Time Partitioned File Dataset

**Schema:** The Avro schema of the record being read from the Source as a JSON Object.

**TPFS Name:** Name of the Time Partitioned FileSet Dataset to which the records have to be read from.

**Base Path:** Base path for the Time Partitioned FileSet. Defaults to the name of the
dataset.

**Duration:** Size of the time window to read with each run of the pipeline. The format is
expected to be a number followed by a 's', 'm', 'h', or 'd' specifying the time unit, with
's' for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, a value of
'5m' means each run of the pipeline will read 5 minutes of events from the TPFS source.

**Delay:** Optional delay for reading from TPFS source. The value must be of the same
format as the duration value. For example, a duration of '5m' and a delay of '10m' means
each run of the pipeline will read events from 15 minutes before its logical start time to
10 minutes before its logical start time. The default value is 0.
