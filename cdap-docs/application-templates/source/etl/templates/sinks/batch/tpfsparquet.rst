.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sinks: Batch: TPFSParquet
===============================

.. rubric:: Description: Parquet Sink with Time Partitioned File Dataset

**Schema:** The Parquet schema of the record being written to the Sink as a JSON Object.

**TPFS Name:** Name of the Time Partitioned FileSet Dataset to which the records have to
be written. If it doesn't exist, it will be created.

**Base Path:** Base path for the Time Partitioned FileSet. Defaults to the name of the
dataset.
