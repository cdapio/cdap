.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sinks: Batch: Table 
===============================

.. rubric:: Description: Batch Sink for CDAP Tables

**Name:** Name of the table dataset. If it does not already exist, one will be  created.

**Property Schema:** Optional schema of the table as a JSON Object. If the table does not
already exist, one will be created with this schema, which will allow the table to be
explored through Hive.

**Property Schema Row Field:** The name of the record field that should be used as the row
key when writing to the table.
