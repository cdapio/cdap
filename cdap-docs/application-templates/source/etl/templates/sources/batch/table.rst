.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sources: Batch: Table
===============================

.. rubric:: Description: CDAP Table Dataset Batch Source

**Name:** Table name. If the table does not already exist, it will be created.

**Property Schema:** Schema of records read from the Table. Row columns map to record
fields. For example, if the schema contains a field named 'user' of type string, the value
of that field will be taken from the value stored in the 'user' column. Only simple types
are allowed (boolean, int, long, float, double, bytes, string).

**Property Schema Row Field:** Optional field name indicating that the field value should
come from the row key instead of a row column. The field name specified must be present in
the schema, and must not be nullable.
