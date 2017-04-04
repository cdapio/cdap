.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===============
Parse AVRO File
===============

The PARSE-AS-AVRO-FILE directive parses AVRO data file. The AVRO data
file stores both the data definition (schema) and the data together in
one file making it easy for programs to dynamically understand the
information stored in an AVRO file. The AVRO schema is in JSON format,
the data is in a binary format making it compact and efficient.

Syntax
------

::

    parse-as-avro-file <column>

The ``<column>`` contains the complete content of AVRO data file in the
binary octet stream.

Usage Notes
-----------

Parsing the AVRO data file will using this directive will flatten the
structure using the following rules:

-  If data type is simple like INT, FLOAT, DOUBLE, STRING, SHORT, the
   column name will be the name of the column in AVRO schema.
-  If data type is record, then the field name is used as prefix to name
   the fields within the record.
