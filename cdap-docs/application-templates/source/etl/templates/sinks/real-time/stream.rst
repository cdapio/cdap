.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

===============================
Sinks: Real-time: Stream
===============================

.. rubric:: Description

Real-time sink that outputs to a specified CDAP Stream

.. rubric:: Use Case

TODO: Fill me out

.. rubric:: Properties

**name:** The name of the stream to output to. Must be a valid stream name. The stream
will be created if it does not exist.

**body.field:** Name of the field in the record that contains the data to be written to
the specified stream. The data could be in binary format as a byte array or a ByteBuffer.
It can also be a String. If unspecified, the 'body' key is used.

**headers.field:** Name of the field in the record that contains headers. Headers are
presumed to be a map of string to string.

.. rubric:: Example

TODO: Fill me out
