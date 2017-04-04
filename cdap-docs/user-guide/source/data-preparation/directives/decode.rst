.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

======
Decode
======

The DECODE directive decodes a column value as one of ``base32``,
``base64``, or ``hex`` following
`RFC-4648 <https://tools.ietf.org/html/rfc4648>`__.

Syntax
------

::

    decode <base32|base64|hex> <column>

The ``<column>`` is the name of the column to which the decoding is
applied.

Usage Notes
-----------

Base decoding of data is used in many situations to store or transfer
data in environments that, for legacy reasons, are restricted to
US-ASCII data. Base decoding can be used in new applications that do not
have legacy restrictions because it allows the manipulation of objects
with text editors.

The DECODE directive generates a new column with a name following the
format of ``<column>_decode_<type>``.

Different column values are handled following these rules:

-  If the column is ``null``, the resulting column will also be
   ``null``.
-  If the column specified is not found in the record, then the record
   is skipped as a no-op.
-  If the column value is not of either type string or byte array, it
   fails.

See also the `ENCODE <encode.md>`__ directive.

Example
-------

Using this record as an example:

::

    {
      "col1": "IJQXGZJTGIQEK3TDN5SGS3TH",
      "col2": "VGVzdGluZyBCYXNlIDY0IEVuY29kaW5n",
      "col3": "48657820456e636f64696e67"
    }

Applying these directives:

::

    decode base32 col1
    decode base64 col2
    decode hex col3

would result in this record:

::

    {
      "col1": "IJQXGZJTGIQEK3TDN5SGS3TH",
      "col2": "VGVzdGluZyBCYXNlIDY0IEVuY29kaW5n",
      "col3": "48657820456e636f64696e67",
      "col1_decode_base32": "Base32 Encoding",
      "col2_decode_base64": "Testing Base 64 Encoding",
      "col3_decode_hex": "Hex Encoding",
    }
