.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

========
Quantize
========

The QUANTIZE directive quantizes a column based on specified ranges.

Syntax
------

::

    copy <source> <destination> <range_lower1:range_upper1=value>[,<range_lower2:range_upper2=value>*]

-  The ``<source>`` column is the column to be quantized
-  The ``<destination>`` column is the column where the results of the
   quantization are to be written

The QUANTIZE directive quantizes based on values of the ``<source>``
column.

Usage Notes
-----------

The QUANTIZE directive quantizes based on values from the ``<source>``
column into the ``<destination>`` column.

Using the list of ranges specified in the options, it will ``<value>``
into the ``<destination>`` column if the source value matches one of the
ranges.

The last range that the source value matches is the one that will be
used.

If a range limit is missing, that range will fail without an error or
exception.

If the ``<destination>`` column already exists, the directive will
override any existing data in that column.

The values in the ``<source>`` column must be numeric.

The ``<value>`` of the range can be numeric or a string.

Example
-------

Using these records as an example:

::

    [
      {
        "body": 1
      },
      {
        "body": 2
      },
      {
        "body": 3
      },
      {
        "body": 4
      },
      {
        "body": 5
      },
    ]

Applying this directives:

::

    quantize body body_q 1:2=20,3:4=40,5:10=max

would result in these records:

::

    [
      {
        "body": 1,
        "body_q": 20
      },
      {
        "body": 2,
        "body_q": 20
      },
      {
        "body": 3,
        "body_q": 40
      },
      {
        "body": 4,
        "body_q": 40
      },
      {
        "body": 5,
        "body_q": "max"
      },
    ]
