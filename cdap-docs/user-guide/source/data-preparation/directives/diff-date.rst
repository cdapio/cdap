.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=========
Diff Date
=========

The DIFF-DATE directive calculates the difference between two dates.

Syntax
------

::

    diff-date <column1> <column2> <destination>

Usage Notes
-----------

The DIFF-DATE directive calculates the difference between two Date
objects (``<column1>`` minus ``<column2>``) and puts the difference (in
milliseconds) into the destination column.

This directive can only be applied on two columns whose date strings
have already been parsed, either using the
`PARSE-AS-DATE <parse-as-date.md>`__ or
`PARSE-AS-SIMPLE-DATE <parse-as-simple-date.md>`__ directives.

A negative difference can be returned when the first column is an
earlier date than the second column.

If one of the dates in the column is ``null``, the resulting column will
be ``null``.

If any of the columns contains the string ``now``, then the current
date-time will be substituted for it. When ``now`` is encountered, the
directive applies the same value for ``now`` across all rows.

Example
-------

Using this record as an example:

::

    {
      "create_date": "02/12/2017",
      "update_date": "02/14/2017"
    }

Applying this directive:

::

    diff-date update_date create_date diff_date

would result in this record:

::

    {
      "create_date": "02/12/2017",
      "update_date": "02/14/2017",
      "diff_date": 17280000
    }
