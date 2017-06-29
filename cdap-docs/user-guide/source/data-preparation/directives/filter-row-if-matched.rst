.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=====================
Filter Row If Matched
=====================

The FILTER-ROW-IF-MATCHED directive filters records that match a pattern
for a column.

Deprecated
----------

Use the `FILTER-ROWS-ON <filter-rows-on.md>`__ directive instead.

Syntax
------

::

    filter-row-if-matched <column> <regex>

The ``<regex>`` is a valid regular expression that is evaluated on the
column value for every record.

Usage Notes
-----------

The FILTER-ROW-IF-MATCHED directive applies the regular expression on a
column value for every record. If the regex matches the column value,
the record is omitted, otherwise it is passed as-is to the input of the
next directive.

If the regex is ``null``, the value is compared against all the ``null``
as well as JSON null values.

Examples
--------

Using this record as an example:

::

    {
      "id": 1,
      "name": "Joltie, Root",
      "emailid": "jolti@hotmail.com",
      "hrlywage": "12.34",
      "gender": "Male",
      "country": "US"
    }

Applying this directive:

::

    filter-row-if-matched country !~ US

would result in filtering out records for individuals that are not in
the US (where ``country`` does not match "US").

Applying this directive:

::

    filter-row-if-matched (country !~ US && hrlywage > 12)

would result in filtering out records for individuals that are not in
the US (where ``country`` does not match "US") and whose hourly wage
(``hrlywage``) is greater than 12.
