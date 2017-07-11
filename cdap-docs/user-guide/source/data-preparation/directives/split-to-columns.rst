.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

================
Split to Columns
================

The SPLIT-TO-COLUMNS directive splits a column based on a regular
expression into multiple columns.

Syntax
------

::

     split-to-columns <column> <regex>

The ``<column>`` is split into one or more columns around matches of the
specified regular expression ``<regex>``.

Usage Notes
-----------

The SPLIT-TO-COLUMNS directive takes a column, applies the regular
expression separator, and then creates multiple columns from the split.
The name of the columns are in the format:

::

    {
      "column": "...",
      "column_1": "...",
      "column_2": "...",
      "column_3": "...",
      ...
      "column_n": "..."
    }

Regular expressions allows the use of complex search patterns when
splitting the data in the column. It supports standard `Java regular
expression <https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html>`__
constructs.

The original column, when it is split into columns, generates new
columns for the record. ``column_1``, ``column_2``, through to
``column_n`` are the new columns that contain the ``n`` parts of the
split generated from applying this directive.

**Note:** This directive can only operate on columns of type string.

Examples
--------

If we have a ``<separator>`` pattern of ``,`` (a comma) over the string:

``This will be split 1,This will be split 2,This will be split 3,Split 4``

This will generate four new columns:

::

    {
      "1": "This will be split 1",
      "2": "This will be split 2",
      "3": "This will be split 3",
      "4": "Split 4"
    }

Using this record as an example:

::

    {
      "id": 1,
      "codes": "USD|AUD|AMD|XCD"
    }

Applying this directive:

::

    split-to-columns codes \|

**Note:** A backslash is required to escape the pipe character (``|``)
as it is an optional separator in a regex pattern.

This would result in four columns being generated, with each split value
being assigned to the column ``codes``:

::

    {
      "id": 1,
      "codes": "USD|AUD|AMD|XCD",
      "codes_1": "USD",
      "codes_2": "AUD",
      "codes_3": "AMD",
      "codes_4": "XCD"
    }
