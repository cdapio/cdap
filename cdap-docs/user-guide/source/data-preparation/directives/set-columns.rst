.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

===========
Set Columns
===========

The SET COLUMNS directive sets the names of columns, in the order they
are specified.

Syntax
------

::

    set columns <columm>[,<column>*]

The ``<column>`` specifies the new name of an existing column or
columns.

Usage Notes
-----------

The most common use of the SET COLUMNS directive is to set the name of
columns when a CSV file is parsed. The column names will be applied to
the record starting from the first field, in the order that they are
specified.

Examples
--------

Using this record as an example:

::

    {
      "body": "1,2,3,4,5"
    }

If you have parsed this ``body`` using the
`PARSE-AS-CSV <parse-as-csv.md>`__ directive:

::

    parse-as-csv body , false

the resulting record would be:

::

    {
      "body": "1,2,3,4,5",
      "body_1": "1",
      "body_2": "2",
      "body_3": "3",
      "body_4": "4",
      "body_5": "5"
    }

If you then apply the SET COLUMNS directive:

::

    set columns a,b,c,d,e

This would generate a record that has these column names:

::

    {
      "a": "1,2,3,4,5",
      "b": "1",
      "c": "2",
      "d": "3",
      "e": "4",
      "body_5": "5"
    }

Note that the last field (``body_5``) was not assigned the expected
name.

In order to correct this, either rename all the columns using:

::

    parse-as-csv body , false
    set columns body,a,b,c,d,e

resulting in this record:

::

    {
      "body": "1,2,3,4,5",
      "a": "1",
      "b": "2",
      "c": "3",
      "d": "4",
      "e": "5"
    }

or use a `DROP <drop.md>`__ directive:

::

    parse-as-csv body , false
    drop body
    set columns a,b,c,d,e

The result would then be this record:

::

    {
      "a": "1",
      "b": "2",
      "c": "3",
      "d": "4",
      "e": "5"
    }

Common Mistakes
---------------

When using the SET COLUMNS directive, the number of fields in the record
should be same as number of column names in the SET COLUMNS directive.
If they are not, then this directive will partially name the record
fields.

The names of the columns are in a single option, separated by commas.
Separating by spaces will set only the name of the first column.

When this directive is executed in a pipeline and the field "Name of
field" to be transformed is set to ``*``, then all fields are added to
the record causing issues with the naming of the columns, as it would
also include column names that are coming from the input.
