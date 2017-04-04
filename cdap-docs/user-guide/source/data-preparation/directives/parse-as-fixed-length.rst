.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=====================
Parse as Fixed Length
=====================

The PARSE-AS-FIXED-LENGTH directive parses a column as a fixed length
record with widths for each field specified.

Syntax
------

::

    parse-as-fixed-length <column> <width>[,<width>]* [<padding>]

Usage Notes
-----------

Fixed-width text files are special cases of text files where the format
is specified by column widths, pad characters, and left or right
alignment. Column widths are measured in units of characters.

For example, if you have data in a text file where the first column
always has exactly 10 characters, the second column has exactly 5, the
third has exactly 12, and so on; this would be categorized as a
fixed-width text file.

If not defined, the ``<padding>`` character is assumed to be a space
character.

Example
-------

Using this record as an example:

::

    {
      "body": "12  10  ABCXYZ"
    }

Applying this directive:

::

    parse-as-fixed-length body 2,4,5,3

would result in this record:

::

    {
      "body": "12  10  ABCXYZ",
      "body_1": "12",
      "body_2": "  10",
      "body_3": "  ABC",
      "body_4": "XYZ
    }
