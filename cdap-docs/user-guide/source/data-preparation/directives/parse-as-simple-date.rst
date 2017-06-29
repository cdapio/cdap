.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

====================
Parse as Simple Date
====================

The PARSE-AS-SIMPLE-DATE directive parses date strings.

Syntax
------

::

    parse-as-simple-date <column> <pattern>

Usage Notes
-----------

The PARSE-AS-SIMPLE-DATE directive will parse a date string, using a
pattern string. If the column is ``null`` or has already been parsed as
a date, applying this directive is a no-op. The column to be parsed as a
date should be of type string.

Examples
--------

See `FORMAT-DATE <format-date.md>`__ for an explanation and example of
these pattern strings.
