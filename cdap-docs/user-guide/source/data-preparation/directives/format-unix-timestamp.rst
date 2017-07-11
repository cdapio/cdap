.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

=====================
Format UNIX Timestamp
=====================

The FORMAT-UNIX-TIMESTAMP directive formats a UNIX timestamp as a date.

Syntax
------

::

    format-unix-timestamp <column> <pattern>

-  ``<column>`` is the column to be formatted as a date
-  ``<pattern>`` is the pattern string to be used in formatting the
   timestamp

Usage Notes
-----------

The FORMAT-UNIX-TIMESTAMP directive will parse a UNIX timestamp, using a
pattern string. The ``<column>`` should contain valid UNIX-style
timestamps.

Examples
--------

See `FORMAT-DATE <format-date.md>`__ for an explanation and example of
these pattern strings.
