.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

===============
Parse Timestamp
===============

The PARSE-TIMESTAMP directive parses column values that represent a Unix timestamp.

Syntax
------

::

    parse-timestamp :<column> '<timeunit>'

The ``<column>`` is the name of the column in the record that has timestamp. The ``<timeunit>`` can be
'seconds', 'milliseconds', or 'microseconds'.

Usage Notes
-----------

The PARSE-TIMESTAMP directive will parse a column that represents a Unix timestamp. The column type can be string or long.
The directive also has an optional parameter 'timeunit', which can be 'seconds', 'milliseconds', or 'microseconds'.
The default timeunit is 'milliseconds'. If the column is null or is already the timestamp type,
applying this directive is a no-op.


Examples
--------
If the column has unix timestamp 1536332271894 or "1536332271894", then after applying this directive,
will be a timestamp type that represents Friday, September 7, 2018 14:57:51.894 UTC.
