.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

================
Set Record Delim
================

The SET-RECORD-DELIM directive sets the record delimiter.

Syntax
------

::

     set-record-delim <column> <delimiter> [<limit>]

Usage Notes
-----------

This directive applies the record delimiter (``<delimiter>``) to
generate additional records using the ``<column>``. Optionally, a limit
(``<limit>``) can be specified to control the number of records being
generated as the delimiter is applied.
