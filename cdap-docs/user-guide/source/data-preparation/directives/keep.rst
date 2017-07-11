.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

====
Keep
====

The KEEP directive is used to keep specified columns from the record.
This is the opposite behavior of the `DROP <drop.md>`__ directive.

Syntax
------

::

    keep <column>[,<column>]

``column`` is the name of a column in the record to be kept.

Usage Notes
-----------

After the KEEP directive is applied, the column(s) specified in the
directive are preserved, and all other columns are removed from the
record.

Example
-------

Using this record as an example:

::

    {
      "id": 1,
      "timestamp": 1234434343,
      "measurement": 10.45,
      "isvalid": true
    }

Applying this directive:

::

    keep id,measurement

would result in this record:

::

    {
      "id": 1,
      "measurement": 10.45
    }
