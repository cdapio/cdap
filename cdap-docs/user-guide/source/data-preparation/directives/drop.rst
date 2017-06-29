.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

====
Drop
====

The DROP directive is used to drop a column in a record.

Syntax
------

::

    drop <column>[,<column>]*

The ``<column>`` is the name of the column in the record to be droped.

Usage Notes
-----------

After the DROP directive is applied, the column and its associated value
are removed from the record. Later directives will not be able to
reference the dropped column.

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

    drop isvalid,measurement

would result in a record with no ``isvalid`` or ``measurement`` fields:

::

    {
      "id": 1,
      "timestamp": 1234434343
    }
