.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

====================
Cleanse Column Names
====================

The CLEANSE-COLUMN-NAMES directive sanatizes column names, following
these rules:

-  Trim leading and trailing spaces
-  Lowercases the column name
-  Replaces any character that are not one of ``[A-Z][a-z][0-9]`` or
   ``_`` with an underscore (``_``)

Syntax
------

::

    cleanse-column-names

Example
-------

Using this record as an example:

::

    {
      "COL1": 1,
      "col:2": 2,
      "Col3": 3,
      "COLUMN4": 4,
      "col!5": 5
    }

Applying this directive:

::

    cleanse-column-names

would result in this record:

::

    {
      "col1": 1,
      "col_2": 2,
      "col3": 3,
      "column4": 4,
      "col_5": 5
    }
