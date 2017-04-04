.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

============
Table Lookup
============

The TABLE-LOOKUP directive performs lookups into Table datasets.

Syntax
------

::

    table-lookup <column> <table>

-  ``<column>`` is an existing column that exists in both the current
   records and the table
-  ``<table>`` is a Table Dataset that has a column named by
   ``<column>``

Usage Notes
-----------

The TABLE-LOOKUP directive uses a column as the lookup key into a
specified Table dataset for each record. The column should be of type
string. The values in the row of the Table will be parsed as strings and
placed in the record in new columns, the names constructed from
combining the lookup key and the row column name with an underscore.

Example
-------

This example represents a lookup into the ``customerTable`` dataset of
type Table, where the record's field name ``customerUserId`` will be
used as the lookup key.

Suppose that this data is in the Table ``customerTable``:

+--------------------+---------------------+
| CustomerUserId     | City                |
+====================+=====================+
| bobistheman        | Palo Alto, CA       |
+--------------------+---------------------+
| joe1984            | Los Angeles, CA     |
+--------------------+---------------------+
| randomUserqwerty   | New York City, NY   |
+--------------------+---------------------+

If the input records to the directive are:

+------------------+-----------+------------+
| CustomerUserId   | Product   | Quantity   |
+==================+===========+============+
| bobistheman      | Apples    | 10         |
+------------------+-----------+------------+
| joe1984          | Bicycle   | 1          |
+------------------+-----------+------------+

Applying this directive:

::

    table-lookup customerUserId customerTable

would result in these output records:

+------------------+-----------+------------+------------------------+
| CustomerUserId   | Product   | Quantity   | CustomerUserId\_City   |
+==================+===========+============+========================+
| bobistheman      | Apples    | 10         | Palo Alto, CA          |
+------------------+-----------+------------+------------------------+
| joe1984          | Bicycle   | 1          | Los Angeles, CA        |
+------------------+-----------+------------+------------------------+
