.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2017 Cask Data, Inc.

======================
Performance Evaluation
======================

Setup
-----

Hardware used for measuring the performance:

-  2.9 GHz Intel Core i5
-  16 GB 2133 MHz LPDDR3
-  Java 7

Light Data Transformation DMD
-----------------------------

These are the high-level transformations being performed on the data:

-  Parsing of CSV
-  Drop columns
-  Setting defaults on column
-  Changing case
-  Masking data
-  Filtering rows based on an expression

Directives
----------

::

      parse-as-csv demo , true
      drop demo
      drop demo_12
      fill-null-or-empty demo_11 N/A
      uppercase demo_17
      mask-number demo_18 xxx###
      drop demo_6
      drop demo_7
      fill-null-or-empty demo_5 N/A
      uppercase demo_3
      filter-row-if-true demo_9 =~ "CA"
      mask-number demo_10 xxx##
      mask-shuffle demo_4

Experiments
-----------

These two experiments were run: the first with 13M records, and the
second with 80M records.

Experiment #1
~~~~~~~~~~~~~

-  Number of records: 13,499,973
-  Number of bytes: 4,499,534,313 (~ 4GB)
-  Number of columns: 18

Performance Numbers
~~~~~~~~~~~~~~~~~~~

::

    count          = 13,376,053
    mean rate      = 64998.50 records/second
    1-minute rate  = 64921.29 records/second
    5-minute rate  = 46866.70 records/second
    15-minute rate = 36149.86 records/second

Experiment #2
~~~~~~~~~~~~~

-  Number of records: 80,999,838 (80M)
-  Number of bytes: 26,997,205,878 (~ 26GB)
-  Number of columns: 18
-  Total time: 1294 seconds (21.5 minutes)

Performance Numbers
~~~~~~~~~~~~~~~~~~~

::

    count          = 80,944,061
    mean rate      = 62465.93 records/second
    1-minute rate  = 62706.39 records/second
    5-minute rate  = 60755.41 records/second
    15-minute rate = 56673.32 records/second
