.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. highlight:: console

.. _supported-characters:

============================================
Supported Characters
============================================

The Cask Data Application Platform (CDAP) has naming conventions for different elements of CDAP.

Streams
----------------

Stream names can have these characters:

- Alphanumeric characters (``a-z A-Z 0-9``)
- Hyphens (``-``)
- Underscores (``_``)


Datasets
----------

Dataset names can have these characters:

- Alphanumeric characters (``a-z A-Z 0-9``)
- Hyphens (``-``)
- Underscores (``_``)
- Periods (``.``)


Hive Limitation and Conversion
------------------------------

`Hive 0.12 <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL>`__
only supports alphanumeric characters (``a-z A-Z 0-9``) and underscores (``_``). 

As a consequence, any hyphens in Stream names and any hyphens or periods in Dataset names
will be converted to underscores while creating Hive tables. 

Examples: 

- The Streams

    - ``my-ingest``
    - ``my_ingest``
  
  will both be converted to ``stream_my_ingest``

- The Datasets

    - ``my-dataset``
    - ``my_dataset``
    - ``my.dataset``
    
  will all be converted to ``dataset_my_dataset``

Names should be carefully constructed to avoid any collisions as a result of conversion.
