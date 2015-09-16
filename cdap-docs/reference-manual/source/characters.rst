.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. highlight:: console

.. _supported-characters:

====================
Supported Characters
====================

The Cask Data Application Platform (CDAP) has different naming conventions for different entities in CDAP.

Alphanumeric Characters
-----------------------
**Metadata tags** are restricted to the alphanumeric character set:

- Alphanumeric characters (``a-z A-Z 0-9``)


Alphanumeric Extended Character Set
-----------------------------------
**Stream names**, **metadata property keys** and **metadata property values** can use
the alphanumeric extended character set:

- Alphanumeric characters (``a-z A-Z 0-9``)
- Hyphens (``-``)
- Underscores (``_``)


Dataset Character Set
---------------------
**Dataset names** can use the alphanumeric extended character set, plus periods:

- Alphanumeric characters (``a-z A-Z 0-9``)
- Hyphens (``-``)
- Underscores (``_``)
- Periods (``.``)


Hive Limitation and Conversion
------------------------------
`Hive 0.12 <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable>`__
only supports alphanumeric characters (``a-z A-Z 0-9``) and underscores (``_``). 

As a consequence, any hyphens in stream names and any hyphens or periods in dataset names
will be converted to underscores while creating Hive tables. 

Examples: 

- The streams

    - ``my-ingest``
    - ``my_ingest``
  
  will both be converted to ``stream_my_ingest``

- The datasets

    - ``my-dataset``
    - ``my_dataset``
    - ``my.dataset``
    
  will all be converted to ``dataset_my_dataset``

Names should be carefully constructed to avoid any collisions as a result of conversion.
