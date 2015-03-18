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

- Alphanumeric characters ('a-zA-Z0-9')
- Hyphens '-'
- Underscores '_''


Datasets
----------

Dataset names can have these characters:

- Alphanumeric characters ('a-zA-Z0-9')
- Hyphens '-'
- Underscores '_''
- Periods '.'


Hive Limitation and Conversion
------------------------------

`Hive 0.12 <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL>`__
only supports alphanumeric characters ('a-zA-Z0-9') and underscores '_' . 

As a consequence, any hyphens in Stream names and any hyphens or periods in Dataset names
will be converted to underscores while creating Hive tables. 

For instance: the Streams ``my-ingest`` and ``my_ingest`` will both be converted to
``cdap_stream_my_ingest``; the Datasets ``my-dataset``, ``my_dataset``, and ``my.dataset``
will all be converted to ``cdap_dataset_my_dataset``.

Names should be carefully constructed to avoid any collisions as a result of conversion.
