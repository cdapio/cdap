.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015-2016 Cask Data, Inc.

.. highlight:: console

.. _supported-characters:

====================
Supported Characters
====================

The Cask Data Application Platform (CDAP) has different naming conventions for different entities in CDAP.
We distinguish these different character sets, and note which entities use them:


Alphanumeric Character Set
--------------------------
This is the basic character set that all entities support:

- Alphanumeric characters (``a-z A-Z 0-9``)


Alphanumeric Extended Character Set
-----------------------------------
**Namespace IDs** can use an *alphanumeric extended character set:*

- Alphanumeric characters (``a-z A-Z 0-9``)
- Underscores (``_``)


Alphanumeric Extra Extended Character Set
-----------------------------------------
Except as noted on this page, **all other CDAP entities** (such as streams, datasets, flows, apps,
plugins) support an *alphanumeric extra extended character set:*

- Alphanumeric characters (``a-z A-Z 0-9``)
- Hyphens (``-``)
- Underscores (``_``)

Note that streams and datasets whose names begin with an underscore (``_``) will not be
visible in the home page of the :ref:`CDAP UI <cdap-ui>`, though they will be visible
elsewhere in the CDAP UI.


Dataset Character Set (Deprecated)
----------------------------------
In earlier versions of CDAP, **Dataset names** used the alphanumeric extra extended character set, plus periods:

- Alphanumeric characters (``a-z A-Z 0-9``)
- Hyphens (``-``)
- Periods (``.``)
- Underscores (``_``)

We recommend that instead you use the *Alphanumeric Extra Extended Character Set*, as support for this may
be removed in the future.


Hive Limitation and Conversion
------------------------------
`Hive 0.12 <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable>`__
only supports the *alphanumeric extended character set:* alphanumeric characters (``a-z
A-Z 0-9``) and underscores (``_``). 

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

Note that if you configure the Hive table name using the ``explore.table.name`` property
(see :ref:`Data Exploration <data-exploration>`), then the value of that property is
used literally; that is, no prefix is added and no character conversion takes place.
