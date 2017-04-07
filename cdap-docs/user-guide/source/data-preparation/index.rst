.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.
    :description: The CDAP User Guide: Data Preparation

:hide-toc: true

.. _user-guide-data-preparation:

================
Data Preparation
================

.. toctree::

    Concepts <concepts>
    Notations <notations>
    Hashing and Masking <hashing-masking>
    Transform <transform>
    Parsers <parsers/index>
    Output Formatters <output-formatters>
    Encoders and Decoders <encoders-decoders>
    Date Transformations <date-transformations>
    Unique ID <unique-id>
    Directives <directives/index>
    Services <services>


Data Preparation (also known as "Data Prep") allows you to parse, transform, cleanse,
blend, and consolidate data for analysis. A combination of a CDAP service and transform,
it let you ensure that data is consistent and high-quality by enabling you to perform data
transformations with immediate visual feedback. You can then run the resulting
transformations at big data scale within minutes.

Data Prep performs the data cleansing, transformation, and filtering using a set of
instructions called *directives*. Directives to manipulate the data are generated either
by using an interactive visual tool or by being entered manually.

.. image:: /_images/data-preparation/data-preparation.png
   :width: 8in
