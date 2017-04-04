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
it allows you to ensure that data is consistent and high-quality by allowing you to
perform data transformations with visual feedback. You can immediately run the resulting
transformations at big data scale in minutes.

Data Prep performs data cleansing, transformation, and filtering using a set of
instructions called "directives". Directives to manipulate the data are generated either
using an interactive visual tool or are manually entered.

.. image:: /_images/data-preparation/data-preparation.png
   :width: 5in
   :align: center
