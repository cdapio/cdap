.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

.. _data-exploration:

============================================
Data Exploration
============================================

.. toctree::
   :maxdepth: 1
   
    Streams <streams>
    Filesets <filesets>
    Custom Datasets <custom-datasets>
    Integration with external systems <integration>

This section covers how you can explore data in CDAP through the use of ad-hoc SQL-style queries.
Queries can be run over Streams and certain types of Datasets, with a jdbc driver
provided for integration with external systems like various Business Intelligence (BI) tools.

.. rubric:: Exploring Streams

All streams can be explored, with support for attaching a read format and schema to enable
more powerful queries. :ref:`stream-exploration` describes how you can attach schema to a stream.

.. rubric:: Exploring FileSet Datasets

:ref:`fileset-exploration` describes how you can make a ``FileSet`` or ``TimePartitionedFileSet`` explorable.

.. rubric:: Exploring Custom Datasets

:ref:`custom-dataset-exploration` describes how you can make your custom dataset explorable.

.. rubric:: Integration with External Systems

Through the use of a jdbc driver, CDAP can be integrated with a wide range of external systems. :ref:`exploration-integration`
describes how to use the jdbc driver programmatically, as well as examples of integrating CDAP with a couple
business intelligence tools.
