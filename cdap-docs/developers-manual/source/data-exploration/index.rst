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
    Integration with External Systems <integration>

This section covers how you can explore data in CDAP through the use of ad-hoc SQL-like queries.
Queries can be run over Streams and certain types of Datasets, with a JDBC driver
providing integration with external systems such as Business Intelligence (BI) tools.

.. rubric:: Exploring Streams

All streams can be explored, with support for attaching read formats and schemas, enabling powerful queries.
:ref:`stream-exploration` describes how you can attach a schema to a Stream.

.. rubric:: Exploring FileSet Datasets

:ref:`fileset-exploration` describes how you can make a ``FileSet`` or ``TimePartitionedFileSet`` explorable.

.. rubric:: Exploring Custom Datasets

:ref:`custom-dataset-exploration` describes how you can make a custom dataset that is explorable.

.. rubric:: Integration with External Systems

Through the use of a JDBC driver, CDAP can be integrated with a wide range of external systems. :ref:`exploration-integration`
describes how to use the JDBC driver programmatically, with two examples of integrating CDAP with
business intelligence tools.
