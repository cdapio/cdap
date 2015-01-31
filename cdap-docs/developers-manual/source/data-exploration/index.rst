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
   
    Stream Exploration <streams>
    Fileset Exploration <filesets>
    Custom Dataset Exploration <custom-datasets>
    Integration with External Systems <integration>

This section covers how you can explore data in CDAP through the use of ad-hoc SQL-like queries.
Queries can be run over Streams and certain types of Datasets, with a JDBC driver
providing integration with external systems such as Business Intelligence (BI) tools.


.. |stream-exploration| replace:: **Stream Exploration:**
.. _stream-exploration: streams.html

- |stream-exploration|_ **All streams can be explored**, with support for attaching read
  formats and schemas, **enabling powerful queries.** This section covers how you attach a
  format and schema to a Stream.


.. |fileset-exploration| replace:: **Fileset Exploration:**
.. _fileset-exploration: filesets.html

- |fileset-exploration|_ Describes how you can make a ``FileSet`` or 
  ``TimePartitionedFileSet`` that is **explorable.**


.. |custom-dataset-exploration| replace:: **Custom Dataset Exploration:**
.. _custom-dataset-exploration: custom-datasets.html

- |custom-dataset-exploration|_ Describes how you can make a **custom Dataset** that is **explorable.**


.. |integration-exploration| replace:: **Integration with External Systems:**
.. _integration-exploration: integration.html

- |integration-exploration|_ Through the use of a **JDBC driver**, CDAP can be integrated with
  a wide range of external systems. This describes how to use the JDBC driver
  programmatically, with **two examples of integrating CDAP** with business intelligence
  tools.
