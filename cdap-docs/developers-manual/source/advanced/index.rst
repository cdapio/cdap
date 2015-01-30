.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

:hide-toc: true

============================================
Advanced Topics
============================================

.. toctree::
   :maxdepth: 1
   
    Data Exploration <data-exploration>
    Best Practices <best-practices>
    Adapters <adapters>

This section of the documentation includes articles that cover advanced topics on CDAP that
will be of interest to developers who want a deeper dive into CDAP:

.. |data-exploration| replace:: **Data Exploration:**
.. _data-exploration: data-exploration.html

.. |best-practices| replace:: **Best Practices:**
.. _best-practices: best-practices.html

.. |adapters| replace:: **Adapters:**
.. _adapters: adapters.html

- |data-exploration|_ Exploration of CDAP Datasets using SQL.

- |best-practices|_ Suggestions when developing a CDAP application.

- |adapters|_ Adapters connect a data source to a data sink.
  CDAP currently provides a stream conversion Adapter that regularly reads data from a Stream and
  writes it to a ``TimePartitionedFileSet``, allowing it to be queried through Hive and Impala.
