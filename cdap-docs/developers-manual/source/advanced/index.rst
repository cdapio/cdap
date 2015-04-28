.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

:hide-toc: true

============================================
Advanced Topics
============================================

.. toctree::
   :maxdepth: 1
   
    Best Practices <best-practices>
    Adapters <adapters>
    Creating Application Templates <atap>
    Application Logback <application-logback>

This section of the documentation includes articles that cover advanced topics on CDAP that
will be of interest to developers who want a deeper dive into CDAP:

.. |best-practices| replace:: **Best Practices:**
.. _best-practices: best-practices.html

- |best-practices|_ Suggestions when developing a CDAP application.


.. |adapters| replace:: **Adapters:**
.. _adapters: adapters.html

- |adapters|_ Adapters connect a data source to a data sink.
  CDAP currently provides a stream conversion Adapter that regularly reads data from a Stream and
  writes it to a ``TimePartitionedFileSet``, allowing it to be queried through Hive and Impala.


.. |atap| replace:: **Creating Application Templates:**
.. _atap: atap.html

- |atap|_ Covers creating custom Application Templates, Adapters, and Plugins (custom
  Sources, Sinks, and Transformations).


.. |application-logback| replace:: **Application Logback:**
.. _application-logback: application-logback.html

- |application-logback|_ Adding a custom logback to a CDAP application.