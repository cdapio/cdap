.. :author: Cask Data, Inc.
   :description: placeholder
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
System Datasets
============================================

The Cask Data Application Platform comes with several system-defined Datasets, including but not limited to
key/value Tables, indexed Tables and time series. Each of them is defined with the help of one or more embedded
Tables, but defines its own interface. Examples include:

- The ``KeyValueTable`` implements a key/value store as a Table with a single column.

- The ``IndexedTable`` implements a Table with a secondary key using two embedded Tables,
  one for the data and one for the secondary index.

- The ``TimeseriesTable`` uses a Table to store keyed data over time
  and allows querying that data over ranges of time.

See the :ref:`Javadocs <reference:javadocs>` for these classes and the :ref:`Examples <examples-index>`
to learn more about these Datasets. Any class in the CDAP libraries that implements the ``Dataset`` interface is a
system Dataset.

